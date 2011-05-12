from xml.etree import ElementTree
from time import time
from glob import glob
import multiprocessing
import subprocess
import socket
import os.path
import sys
import os

import simplejson as json
import logging
import clustohttp
import eventlet
import memcache
import bottle
import jinja2

#redis = eventlet.import_patched('redis')
import redis
db = redis.Redis()

class RedisQueue(object):
    def __init__(self, key):
        self.key = key

    def put(self, obj):
        db.lpush(self.key, json.dumps(obj))

    def get(self):
        return json.loads(db.brpop(self.key)[1])

    def qsize(self):
        return db.llen(self.key)


rrdqueue = RedisQueue('rrdqueue')


RRDPATH = '/var/lib/metartg/rrds/%(host)s/%(service)s/%(metric)s.rrd'

def rrdtool(args):
    p = subprocess.Popen(['rrdtool'] + args.split(' '))
    p.wait()


def create_rrd(filename, metric, data):
    try:
        os.makedirs(os.path.dirname(filename))
    except:
        pass

    rrdtool('create %(filename)s --start %(start)s --step 60 \
DS:sum:%(dstype)s:600:U:U \
RRA:AVERAGE:0.5:1:1500 \
RRA:AVERAGE:0.5:5:2304 \
RRA:AVERAGE:0.5:30:4320' % {
    'filename': filename,
    'start': (data['ts'] - 1),
    'dstype': data['type'],
})


def update_rrd(filename, metric, data):
    #filename = filename.split('/var/lib/metartg/rrds/', 1)[1]
    #rrdtool('update --daemon 127.0.0.1:42217 %s %s:%s' % (filename, str(data['ts']), str(data['value'])))
    #ts = data['ts'] - (data['ts'] % 60)
    rrdtool('update %s %s:%s' % (filename, str(data['ts']), str(data['value'])))


def update_redis(host, service, metricname, metric):
    db.sadd('hosts', host)
    db.hset('metrics/%s' % host, '%s/%s' % (service, metricname), json.dumps((metric['ts'], metric['value'])))

def process_rrd_update(host, service, body):
    metrics = json.loads(body)
    for metric in metrics:
        rrdfile = RRDPATH % {
            'host': host,
            'service': service,
            'metric': metric,
        }
        if not os.path.exists(rrdfile):
            create_rrd(rrdfile, metric, metrics[metric])
        update_rrd(rrdfile, metric, metrics[metric])
        update_redis(host, service, metric, metrics[metric])
    return


def rrdupdate_worker(i, queue):
    count = 0
    while True:
        #sys.stdout.write('.')
        #sys.stdout.flush()
        metric = queue.get()
        if metric:
            process_rrd_update(*metric)
        count += 1
        if (count % 100) == 0:
            print 'Process[%i]: wrote %i metrics' % (i, count)

def main():
    procs = []
    for i in range(8):
        p = multiprocessing.Process(target=rrdupdate_worker, args=(i, rrdqueue))
        p.start()
        print 'Process[%i]: running' % i
        procs.append(p)

    for i in procs:
        p.join()

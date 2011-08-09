from xml.etree import ElementTree
from time import time
from glob import glob
import multiprocessing
import subprocess
import os.path
import sys
import os

import simplejson as json
import logging
import clustohttp
from eventlet.green import socket
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


class RRDCached(object):
    def __init__(self, host, port=42217):
        self.sock = socket.socket()
        self.sock.connect((host, port))
        self.buf = ''

    def update(self, filename, values):
        self.sock.sendall('UPDATE %s %s\n' % (filename, values))
        line = self.readline()
        status, line = line.split(' ', 1)
        status = int(status)
        if status < 0:
            logging.warning('Updating %s: %s' % (filename, line))
            return
        for i in range(status):
            logging.debug(self.readline())
    
    def readline(self):
        while True:
            if self.buf.find('\n') != -1:
                line, self.buf = self.buf.split('\n', 1)
                return line
            self.buf += self.sock.recv(1024)


rrdqueue = RedisQueue('rrdqueue')
rrdcache = RRDCached('127.0.0.1')
#eventlet.spawn_n(rrdcache.run)

RRDPATH = '%(host)s/%(service)s/%(metric)s.rrd'

def rrdtool(args):
    p = subprocess.Popen(['rrdtool'] + args.split(' '))
    p.wait()


def create_rrd(filename, metric, data):
    print 'Creating rrd', filename
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
    'start': (int(data['ts']) - 1),
    'dstype': data['type'],
})


def update_rrd(filename, metric, data):
    #filename = filename.split('/var/lib/metartg/rrds/', 1)[1]
    #rrdtool('update --daemon 127.0.0.1:42217 %s %s:%s' % (filename, str(data['ts']), str(data['value'])))
    #ts = data['ts'] - (data['ts'] % 60)
    #rrdtool('update %s %s:%s' % (filename, str(data['ts']), str(data['value'])))
    rrdcache.update(filename, '%s:%s' % (str(data['ts']), str(data['value'])))


def update_redis(host, service, metricname, metric):
    db.sadd('hosts', host)
    db.hset('metrics/%s' % host, '%s/%s' % (service, metricname), json.dumps((metric['ts'], metric['value'])))
    db.incr('processed')

def process_rrd_update(host, service, body):
    metrics = json.loads(body)
    for metric in metrics:
        rrdfile = RRDPATH % {
            'host': host,
            'service': service,
            'metric': metric,
        }
        if not 'ts' in metrics[metric]:
            print 'Invalid metric:', host, service, metric, metrics[metric]
            continue
        rrdfilepath = '/var/lib/metartg/rrds/' + rrdfile
        if not os.path.exists(rrdfilepath):
            create_rrd(rrdfilepath, metric, metrics[metric])
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
    rrdupdate_worker(0, rrdqueue)
    return

    procs = []
    for i in range(2):
        p = multiprocessing.Process(target=rrdupdate_worker, args=(i, rrdqueue))
        p.start()
        print 'Process[%i]: running' % i
        procs.append(p)

    for i in procs:
        p.join()

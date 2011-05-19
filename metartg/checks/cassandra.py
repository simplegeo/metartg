#!/usr/bin/env python
import simplejson as json
from time import time
import subprocess
import os
import urllib2

from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol.TBinaryProtocol import TBinaryProtocolAccelerated
from metartg.thrift.cassandra import Cassandra
from metartg.thrift.cassandra.ttypes import *

def get_local_ipv4():
    # This only works on ec2 and should thus be made better. But it
    # was the quickest way I could think of to gett the IP (no,
    # 'localhost' won't work.)
    url = 'http://169.254.169.254/latest/meta-data/local-ipv4'
    return urllib2.urlopen(url).read()

def get_keyspaces():
    socket = TSocket.TSocket(get_local_ipv4(), 9160)
    transport = TTransport.TFramedTransport(socket)
    protocol = TBinaryProtocolAccelerated(transport)
    client = Cassandra.Client(protocol)
    try:
        transport.open()
        keyspaces = client.describe_keyspaces()
    except Thrift.TException, tx:
        print 'Thrift: %s' % tx.message
    finally:
        transport.close()

    return keyspaces

def get_column_families(keyspace):
    socket = TSocket.TSocket(get_local_ipv4(), 9160)
    transport = TTransport.TFramedTransport(socket)
    protocol = TBinaryProtocolAccelerated(transport)
    client = Cassandra.Client(protocol)
    try:
        transport.open()
        column_families = client.describe_keyspace(keyspace)
    except Thrift.TException, tx:
        print 'Thrift: %s' % tx.message
    finally:
        transport.close()

    return column_families

def cfstats_cache_metrics():
    keyspaces = get_keyspaces()
    now = int(time())

    metrics = {}
    for keyspace in keyspaces:
        column_families = get_column_families(keyspace)
        for column_family in column_families.keys():
            for cache_type in ('Key', 'Row'):
                url = 'http://%s:8778/jolokia/read/' \
                    'org.apache.cassandra.db:cache=' \
                    '%s%sCache,keyspace=%s,type=Caches' % (
                    get_local_ipv4(), column_family, cache_type, keyspace)
                cache_stats = json.load(urllib2.urlopen(url))
                for label in ('RecentHitRate', 'Capacity', 'Size'):
                    metrics['%s-%s-%sCache%s' % (keyspace, column_family, cache_type, label)] = {
                        'ts': now,
                        'type': 'GAUGE',
                        'value': cache_stats['value'][label],
                        }

    return metrics


def tpstats_metrics():
    p = subprocess.Popen([
        '/usr/bin/java',
        '-jar', '/usr/share/metartg/contrib/GenericJMXLogJSON.jar',
        'localhost', '8080', 'org.apache.cassandra.concurrent:*',
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    now = int(time())

    metrics = {}
    for line in stdout.split('\n'):
        if not line:
            continue
        line = json.loads(line)
        name = line['name'].split('=', 1)[1]
        for label in ('ActiveCount', 'PendingTasks', 'CompletedTasks'):
            if label == 'CompletedTasks':
                datatype = 'COUNTER'
            else:
                datatype = 'GAUGE'

            metrics['%s_%s' % (name, label)] = {
                'ts': now,
                'type': datatype,
                'value': line[label],
            }

    return metrics

def sstables_metrics():
    metrics = {}

    for dirname in ('penelope', 'cassandra'):
        if os.path.exists('/mnt/var/lib/' + dirname):
            break

    for keyspace in os.listdir('/mnt/var/lib/%s/data' % dirname):
        now = int(time())
        sizes = {}
        for filename in os.listdir('/mnt/var/lib/%s/data/%s' % (dirname, keyspace)):
            if not filename.endswith('-Data.db'):
                continue

            columnfamily = filename.split('-', 1)[0]
            if not columnfamily in sizes:
                sizes[columnfamily] = []

            st = os.stat('/mnt/var/lib/%s/data/%s/%s' % (dirname, keyspace, filename))
            sizes[columnfamily].append(st.st_size)

        for columnfamily in sizes:
            metrics['%s.%s.min' % (keyspace, columnfamily)] = {
                'ts': now,
                'type': 'GAUGE',
                'value': min(sizes[columnfamily]),
            }
            metrics['%s.%s.max' % (keyspace, columnfamily)] = {
                'ts': now,
                'type': 'GAUGE',
                'value': max(sizes[columnfamily]),
            }
            metrics['%s.%s.avg' % (keyspace, columnfamily)] = {
                'ts': now,
                'type': 'GAUGE',
                'value': (sum(sizes[columnfamily]) / len(sizes[columnfamily])),
            }
            metrics['%s.%s.total' % (keyspace, columnfamily)] = {
                'ts': now,
                'type': 'GAUGE',
                'value': sum(sizes[columnfamily]),
            }
            metrics['%s.%s.count' % (keyspace, columnfamily)] = {
                'ts': now,
                'type': 'GAUGE',
                'value': len(sizes[columnfamily]),
            }

    return metrics


def scores_metrics():
    try:
        keyspace = file('/etc/metartg_cassandra_keyspace', 'r').read().strip('\r\n\t ')
    except:
        keyspace = 'Underdog_Records'

    p = subprocess.Popen([
        '/usr/bin/java',
        '-jar', '/usr/share/cassandra/jmxterm-1.0-alpha-4-uber.jar',
        '-v', 'silent', '-l', 'localhost:8080', '-n',
    ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    script = '''bean org.apache.cassandra.db:keyspace=%s,type=DynamicEndpointSnitch
get Scores''' % keyspace
    stdout, stderr = p.communicate(script)
    now = int(time())

    metrics = {}
    for line in stdout.split('\n'):
        line = line.strip(';\r\n\t ')
        if not line.startswith('/'):
            continue
        line = line.lstrip('/')
        host, score = line.split(' = ', 1)
        metrics[host] = {
            'ts': now,
            'type': 'GAUGE',
            'value': float(score),
        }
    return metrics


def run_check(callback):
    callback('cassandra_tpstats', tpstats_metrics())
    callback('cassandra_sstables', sstables_metrics())
    callback('cassandra_scores', scores_metrics())

    try:
        callback('cassandra_cfstats_cache', cfstats_cache_metrics())
    except urllib2.URLError:
        pass

if __name__ == '__main__':
    print json.dumps(scores_metrics(), indent=2)

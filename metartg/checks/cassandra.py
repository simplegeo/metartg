#!/usr/bin/env python
import simplejson as json
from time import time
import subprocess
import os
import urllib2
import sys
import re


def cfstats_cache_metrics():
    now = int(time())
    url = "http://localhost:8778/jolokia/read/*:*,type=Caches"
    try:
        caches = json.loads(urllib2.urlopen(url).read())['value']
    except Exception, e:
        sys.stderr.write("Error fetching list of CF Cache mbeans: %s" % e)
        return None

    pattern = re.compile(":cache\=(?P<column_family>.+?)(?P<cache_type>Row|Key)Cache,keyspace\=(?P<keyspace>.+?),")

    metrics = {}
    for mbean, cache in caches.items():
        attrs = pattern.search(mbean).groupdict()
        for metric in ('RecentHitRate', 'Capacity', 'Size'):
            metrics['%s-%s-%sCache%s' % (attrs['keyspace'], attrs['column_family'], attrs['cache_type'], metric)] = {
                'ts': now,
                'type': 'GAUGE',
                'value': cache[metric] or 0,
            }

    return metrics


def tpstats_metrics():
    now = int(time())
    url = 'http://localhost:8778/jolokia/read/org.apache.cassandra.concurrent:*'
    try:
        pools = json.loads(urllib2.urlopen(url).read())['value']
    except Exception, e:
        sys.stderr.write("Error while fetching list of beans for tpstats: %s" % e)
        return None

    metrics = {}
    for mbean, values in pools.items():
        pool = mbean.split('=')[-1]
        for metric, datatype in (('ActiveCount', 'GAUGE'), ('PendingTasks', 'GAUGE'), ('CompletedTasks', 'COUNTER')):
            metrics['%s_%s' % (pool, metric)] = {
                'ts': now,
                'type': datatype,
                'value': values[metric]
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
    now = int(time())
    try:
        keyspace = file('/etc/metartg_cassandra_keyspace', 'r').read().strip('\r\n\t ')
    except:
        keyspace = 'Underdog_Records'

    url = 'http://localhost:8778/jolokia/read/org.apache.cassandra.db:keyspace=%s,type=DynamicEndpointSnitch/Scores' % keyspace
    try:
        scores = json.loads(urllib2.urlopen(url).read())['value']
    except Exception, e:
        sys.stderr.write("Error while fetching DES scores: %s" % e)
        return None

    metrics = {}
    for endpoint, score in scores.items():
        endpoint = endpoint.lstrip('/')
        try:
            metrics[endpoint] = {
                'ts': now,
                'type': 'GAUGE',
                'value': float(score),
            }
        except TypeError:
            pass
    return metrics


def memory_metrics():
    now = int(time())
    url = 'http://localhost:8778/jolokia/read/java.lang:type=Memory'
    try:
        results = json.loads(urllib2.urlopen(url).read())['value']
    except Exception, e:
        sys.stderr.write("Error while fetching memory metrics: %s" % e)
        return None

    mapping = {
        'jvm.heap.committed': ('HeapMemoryUsage', 'committed'),
        'jvm.heap.used': ('HeapMemoryUsage', 'used'),
        'jvm.nonheap.committed': ('NonHeapMemoryUsage', 'committed'),
        'jvm.nonheap.used': ('NonHeapMemoryUsage', 'used'),
    }

    metrics = {}
    for name, (memory_type, metric) in mapping.items():
        metrics[name] = {
            'ts': now,
            'type': 'GAUGE',
            'value': results[memory_type][metric],
        }

    return metrics


def compaction_metrics():
    now = int(time())
    url = 'http://localhost:8778/jolokia/read/org.apache.cassandra.db:type=CompactionManager'
    try:
        results = json.loads(urllib2.urlopen(url).read())['value']
    except Exception, e:
        sys.stderr.write("Error while fetching compaction metrics: %s " % e)
        return None

    metrics = {}

    if results['PendingTasks']:
        metrics['tasks.pending'] = {
            'ts': now,
            'type': 'GAUGE',
            'value': results['PendingTasks']
        }

    if results['BytesTotalInProgress']:
        metrics.update({
            'bytes.compacting': {
                'ts': now,
                'type': 'GAUGE',
                'value': results['BytesTotalInProgress'],
            },
            'bytes.remaining': {
                'ts': now,
                'type': 'GAUGE',
                'value': results['BytesCompacted'],
            }
        })

    return metrics or None


def commitlog_metrics():
    now = int(time())
    url = 'http://localhost:8778/jolokia/read/org.apache.cassandra.db:type=Commitlog'
    try:
        results = json.loads(urllib2.urlopen(url).read())['value']
    except Exception, e:
        sys.stderr.write("Error while fetching streaming metrics: %s " % e)
        return None

    metrics = {
        'tasks.completed': {
            'ts': now,
            'type': 'COUNTER',
            'value': results['CompletedTasks'],
        },
        'tasks.pending': {
            'ts': now,
            'type': 'GAUGE',
            'value': results['PendingTasks'],
        },
    }

    return metrics


def streaming_metrics():
    now = int(time())
    url = 'http://localhost:8778/jolokia/read/org.apache.cassandra.streaming:type=StreamingService'
    try:
        results = json.loads(urllib2.urlopen(url).read())['value']
    except Exception, e:
        sys.stderr.write("Error while fetching streaming metrics: %s " % e)
        return None

    metrics = {}
    pattern = re.compile("Receiving from:\n(?P<sources>.*?)\nSending to:\n(?P<destinations>.*?)\n", re.S)
    match = pattern.search(results['Status'])
    if not match:
        return None

    values = match.groupdict()
    if values['sources']:
        metrics.update({
            'streaming.from': {
                'ts': now,
                'type': 'GAUGE',
                'value': len(values['sources'].strip().split('\n')),
            }
        })
    if values['destinations']:
        metrics.update({
            'streaming.to': {
                'ts': now,
                'type': 'GAUGE',
                'value': len(values['destinations'].strip().split('\n')),
            }
        })

    return metrics


def connection_metrics():
    command = subprocess.Popen("netstat -an | grep ESTABLISHED | awk '{print $4}' | grep \":9160$\" | wc -l", shell=True, stdout=subprocess.PIPE)
    count = int(command.communicate()[0].strip())
    return {
        'connections.open': {
            'ts': int(time()),
            'type': 'GAUGE',
            'value': count,
        }
    }


def run_check(callback):
    callback('cassandra_tpstats', tpstats_metrics())
    callback('cassandra_sstables', sstables_metrics())
    callback('cassandra_scores', scores_metrics())
    callback('cassandra_memory', memory_metrics())
    callback('cassandra_cfstats_cache', cfstats_cache_metrics())
    callback('cassandra_compaction', compaction_metrics())
    callback('cassandra_commitlog', commitlog_metrics())
    callback('cassandra_streaming', streaming_metrics())
    callback('cassandra_connection', connection_metrics())


if __name__ == '__main__':
    print json.dumps(scores_metrics(), indent=2)
    print json.dumps(cfstats_cache_metrics(), indent=2)
    print json.dumps(tpstats_metrics(), indent=2)
    print json.dumps(sstables_metrics(), indent=2)
    print json.dumps(memory_metrics(), indent=2)
    print json.dumps(compaction_metrics(), indent=2)
    print json.dumps(commitlog_metrics(), indent=2)
    print json.dumps(streaming_metrics(), indent=2)
    print json.dumps(connection_metrics(), indent=2)


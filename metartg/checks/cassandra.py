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
    url = 'http://localhost:8778/jolokia/list/org.apache.cassandra.concurrent'
    try:
        beans = [x for x in json.loads(urllib2.urlopen(url).read())['value'].keys()]
    except Exception, e:
        sys.stderr.write("Error while fetching list of beans for tpstats: %s" % e)
        return None

    metrics = {}
    for bean in beans:
        try:
            url = 'http://localhost:8778/jolokia/read/%s:%s' % ('org.apache.cassandra.concurrent', bean)
            values = json.loads(urllib2.urlopen(url).read())['value']
            name = bean.split('=', 1)[1]
            for metric, datatype in (('ActiveCount', 'GAUGE'), ('PendingTasks', 'GAUGE'), ('CompletedTasks', 'COUNTER')):
                metrics['%s_%s' % (name, metric)] = {
                    'ts': now,
                    'type': datatype,
                    'value': values[metric]
                }
        except Exception, e:
            sys.stderr.write("Error while fetching tpstats for %s: %s" % (bean, e))
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
        metrics[endpoint] = {
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
    print json.dumps(cfstats_cache_metrics(), indent=2)
    print json.dumps(tpstats_metrics(), indent=2)
    print json.dumps(sstables_metrics(), indent=2)


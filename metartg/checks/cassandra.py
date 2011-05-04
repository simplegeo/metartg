#!/usr/bin/env python
import simplejson as json
from time import time
import subprocess
import os

def tpstats_metrics():
    p = subprocess.Popen([
        '/usr/bin/java',
        '-jar', '/usr/share/metartg/contrib/GenericJMXLogJSON.jar',
        'localhost', '8080', 'org.apache.cassandra.concurrent:*',
    ], stdout=subprocess.PIPE)
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

    for keyspace in os.listdir('/mnt/var/lib/penelope/data'):
        now = int(time())
        sizes = {}
        for filename in os.listdir('/mnt/var/lib/penelope/data/' + keyspace):
            if not filename.endswith('-Data.db'):
                continue

            columnfamily = filename.split('-', 1)[0]
            if not columnfamily in sizes:
                sizes[columnfamily] = []

            st = os.stat('/mnt/var/lib/penelope/data/%s/%s' % (keyspace, filename))
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

    return metrics

def run_check(callback):
    callback('cassandra_tpstats', tpstats_metrics())
    callback('cassandra_sstables', sstables_metrics())

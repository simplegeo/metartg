#!/usr/bin/env python
import simplejson as json
from time import time
import subprocess

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

def run_check(callback):
    callback('cassandra_tpstats', tpstats_metrics())

#!/usr/bin/env python
from time import time

def rabbitmq_metrics():
    p = subprocess.Popen(['/usr/sbin/rabbitmqctl', '-q', 'list_queues', '-p', 'simplegeo'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    now = int(time())
    metrics = {}
    for line in stdout.strip().split('\n'):
        queue, count = line.split('\t', 1)
        metrics[queue] = {
            'ts': now,
            'type': 'GAUGE',
            'value': int(count),
        }
    return metrics

    
def run_check(callback):
    callback('rabbitmq', rabbitmq_metrics())

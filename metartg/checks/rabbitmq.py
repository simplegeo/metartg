#!/usr/bin/env python
from time import time
import subprocess

def rabbitmq_metrics():
    p = subprocess.Popen(['/usr/sbin/rabbitmqctl', '-q', 'list_queues', '-p', 'simplegeo', 'name', 'messages', 'messages_unacknowledged'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()

    now = int(time())
    metrics = {}
    for line in stdout.strip().split('\n'):
        queue, count, unack = line.split('\t', 2)
        metrics[queue] = {
            'ts': now,
            'type': 'GAUGE',
            'value': int(count),
        }
        metrics[queue + '_unack'] = {
            'ts': now,
            'type': 'GAUGE',
            'value': int(unack),
        }
    return metrics

    
def run_check(callback):
    callback('rabbitmq', rabbitmq_metrics())

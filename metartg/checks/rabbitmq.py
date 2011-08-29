#!/usr/bin/env python
from time import time
import subprocess
import json

import logging

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


def rabbitmq_rate_metrics():
    p = subprocess.Popen(['/usr/bin/curl', '-s', '-u', 'simplegeo:simplegeo', 'http://localhost:55672/api/queues/simplegeo'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    try:
        queues = json.loads(stdout)
    except ValueError:
        return None

    stats = ['deliver', 'deliver_get', 'ack', 'publish']
    now = int(time())
    metrics = {}
    for queue in queues:
        if 'message_stats' in queue:
            for statname in stats:
                try:
                    statdetails = '%s_details' % statname
                    if isinstance(queue['message_stats'], dict) and statdetails in  queue['message_stats'] and isinstance(queue['message_stats'][statdetails], dict):
                        metrics['%s_%s_rate' % (queue['name'], statname)] = {
                            'ts': now,
                            'type': 'GAUGE',
                            'value': long(queue['message_stats'][statdetails]['rate']),
                        }
                except Exception, e:
                    logging.error('Failure getting metrics for queue %s:' % queue['name'], e)
    return metrics


def run_check(callback):
    callback('rabbitmq', rabbitmq_metrics())
    rates_result = rabbitmq_rate_metrics()
    if rates_result:
        callback('rabbitmq', rates_result)


if __name__ == '__main__':
    print json.dumps(rabbitmq_metrics(), indent=2)
    print json.dumps(rabbitmq_rate_metrics(), indent=2)


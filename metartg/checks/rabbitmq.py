#!/usr/bin/env python
from time import time
import subprocess
import json


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

    now = int(time())
    metrics = {}
    for queue in queues:
        if 'message_stats' in queue:
            # rate at which msgs are being delivered to consumers
            metrics['%s_deliver_rate' % queue['name']] = {
                'ts': now,
                'type': 'GAUGE',
                'value': long(queue['message_stats']['deliver_details']['rate']),
            }
            # not exactly sure what the difference is between deliver and deliver_get, but graphing it anyway in the event we want it at some point
            metrics['%s_deliver_get_rate' % queue['name']] = {
                'ts': now,
                'type': 'GAUGE',
                'value': long(queue['message_stats']['deliver_get_details']['rate']),
            }
            # rate at which msgs are being acknowledged
            metrics['%s_ack_rate' % queue['name']] = {
                'ts': now,
                'type': 'GAUGE',
                'value': long(queue['message_stats']['ack_details']['rate']),
            }
            # rate at which messages are coming in
            metrics['%s_pub_rate' % queue['name']] = {
                'ts': now,
                'type': 'GAUGE',
                'value': long(queue['message_stats']['publish_details']['rate']),
            }
    return metrics


def run_check(callback):
    callback('rabbitmq', rabbitmq_metrics())
    rates_result = rabbitmq_rate_metrics()
    if rates_result:
        callback('rabbitmq', rates_result)


if __name__ == '__main__':
    print json.dumps(rabbitmq_metrics(), indent=2)
    print json.dumps(rabbitmq_rate_metrics(), indent=2)


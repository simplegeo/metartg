#!/usr/bin/env python
from time import time
import subprocess


def cpu_metrics():
    metrics = {}

    now = int(time())
    p = subprocess.Popen(['/usr/bin/mpstat', '1', '1'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    stdout = stdout.split('\n')[-2]
    stdout = [x for x in stdout.split(' ')[1:] if x]
    stdout = [int(float(x)) for x in stdout[1:]]
    for i, name in enumerate(('user', 'nice', 'sys', 'iowait', 'irq', 'soft', 'steal', 'guest', 'idle')):
        metrics[name] = {
            'ts': now,
            'type': 'GAUGE',
            'value': stdout[i],
        }

    return metrics


def mem_metrics():
    metrics = {}

    p = subprocess.Popen(['/usr/bin/vmstat', '-s'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    now = int(time())

    for line in stdout.split('\n'):
        line = line.strip('\r\n\t ')
        if line.find(' K ') == -1:
            continue
        count, label = line.split(' K ', 1)
        label = label.replace(' ', '_')
        metrics[label] = {
            'ts': now,
            'type': 'GAUGE',
            'value': int(count),
        }
    return metrics


def network_metrics():
    metrics = {}
    now = int(time())
    fd = file('/proc/net/dev', 'r')
    head1 = fd.next()
    head2 = fd.next()

    for line in fd:
        line = line.strip('\n ')
        iface, line = line.split(':', 1)
        if not iface.startswith('eth'):
            continue
        line = [x for x in line.split(' ') if x]
        metrics.update({
            '%s_rx_bytes' % iface: {
                'ts': now,
                'type': 'COUNTER',
                'value': int(line[0]),
            },
            '%s_tx_bytes' % iface: {
                'ts': now,
                'type': 'COUNTER',
                'value': int(line[8]),
            },
            '%s_rx_packets' % iface: {
                'ts': now,
                'type': 'COUNTER',
                'value': int(line[1]),
            },
            '%s_tx_packets' % iface: {
                'ts': now,
                'type': 'COUNTER',
                'value': int(line[9]),
            }
        })
    return metrics


def run_check(callback):
    callback('cpu', cpu_metrics())
    callback('memory', mem_metrics())
    callback('network', network_metrics())

if __name__ == '__main__':
    import json
    print json.dumps(network_metrics(), indent=2)

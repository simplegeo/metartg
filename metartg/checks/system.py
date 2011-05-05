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
            'value': count,
        }
    return metrics


def run_check(callback):
    callback('cpu', cpu_metrics())
    callback('memory', mem_metrics())

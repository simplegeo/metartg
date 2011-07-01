#!/usr/bin/env python
from collections import namedtuple
from time import time
import subprocess
import simplejson as json
import os


IOStat = namedtuple('IOStat', 'rrqm wrqm reads writes rkb wkb avgrq avgq await svctm util')
def disk_metrics():
    p = subprocess.Popen(['/usr/bin/iostat', '-x', '-d', '-k', '1', '2'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    now = int(time())

    metrics = {}
    for line in stdout.rsplit('\n\n', 2)[1].split('\n')[1:]:
        line = [x for x in line.split(' ') if x]
        device = line[0]
        if not device in ('sda1', 'md0'):
            continue
        iostat = IOStat(*line[1:])._asdict()
        for field in iostat:
            metrics['%s.%s' % (device, field)] = {
                'ts': now,
                'type': 'GAUGE',
                'value': float(iostat[field]),
            }
    return metrics


def disk_space_metrics():
    if not os.path.exists('/proc/mounts'):
        return {}

    metrics = {}
    for l in file('/proc/mounts'):
        if l[0] == '/':
            l = l.split()
            device = l[0]
            path = l[1]
            stats = os.statvfs(path)
            device_metrics = {
                    'preferred_block_size': int(stats.f_bsize),
                    'fundamental_block_size': int(stats.f_blocks),
                    'total_blocks': int(stats.f_blocks),
                    'free_blocks' : int(stats.f_bfree),
                    'available_blocks' : int(stats.f_bavail),
                    'total_file_nodes' : int(stats.f_files),
                    'free_file_nodes' : int(stats.f_ffree)
                    }

            now = int(time())
            for name, value in device_metrics.items():
                metrics['%s.%s' % (device, name)] = {
                        'value': value,
                        'ts': now,
                        'type': 'GAUGE'
                        }
    return metrics


def run_check(callback):
    callback('disk', disk_metrics())
    callback('disk-space', disk_space_metrics())

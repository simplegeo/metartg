#!/usr/bin/env python
from collections import namedtuple
from time import time
import subprocess
import simplejson as json
import os, sys


DiskStat = namedtuple('DiskStat', 'major_dev_num minor_dev_num device reads reads_merged sectors_read ms_reading writes writes_merged sectors_written ms_writing current_iops ms_doing_io weighted_ms_doing_io')


def disk_metrics():
    now = int(time())

    metrics = {}
    for line in file('/proc/diskstats', 'r'):
        line = [x for x in line.split(' ') if x]
        line = DiskStat(*line)._asdict()

        for field in line:
            if field in ('major_dev_num', 'minor_dev_num', 'device',
                         'ms_reading', 'ms_writing', 'ms_doing_io',
                         'reads_merged', 'writes_merged'):
                continue

            if not line['device'] in ('sda', 'md0'):
                continue

            if field in ('current_iops', 'weighted_ms_doing_io'):
                metric_type = 'GAUGE'
            else:
                metric_type = 'COUNTER'

            metrics['%s.%s' % (line['device'], field)] = {
                'ts': now,
                'type': metric_type,
                'value': int(line[field]),
            }

    return metrics


def disk_space_metrics():
    if not os.path.exists('/proc/mounts'):
        sys.stderr.write("Couldn't retreive list of mounts in disk space checks")
        return None

    metrics = {}
    for l in file('/proc/mounts'):
        if l[0] == '/':
            l = l.split()
            device = l[0]
            path = l[1]
            stats = os.statvfs(path)
            device_metrics = {
                    'free_space' : int(stats.f_bfree * stats.f_frsize),
                    'used_nodes' : int(stats.f_files),
                    'free_nodes' : int(stats.f_ffree)
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

if __name__ == '__main__':
    print json.dumps(disk_metrics(), indent=2)
    print json.dumps(disk_space_metrics(), indent=2)


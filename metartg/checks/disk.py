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

        if line['device'] in ('dm-0', 'md0'):
            line['device'] = 'raid0'

        if not line['device'] in ('sda1', 'raid0'):
            continue

        for field in line:
            if field in ('major_dev_num', 'minor_dev_num', 'device',
                         'ms_reading', 'ms_writing', 'ms_doing_io',
                         'reads_merged', 'writes_merged'):
                continue

            value = int(line[field])

            if field in ('current_iops', 'weighted_ms_doing_io'):
                metric_type = 'GAUGE'
            else:
                metric_type = 'COUNTER'

            if field in ('sectors_read', 'sectors_written'):
                field = 'bytes_' + field.split('_', 1)[1]
                value *= 1024

            metrics['%s.%s' % (line['device'], field)] = {
                'ts': now,
                'type': metric_type,
                'value': value,
            }

        # Use ext4 stats if we can, as they're more accurate
        if os.path.exists('/sys/fs/ext4/%s/lifetime_write_kbytes' %
            line['device']):
            value = int(file('/sys/fs/ext4/%s/lifetime_write_kbytes' % 
                        line['device']).read()) * 1024
            metrics['%s.bytes_written' % line['device']] = {
                'ts': now,
                'type': 'COUNTER',
                'value': value,
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
                    'free_space' : int(stats.f_bfree * stats.f_frsize / 1000000000),
                    'used_space' : int((stats.f_blocks - stats.f_bfree) * stats.f_bsize / 1000000000),
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


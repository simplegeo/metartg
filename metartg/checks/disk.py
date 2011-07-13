#!/usr/bin/env python
from collections import namedtuple
from time import time
import subprocess
import simplejson as json
import os, sys


IOStat = namedtuple('IOStat', 'rrqm wrqm reads writes rkb wkb avgrq avgq await svctm util')
def disk_metrics():
    p = subprocess.Popen(['/usr/bin/iostat', '-x', '-d', '-k', '1', '2'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    now = int(time())

    metrics = {}
    measured_devices = {}
    for line in stdout.rsplit('\n\n', 2)[1].split('\n')[1:]:
        line = [x for x in line.split(' ') if x]
        device = line[0]
        iostat = IOStat(*line[1:])._asdict()
        if device == 'dm-0':
            measured_devices['raid0'] = iostat
        elif device == 'xvdap1':
            measured_devices['sda1'] = iostat
        else:
            measured_devices[device] = iostat

    # approximating hack to make raid0 stats work for mdadm devices
    if not 'raid0' in measured_devices and 'md0' in measured_devices:
        total_avgq = 0.0
        total_await = 0.0
        total_svctm = 0.0
        total_util = 0.0
        devices = 0.0
        for device, iostat in measured_devices.items():
            if device in ('xvdb', 'xvdc', 'xvdd', 'xvde'):
                devices += 1.0
                total_avgq += float(iostat['avgq'])
                total_await += float(iostat['await'])
                total_svctm += float(iostat['svctm'])
                total_util += float(iostat['util'])

        raid_stats = measured_devices['md0']
        raid_stats['avgq'] = total_avgq
        raid_stats['await'] = total_await / devices
        raid_stats['svctm'] = total_await / devices
        raid_stats['util'] = total_util / devices

        measured_devices['raid0'] = raid_stats

    for device in ('raid0', 'sda1'):
        iostat = measured_devices[device]
        for field in iostat:
            metrics['%s.%s' % (device, field)] = {
                'ts': now,
                'type': 'GAUGE',
                'value': float(iostat[field]),
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


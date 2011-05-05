#!/usr/bin/env python
from collections import namedtuple
from time import time
import subprocess
import simplejson as json


IOStat = namedtuple('IOStat', 'rrqm wrqm reads writes rkb wkb avgrq avgq await svctm util')
def disk_metrics():
    p = subprocess.Popen(['/usr/bin/iostat', '-x', '-d', '-k', '1', '2'], stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    now = int(time())

    metrics = {}
    for line in stdout.rsplit('\n\n', 2)[1].split('\n')[1:]:
        line = [x for x in line.split(' ') if x]
        device = line[0]
        iostat = IOStat(*line[1:])._asdict()
        for field in iostat:
            metrics['%s.%s' % (device, field)] = {
                'ts': now,
                'type': 'GAUGE',
                'value': float(iostat[field]),
            }
    return metrics


def run_check(callback):
    callback('disk', disk_metrics())

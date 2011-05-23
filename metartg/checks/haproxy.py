#!/usr/bin/env python
from time import time
import urllib

HAPROXY_METRICS = {
    'stot': 'COUNTER',
    'ereq': 'GAUGE',
    'eresp': 'GAUGE',
    'econ': 'GAUGE',
    'chkfail': 'COUNTER',
    'chkdown': 'COUNTER',
    'req_rate': 'GAUGE',
    'req_tot': 'COUNTER',
    'cli_abrt': 'COUNTER',
    'srv_abrt': 'COUNTER',
}

def haproxy_metrics():
    resp = urllib.urlopen('http://localhost:22222/;csv')
    csv = (x.split(',') for x in resp.read().split('\n') if x)
    now = int(time())

    fields = csv.next()
    metrics = {}
    for line in csv:
        svname = line[1]
        if svname in ('FRONTEND', 'BACKEND'):
            continue
        for i, value in enumerate(line):
            field = fields[i]
            if field in HAPROXY_METRICS:
                metrictype = HAPROXY_METRICS[field]
                if not value:
                    value = 0
                metrics['%s_%s' % (svname, field)] = {
                    'ts': now,
                    'type': metrictype,
                    'value': int(value),
                }
    return metrics
            

def run_check(callback):
    metrics = haproxy_metrics()
    callback('haproxy', metrics)


if __name__ == '__main__':
    import json
    print json.dumps(haproxy_metrics(), indent=2)

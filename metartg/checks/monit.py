#!/usr/bin/env python
from xml.etree import ElementTree
from decimal import Decimal
from time import time
import urllib2


def monit_metrics():
    req = urllib2.Request('http://localhost:2812/_status?format=xml', headers={
        'Authorization': 'Basic %s' % 'admin:monit'.encode('base64'),
    })
    resp = urllib2.urlopen(req)
    tree = ElementTree.parse(resp)
    now = int(time())
    results = {}
    for service in tree.getiterator('service'):
        if service.findtext('status') != '0' or service.attrib['type'] != '3':
            continue
        name = service.findtext('name')
        memory = service.findtext('memory/kilobytetotal')
        cpu = service.findtext('cpu/percenttotal')

        results['%s_memory' % name] = {
            'ts': now,
            'type': 'GAUGE',
            'value': int(memory),
        }
        results['%s_cpu' % name] = {
            'ts': now,
            'type': 'GAUGE',
            'value': Decimal(cpu),
        }
    return results


def run_check(callback):
    metrics = monit_metrics()
    callback('monit', metrics)


if __name__ == '__main__':
    import simplejson as json
    print json.dumps(monit_metrics(), indent=2, use_decimal=True, sort_keys=True)

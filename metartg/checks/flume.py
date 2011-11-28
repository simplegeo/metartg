#!/usr/bin/env python
import simplejson as json
import urllib2
import time

def flume_metrics():
    resp = urllib2.urlopen('http://localhost:35862/node/reports')
    physical_node = json.load(resp)

    results = {}
    now = int(time.time())
    for key, value in physical_node['jvmInfo'].items():
        if not key.startswith('mem.'):
            continue
        results['jvm.%s' % key] = {
            'ts': now,
            'type': 'GAUGE',
            'value': value,
        }

    for name, endpoint in physical_node['logicalnodes'].items():
        if not name.startswith('sg_api'):
            continue
        nodetype = name.split('-', 1)[0]

        now = int(time.time())
        resp = urllib2.urlopen(endpoint)
        logical_node = json.load(resp)

        if nodetype == 'sg_api_writer_api':
            results['%s.events' % nodetype] = {
                'ts': now,
                'type': 'COUNTER',
                'value': logical_node['source.LazyOpenSource.CollectorSource.number of events'],
            }
            results['%s.appendSuccess' % nodetype] = {
                'ts': now,
                'type': 'COUNTER',
                'value': logical_node['sink.LazyOpenDecorator.JSONExtractor.DateExtractor.MaskDecorator.Collector.AckChecksumChecker.InsistentAppend.StubbornAppend.appendSuccess'],
            }
            results['%s.appendFails' % nodetype] = {
                'ts': now,
                'type': 'COUNTER',
                'value': logical_node['sink.LazyOpenDecorator.JSONExtractor.DateExtractor.MaskDecorator.Collector.AckChecksumChecker.InsistentAppend.StubbornAppend.appendFails'],
            }
            results['%s.appendRecovers' % nodetype] = {
                'ts': now,
                'type': 'COUNTER',
                'value': logical_node['sink.LazyOpenDecorator.JSONExtractor.DateExtractor.MaskDecorator.Collector.AckChecksumChecker.InsistentAppend.StubbornAppend.appendRecovers'],
            }

        if nodetype == 'sg_api_tailer_api':
            results['%s.events' % nodetype] = {
                'ts': now,
                'type': 'COUNTER',
                'value': logical_node['source.LazyOpenSource.TailDirSource.number of events'],
            }

    return results


def run_check(callback):
    metrics = flume_metrics()
    callback('flume', metrics)


if __name__ == '__main__':
    print json.dumps(flume_metrics(), indent=2, use_decimal=True, sort_keys=True)

#!/usr/bin/env python

import socket
import pyes
import simplejson as json
from time import time
from pprint import pprint

def shards():
    """ shards stats """
    metrics = {}

    try:
        es = pyes.ES(['localhost:9200'])
        health = es.cluster_health()
    except Exception, ex:
        print "status alert Unable to fetch cluster health " \
            "on node %s: %s: %s" % (
            socket.gethostname(), type(ex).__name__, str(ex))
        return

    now = int(time())
    for key in ('number_of_nodes', 'unassigned_shards',
                'active_primary_shards', 'active_shards',
                'initializing_shards', 'number_of_data_nodes'):
        metrics[key] = {
            'ts': now,
            'type': 'GAUGE',
            'value': health[key],
        }

    return metrics


def jvm_memory():
    """ jvm memory stats """
    metrics = {}
    mapping = {
        'jvm.heap.committed': 'heap_committed_in_bytes',
        'jvm.heap.used': 'heap_used_in_bytes',
        'jvm.nonheap.committed': 'non_heap_committed_in_bytes',
        'jvm.nonheap.used': 'non_heap_used_in_bytes',
    }
    now = int(time())

    try:
        es = pyes.ES(['localhost:9200'])
        stats = es.cluster_stats(nodes=['_local'])
    except Exception, ex:
        print "status alert Unable to fetch cluster health " \
            "on node %s: %s: %s" % (
            socket.gethostname(), type(ex).__name__, str(ex))
        return

    node = stats['nodes'].keys()[0]
    stats = stats['nodes'][node]['jvm']['mem']

    for name,stat in mapping.items():
        metrics[name] = {
            'ts': now,
            'type': 'GAUGE',
            'value': stats[stat]
        }

    return metrics


def jvm_gc():
    """ jvm garbage collection stats """
    metrics = {}
    mapping = {
        'gc.collection.count': 'collection_count',
        'gc.collection.time': 'collection_time_in_millis',
    }
    now = int(time())

    try:
        es = pyes.ES(['localhost:9200'])
        stats = es.cluster_stats(nodes=['_local'])
    except Exception, ex:
        print "status alert Unable to fetch cluster health " \
            "on node %s: %s: %s" % (
            socket.gethostname(), type(ex).__name__, str(ex))
        return

    node = stats['nodes'].keys()[0]
    stats = stats['nodes'][node]['jvm']['gc']

    for name,stat in mapping.items():
        metrics[name] = {
            'ts': now,
            'type': 'DERIVE',
            'value': stats[stat]
        }

    return metrics


def run_check(callback):
    callback('elasticsearch-shards', shards())
    callback('elasticsearch-memory', jvm_memory())
    callback('elasticsearch-gc', jvm_gc())


if __name__ == '__main__':
    print json.dumps(shards(), indent=2)
    print json.dumps(jvm_memory(), indent=2)
    print json.dumps(jvm_gc(), indent=2)


#!/usr/bin/env python

import socket
import pyes
import json
import httplib2
import simplejson as json
from time import time
from pprint import pprint


#
# TODO: Remove the dependency on pyes
#

def get_client():
    return pyes.ES(['localhost:9200'])


def alert(msg, ex):
    print "status alert %s %s: %s: %s" % (socket.gethostname(), type(ex).__name__, str(ex))


def flatten(l):
    return reduce(lambda x,y: x+[y] if type(y) != list else x+flatten(y), l,[])


http_client = httplib2.Http()

def curl(endpoint, method="GET"):
     resp, content = http_client.request(endpoint, method)
     return json.loads(content)


def shards():
    """ shards stats """
    metrics = {}

    try:
        es = get_client()
        health = es.cluster_health()
    except Exception, ex:
        alert("Unable to fetch cluster health")
    else:
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

    try:
        es = get_client()
        stats = es.cluster_stats(nodes=['_local'])
    except Exception, ex:
        alert("Unable to fetch cluster health", ex)
    else:
        node = stats['nodes'].keys()[0]
        stats = stats['nodes'][node]['jvm']['mem']

        now = int(time())

        for name,stat in mapping.items():
            metrics[name] = {
                'ts': now,
                'type': 'GAUGE',
                'value': stats[stat]
            }

        return metrics


def jvm_gc():
    """ jvm garbage collection stats """
    mapping = {
        'gc.collection.count': 'collection_count',
        'gc.collection.time': 'collection_time_in_millis',
    }

    try:
        es = get_client()
        stats = es.cluster_stats(nodes=['_local'])
    except Exception, ex:
        alert("Unable to fetch cluster health")
    else:
        metrics = {}

        node = stats['nodes'].keys()[0]
        stats = stats['nodes'][node]['jvm']['gc']

        now = int(time())

        for name,stat in mapping.items():
            metrics[name] = {
                'ts': now,
                'type': 'DERIVE',
                'value': stats[stat]
            }

        return metrics


def segments():
    """ segment stats """

    try:
        stats = curl('http://localhost:9200/_segments')
    except Exception, ex:
        alert("Unable to fetch segments")
    else:
        metrics = {}
        for index_name,index in stats['indices'].items():

            count_name = '%s.segment.count' % (index_name, )
            size_name = '%s.segment.size' % (index_name, )
            docs_name = '%s.segment.docs' % (index_name, )

            metrics[count_name] = 0
            metrics[size_name] = 0
            metrics[docs_name] = 0

            for shard in flatten(index['shards'].values()):
                segments = shard['segments']
                metrics[count_name] += len(segments)

                for segment in segments.values():
                    metrics[docs_name] += segment['num_docs']
                    metrics[size_name] += segment['size_in_bytes']

            metrics[size_name] = metrics[size_name] / 1000000

        return metrics


def run_check(callback):
    callback('elasticsearch-shards', shards())
    callback('elasticsearch-memory', jvm_memory())
    callback('elasticsearch-gc', jvm_gc())
    callback('elasticsearch-segments', segments())


if __name__ == '__main__':
    print json.dumps(shards(), indent=2)
    print json.dumps(jvm_memory(), indent=2)
    print json.dumps(jvm_gc(), indent=2)
    print json.dumps(segments(), indent=2)


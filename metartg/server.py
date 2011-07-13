from xml.etree import ElementTree
from time import time
from glob import glob
#import multiprocessing
import eventlet.green.subprocess as subprocess
import socket
import os.path
import sys
import os

import simplejson as json
import logging
import clustohttp
import eventlet
import memcache
import bottle
import jinja2

redis = eventlet.import_patched('redis')

STATIC_PATH = '/usr/share/metartg/static'
TEMPLATE_PATH = '/usr/share/metartg/templates'

bottle.debug(True)
application = bottle.default_app()
env = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATE_PATH))
cache = memcache.Client(['127.0.0.1:11211'])
gpool = eventlet.GreenPool(200)
clusto = clustohttp.ClustoProxy('http://clusto.simplegeo.com/api')
#rrdqueue = eventlet.Queue()
db = redis.Redis()

class RedisQueue(object):
    def __init__(self, key):
        self.key = key

    def put(self, obj):
        db.lpush(self.key, json.dumps(obj))

    def get(self):
        return json.loads(db.brpop(self.key)[1])

    def qsize(self):
        return db.llen(self.key)


rrdqueue = RedisQueue('rrdqueue')


RRDPATH = '%(host)s/%(service)s/%(metric)s.rrd'

RRD_GRAPH_DEFS = {
    'system-memory': [
        'DEF:mem_free=%(rrdpath)s/memory/free_memory.rrd:sum:AVERAGE',
        'DEF:mem_total=%(rrdpath)s/memory/total_memory.rrd:sum:AVERAGE',
        'DEF:mem_buffers=%(rrdpath)s/memory/buffer_memory.rrd:sum:AVERAGE',
        'CDEF:gb_mem_free=mem_free,1024,*',
        'CDEF:gb_mem_total=mem_total,1024,*',
        'CDEF:gb_mem_buffers=mem_buffers,1024,*',
        'CDEF:t_mem_used=gb_mem_total,gb_mem_free,-',
        'CDEF:gb_mem_used=t_mem_used,gb_mem_buffers,-',
        'LINE:gb_mem_total#FFFFFF:Total memory\\l',
        'AREA:gb_mem_used#006699:Used memory\\l',
    ],
    'network-bytes': [
        'DEF:rx_bytes=%(rrdpath)s/network/eth0_rx_bytes.rrd:sum:AVERAGE',
        'DEF:tx_bytes=%(rrdpath)s/network/eth0_tx_bytes.rrd:sum:AVERAGE',
        'LINE:rx_bytes#006699:rx bytes\\l',
        'LINE:tx_bytes#996600:tx bytes\\l',
    ],
    'network-packets': [
        'DEF:rx_packets=%(rrdpath)s/network/eth0_rx_packets.rrd:sum:AVERAGE',
        'DEF:tx_packets=%(rrdpath)s/network/eth0_tx_packets.rrd:sum:AVERAGE',
        'LINE:rx_packets#006699:rx packets\\l',
        'LINE:tx_packets#996600:tx packets\\l',
    ],
    'system-cpu': [
        'DEF:cpu_user=%(rrdpath)s/cpu/user.rrd:sum:AVERAGE',
        'DEF:cpu_system=%(rrdpath)s/cpu/sys.rrd:sum:AVERAGE',
        'DEF:cpu_nice=%(rrdpath)s/cpu/nice.rrd:sum:AVERAGE',
        'AREA:cpu_system#FF6600:CPU system\\l:STACK',
        'AREA:cpu_nice#FFCC00:CPU nice\\l:STACK',
        'AREA:cpu_user#FFFF66:CPU user\\l:STACK',
    ],
    #'io': [
    #    'DEF:cpu_wio=%(rrdpath)s/cpu/iowait.rrd:sum:AVERAGE',
    #    'LINE:cpu_wio#EA8F00:CPU iowait\\l',
    #],
    'redis-memory': [
        'DEF:memory=%(rrdpath)s/redis/used_memory.rrd:sum:AVERAGE',
        'LINE:memory#EA8F00:Redis memory\\l',
    ],
    'redis-connections': [
        'DEF:connected_clients=%(rrdpath)s/redis/connected_clients.rrd:sum:AVERAGE',
        'DEF:connected_slaves=%(rrdpath)s/redis/connected_slaves.rrd:sum:AVERAGE',
        'DEF:blocked_clients=%(rrdpath)s/redis/blocked_clients.rrd:sum:AVERAGE',
        'LINE:connected_clients#35962B:Connected clients\\l',
        'LINE:connected_slaves#0000FF:Connected slaves\\l',
        'LINE:blocked_clients#FF0000:Blocked clients\\l',
    ],
    'elb-requests': [
        'DEF:request_count=%(rrdpath)s/elb/request_count.rrd:sum:AVERAGE',
        'LINE:request_count#00FF00:Requests per minute\\l',
    ],
    'elb-latency': [
        #'DEF:min=%(rrdpath)s/elb/latency_min.rrd:sum:AVERAGE',
        #'DEF:max=%(rrdpath)s/elb/latency_max.rrd:sum:AVERAGE',
        'DEF:avg=%(rrdpath)s/elb/latency_avg.rrd:sum:AVERAGE',
        #'LINE:min#FF0000:Upstream latency (min)\\l',
        #'LINE:max#0000FF:Upstream latency (max)\\l',
        'LINE:avg#3333FF:Upstream latency (avg)\\l',
    ],
    'metartg-processed': [
        'DEF:processed=%(rrdpath)s/metartg/processed.rrd:sum:AVERAGE',
        'DEF:queued=%(rrdpath)s/metartg/queued.rrd:sum:AVERAGE',
        'LINE:processed#00FF00:Processed metrics\\l',
        'LINE:queued#FF0000:Queued metrics\\l',
    ],
}

RRD_GRAPH_OPTIONS = {
    'system-cpu': ['--upper-limit', '100.0'],
    #'io': ['--upper-limit', '100.0']
}

RRD_GRAPH_TITLE = {
    'network-bytes': '%(host)s | bytes in/out',
    'network-packets': '%(host)s | packets in/out',
    'system-cpu': '%(host)s | cpu %%',
    'system-memory': '%(host)s | memory utilization',
    #'io': '%(host)s | disk i/o',
    'redis-memory': '%(host)s | redis memory',
    'redis-connections': '%(host)s | redis connections',
    'cassandra-scores': '%(host)s | cassandra scores',
    'elb-requests': '%(host)s | ELB requests/min',
    'elb-latency': '%(host)s | ELB latency (seconds)',
    'metartg-processed': '%(host)s | metrics processed per minute',
}

RRD_GRAPH_TYPES = [
    ('system-cpu', 'CPU'),
    ('system-memory', 'Memory'),
    ('redis-memory', 'Memory'),
    ('redis-connections', 'Connections'),
    ('cassandra-scores', 'Scores'),
    ('elb-requests', 'ELB Requests'),
    ('elb-latency', 'ELB Latency'),
    ('metartg-processed', 'Processed'),
    ('network-bytes', 'Bytes tx/rx'),
    ('network-packets', 'Packets tx/rx'),
#    ('io', 'Disk I/O'),
#    ('redis-memory', 'Redis memory'),
]

tpstats_list = [
    'AE-SERVICE-STAGE',
    'CONSISTENCY-MANAGER',
    'FLUSH-SORTER-POOL',
    'FLUSH-WRITER-POOL',
    'GMFD',
    'HINTED-HANDOFF-POOL',
    'LB-OPERATIONS',
    'LB-TARGET',
    'LOAD-BALANCER-STAGE',
    'MEMTABLE-POST-FLUSHER',
    'MESSAGE-STREAMING-POOL',
    'METADATA-MUTATION-STAGE',
    'MISCELLANEOUS-POOL',
    'PENELOPE-STAGE',
    'RESPONSE-STAGE',
    'ROW-MUTATION-STAGE',
    'ROW-READ-STAGE',
    'STREAM-STAGE',
    'THRIFT',
]

for tpstats in tpstats_list:
    RRD_GRAPH_DEFS['cassandra-tpstats-' + tpstats] = [
        'DEF:pending=%%(rrdpath)s/cassandra_tpstats/%s_PendingTasks.rrd:sum:AVERAGE' % tpstats,
        'DEF:active=%%(rrdpath)s/cassandra_tpstats/%s_ActiveCount.rrd:sum:AVERAGE' % tpstats,
        'LINE:pending#FF6600:%s pending\\l' % tpstats,
        'LINE:active#66FF00:%s active\\l' % tpstats,
    ]
    RRD_GRAPH_DEFS['cassandra-tpstats-%s-completed' % tpstats] = [
        'DEF:completed=%%(rrdpath)s/cassandra_tpstats/%s_CompletedTasks.rrd:sum:AVERAGE' % tpstats,
        'LINE:completed#0066FF:%s completed\\l' % tpstats,
    ]

    RRD_GRAPH_TITLE['cassandra-tpstats-' + tpstats] = '%%(host)s | cassandra %s' % tpstats
    RRD_GRAPH_TITLE['cassandra-tpstats-%s-completed' % tpstats] = '%%(host)s | cassandra %s completed' % tpstats

    RRD_GRAPH_TYPES.append(('cassandra-tpstats-' + tpstats, tpstats))
    RRD_GRAPH_TYPES.append(('cassandra-tpstats-%s-completed' % tpstats, '%s completed' % tpstats))

sstables_list = {}
path = RRDPATH % {
    'host': '*',
    'service': 'cassandra_sstables',
    'metric': '*',
}
for filename in glob(path):
    filename = os.path.basename(filename)
    k = filename.split('.', 3)[:2]
    sstables_list[tuple(k)] = None
sstables_list = sstables_list.keys()

# ks = keyspace
# cf = columnfamily
for ks, cf in sstables_list:
    RRD_GRAPH_DEFS['cassandra-sstables-%s-%s-minmaxavg' % (ks, cf)] = [
        'DEF:min=%%(rrdpath)s/cassandra_sstables/%s.%s.min.rrd:sum:AVERAGE' % (ks, cf),
        'DEF:max=%%(rrdpath)s/cassandra_sstables/%s.%s.max.rrd:sum:AVERAGE' % (ks, cf),
        'DEF:avg=%%(rrdpath)s/cassandra_sstables/%s.%s.avg.rrd:sum:AVERAGE' % (ks, cf),
        'LINE:min#66FFFF:sstable size (min)\\l',
        'LINE:max#FF6600:sstable size (max)\\l',
        'LINE:avg#66FF00:sstable size (avg)\\l',
    ]
    RRD_GRAPH_DEFS['cassandra-sstables-%s-%s-total' % (ks, cf)] = [
        'DEF:total=%%(rrdpath)s/cassandra_sstables/%s.%s.total.rrd:sum:AVERAGE' % (ks, cf),
        'LINE:total#EA8F00:sstable size (total)\\l',
    ]
    RRD_GRAPH_DEFS['cassandra-sstables-%s-%s-count' % (ks, cf)] = [
        'DEF:count=%%(rrdpath)s/cassandra_sstables/%s.%s.count.rrd:sum:AVERAGE' % (ks, cf),
        'LINE:count#00FF00:sstable count\\l',
    ]

    RRD_GRAPH_TITLE['cassandra-sstables-%s-%s-minmaxavg' % (ks, cf)] = '%%(host)s | %s %s size (min/max/avg)' % (ks, cf)
    RRD_GRAPH_TITLE['cassandra-sstables-%s-%s-total' % (ks, cf)] = '%%(host)s | %s %s size (total)' % (ks, cf)
    RRD_GRAPH_TITLE['cassandra-sstables-%s-%s-count' % (ks, cf)] = '%%(host)s | %s %s count' % (ks, cf)

    RRD_GRAPH_TYPES.append(('cassandra-sstables-%s-%s-minmaxavg' % (ks, cf), '%s %s min/max/avg' % (ks, cf)))
    RRD_GRAPH_TYPES.append(('cassandra-sstables-%s-%s-total' % (ks, cf), '%s %s total' % (ks, cf)))
    RRD_GRAPH_TYPES.append(('cassandra-sstables-%s-%s-count' % (ks, cf), '%s %s count' % (ks, cf)))

# Commit log graphs
RRD_GRAPH_DEFS['cassandra-commitlog-pending'] = [
    'DEF:pending=%(rrdpath)s/cassandra_commitlog/tasks.pending.rrd:sum:AVERAGE',
    'LINE:pending#FF0000:tasks pending\\l',
]
RRD_GRAPH_TITLE['cassandra-commitlog-pending'] = '%(host)s | Commitlog - Pending'
RRD_GRAPH_TYPES.append(('cassandra-commitlog-pending', 'Commitlog Pending'))

RRD_GRAPH_DEFS['cassandra-commitlog-completed'] = [
    'DEF:completed=%(rrdpath)s/cassandra_commitlog/tasks.completed.rrd:sum:AVERAGE',
    'LINE:completed#55FF55:tasks completed\\l',
]
RRD_GRAPH_TITLE['cassandra-commitlog-completed'] = '%(host)s | Commitlog - Completed'
RRD_GRAPH_TYPES.append(('cassandra-commitlog-completed', 'Commitlog Completed'))

# Streaming graphs
RRD_GRAPH_DEFS['cassandra-streaming'] = [
    'DEF:from=%(rrdpath)s/cassandra_streaming/streaming.from.rrd:sum:AVERAGE',
    'DEF:to=%(rrdpath)s/cassandra_streaming/streaming.to.rrd:sum:AVERAGE',
    'LINE:from#55FF55:from\\l',
    'LINE:to#FF5555:to\\l',
]
RRD_GRAPH_TITLE['cassandra-streaming'] = '%(host)s | Streaming Activity'
RRD_GRAPH_TYPES.append(('cassandra-streaming', 'Streaming Activity'))

# Compaction graphs
RRD_GRAPH_DEFS['cassandra-compaction'] = [
    'DEF:compacting=%(rrdpath)s/cassandra_compaction/bytes.compacting.rrd:sum:AVERAGE',
    'DEF:remaining=%(rrdpath)s/cassandra_compaction/bytes.remaining.rrd:sum:AVERAGE',
    'LINE:compacting#55FF55:compacting\\l',
    'LINE:remaining#FF5555:remaining\\l',
]
RRD_GRAPH_TITLE['cassandra-compaction'] = '%(host)s | Compaction Activity'
RRD_GRAPH_TYPES.append(('cassandra-compaction', 'Compaction Activity'))

RRD_GRAPH_DEFS['cassandra-compaction-tasks'] = [
    'DEF:pending=%(rrdpath)s/cassandra_compaction/tasks.pending.rrd:sum:AVERAGE',
    'LINE:pending#55FF55:pending\\l',
]
RRD_GRAPH_TITLE['cassandra-compaction-tasks'] = '%(host)s | Compaction Tasks'
RRD_GRAPH_TYPES.append(('cassandra-compaction-tasks', 'Compaction Tasks'))

# Heap graphs
RRD_GRAPH_DEFS['cassandra-memory'] = [
    'DEF:heap_committed=%(rrdpath)s/cassandra_memory/jvm.heap.committed.rrd:sum:AVERAGE',
    'DEF:heap_used=%(rrdpath)s/cassandra_memory/jvm.heap.used.rrd:sum:AVERAGE',
    'DEF:nonheap_committed=%(rrdpath)s/cassandra_memory/jvm.nonheap.committed.rrd:sum:AVERAGE',
    'DEF:nonheap_used=%(rrdpath)s/cassandra_memory/jvm.nonheap.used.rrd:sum:AVERAGE',
    'CDEF:nonheap_committed_stack=heap_committed,nonheap_committed,+',
    'AREA:heap_used#006699:heap used\\l',
    'LINE:heap_committed#FFFFFF:heap committed\\l',
    'AREA:nonheap_used#009966:nonheap used\\l:STACK',
    'LINE:nonheap_committed_stack#FFFFFF:nonheap committed\\l',
]
RRD_GRAPH_TITLE['cassandra-memory'] = '%(host)s | Cassandra Memory'
RRD_GRAPH_TYPES.append(('cassandra-memory', 'Cassandra Memory'))

queues_list = {}
path = RRDPATH % {
    'host': '*',
    'service': 'rabbitmq',
    'metric': '*',
}
for filename in glob(path):
    filename = os.path.basename(filename)
    k = filename.rsplit('.', 1)[0]
    if k.endswith('_unack') or k.endswith('_rate'):
        continue
    queues_list[k] = None
queues_list = queues_list.keys()

for queue in queues_list:
    RRD_GRAPH_DEFS['rabbitmq-%s' % queue] = [
        'DEF:size=%%(rrdpath)s/rabbitmq/%s.rrd:sum:AVERAGE' % queue,
        'DEF:unack=%%(rrdpath)s/rabbitmq/%s_unack.rrd:sum:AVERAGE' % queue,
        'LINE:size#FF6600:%s queue size\\l' % queue,
        'LINE:unack#FF0000:%s unacknowledged\\l' % queue,
    ]
    RRD_GRAPH_TITLE['rabbitmq-%s' % queue] = '%%(host)s | %s queue size' % queue
    RRD_GRAPH_TYPES.append(('rabbitmq-%s' % queue, queue))

    rabbitmq_rate_graph = 'rabbitmq-rates-%s' % queue
    RRD_GRAPH_DEFS[rabbitmq_rate_graph] = [
        'DEF:in=%%(rrdpath)s/rabbitmq/%s_incoming_rate.rrd:sum:AVERAGE' % queue,
        'DEF:ack=%%(rrdpath)s/rabbitmq/%s_ack_rate.rrd:sum:AVERAGE' % queue,
        'LINE:in#FF3484:%s incoming/s\\l' % queue,
        'LINE:ack#4EFF4D:%s acknowledged/s\\l' % queue,
    ]
    RRD_GRAPH_TITLE[rabbitmq_rate_graph] = '%%(host)s | %s queue rates' % queue
    RRD_GRAPH_TYPES.append((rabbitmq_rate_graph, '%s rates' % queue))

path = RRDPATH % {
    'host': '*',
    'service': 'ebs',
    'metric': '*',
}
ebs_mounts = {}
for filename in glob(path):
    filename = os.path.basename(filename)
    k = filename.split('_', 1)[0]
    if not k in ebs_mounts:
        ebs_mounts[k] = True
ebs_mounts = ebs_mounts.keys()

for mount in ebs_mounts:
    RRD_GRAPH_DEFS['ebs-%s-ops' % mount] = [
        'DEF:reads=%%(rrdpath)s/ebs/%s_read_ops.rrd:sum:AVERAGE' % mount,
        'DEF:writes=%%(rrdpath)s/ebs/%s_write_ops.rrd:sum:AVERAGE' % mount,
        'LINE:reads#FF6600:max reads/sec\\l',
        'LINE:writes#00FF66:max writes/sec\\l',
    ]
    RRD_GRAPH_DEFS['ebs-%s-queue' % mount] = [
        'DEF:queue=%%(rrdpath)s/ebs/%s_queue_length.rrd:sum:AVERAGE' % mount,
        'LINE:queue#66FFFF:Queued operations\\l',
    ]

    RRD_GRAPH_TITLE['ebs-%s-ops' % mount] = '%%(host)s | %s ebs iops' % mount
    RRD_GRAPH_TITLE['ebs-%s-queue' % mount] = '%%(host)s | %s ebs queue' % mount

    RRD_GRAPH_TYPES.append(('ebs-%s-ops' % mount, '%s ebs iops' % mount))
    RRD_GRAPH_TYPES.append(('ebs-%s-queue' % mount, '%s ebs queue' % mount))


path = RRDPATH % {
    'host': '*',
    'service': 'monit',
    'metric': '*',
}
services = {}
for filename in glob(path):
    filename = os.path.basename(filename)
    monitservice = filename.split('_', 1)[0]
    services[monitservice] = None

for monitservice in services.keys():
    RRD_GRAPH_DEFS['monit-%s-cpu' % monitservice] = [
        'DEF:cpu=%%(rrdpath)s/monit/%s_cpu.rrd:sum:AVERAGE' % monitservice,
        #'CDEF:cpu_pct=cpu,100,*',
        'LINE:cpu#FF00FF:%s CPU\\l' % monitservice,
    ]
    RRD_GRAPH_DEFS['monit-%s-memory' % monitservice] = [
        'DEF:memory=%%(rrdpath)s/monit/%s_memory.rrd:sum:AVERAGE' % monitservice,
        'CDEF:memory_bytes=memory,1024,*',
        'LINE:memory_bytes#00FF00:%s memory\\l' % monitservice,
    ]

    RRD_GRAPH_TITLE['monit-%s-cpu' % monitservice] = '%%(host)s | %s CPU' % monitservice
    RRD_GRAPH_TITLE['monit-%s-memory' % monitservice] = '%%(host)s | %s memory' % monitservice

    RRD_GRAPH_TYPES.append(('monit-%s-cpu' % monitservice, '%s CPU' % monitservice))
    RRD_GRAPH_TYPES.append(('monit-%s-memory' % monitservice, '%s Memory' % monitservice))

    RRD_GRAPH_OPTIONS['monit-%s-cpu' % monitservice] = ['--upper-limit', '100.0']

#path = RRDPATH % {
#    'host': '*',
#    'service': 'haproxy',
#    'metric': '*',
#}
#hosts = []
#for filename in glob(path):
#    hostname = filename.split('/')[-1].split('_', 1)[0]
#    if not hostname in hosts:
#        hosts.append(hostname)
#
#RRD_GRAPH_DEFS['haproxy-sessions'] = []
#for host in hosts:
#    RRD_GRAPH_DEFS['haproxy-sessions'] += [
#        'DEF:%s_total=%%(rrdpath)s/haproxy/%s_stot.rrd' % (host, host),
#        'LINE:%s_total#66FFFF:%s total sessions\\l' % (host, host),
#    ]
#RRD_GRAPH_TITLE['haproxy-sessions'] = '%%(host)s | haproxy sessions'
#RRD_GRAPH_TYPES.append(('haproxy-sessions', 'Sessions'))


for disk in ('raid0', 'sda1'):
    RRD_GRAPH_DEFS['disk-%s-requests' % disk] = [
        'DEF:rrqm=%%(rrdpath)s/disk/%s.rrqm.rrd:sum:AVERAGE' % disk,
        'DEF:wrqm=%%(rrdpath)s/disk/%s.wrqm.rrd:sum:AVERAGE' % disk,
        'DEF:reads=%%(rrdpath)s/disk/%s.reads.rrd:sum:AVERAGE' % disk,
        'DEF:writes=%%(rrdpath)s/disk/%s.writes.rrd:sum:AVERAGE' % disk,
        'LINE:rrqm#66FFFF:reads queued/s\\l',
        'LINE:wrqm#FF6600:writes queued/s\\l',
        'LINE:reads#33CCCC:reads/s\\l',
        'LINE:writes#CC3300:writes/s\\l',
    ]
    RRD_GRAPH_DEFS['disk-%s-bytes' % disk] = [
        'DEF:rkb=%%(rrdpath)s/disk/%s.rkb.rrd:sum:AVERAGE' % disk,
        'DEF:wkb=%%(rrdpath)s/disk/%s.wkb.rrd:sum:AVERAGE' % disk,
        'CDEF:bytes_read=rkb,1024,*',
        'CDEF:bytes_write=wkb,1024,*',
        'LINE:bytes_read#EA8F00:bytes read/sec\\l',
        'LINE:bytes_write#008FEA:bytes written/sec\\l',
    ]
    RRD_GRAPH_DEFS['disk-%s-latency' % disk] = [
        'DEF:await=%%(rrdpath)s/disk/%s.await.rrd:sum:AVERAGE' % disk,
        'LINE:await#8FEA00:request latency (ms)\\l',
    ]
    RRD_GRAPH_DEFS['disk-%s-util' % disk] = [
        'DEF:util=%%(rrdpath)s/disk/%s.util.rrd:sum:AVERAGE' % disk,
        'CDEF:util_pct=util,100,*',
        'LINE:util_pct#FFFF66:I/O utilization %%\\l',
    ]
    RRD_GRAPH_OPTIONS['disk-%s-util' % disk] = ['--upper-limit', '100.0']

    RRD_GRAPH_TITLE['disk-%s-requests' % disk] = '%%(host)s | %s iops' % disk
    RRD_GRAPH_TITLE['disk-%s-bytes' % disk] = '%%(host)s | %s bytes r/w' % disk
    RRD_GRAPH_TITLE['disk-%s-latency' % disk] = '%%(host)s | %s latency' % disk
    RRD_GRAPH_TITLE['disk-%s-util' % disk] = '%%(host)s | %s I/O utilization' % disk

    RRD_GRAPH_TYPES.append(('disk-%s-requests' % disk, '%s iops' % disk))
    RRD_GRAPH_TYPES.append(('disk-%s-bytes' % disk, '%s bytes r/w' % disk))
    RRD_GRAPH_TYPES.append(('disk-%s-latency' % disk, '%s latency' % disk))
    RRD_GRAPH_TYPES.append(('disk-%s-util' % disk, '%s utilization' % disk))

def get_clusto_name(instanceid):
    key = 'clusto/hostname/%s' % instanceid
    c = cache.get(key)
    if c:
        return c

    try:
        server = clusto.get(instanceid)
        hostname = server[0].attr_value(key='system', subkey='hostname')
        cache.set(key, hostname)
        return hostname
    except:
        cache.set(key, instanceid)
        return instanceid

def dumps(obj):
    callback = bottle.request.params.get('callback', None)
    result = json.dumps(obj, indent=2)

    if callback:
        bottle.response.content_type = 'text/javascript'
        result = '%s(%s)' % (callback, result)
    else:
        bottle.response.content_type = 'application/json'
    return result

def rrdtool(args):
    p = subprocess.Popen(['rrdtool'] + args.split(' '))
    p.wait()


def create_rrd(filename, metric, data):
    print 'Creating rrd', filename, metric, data
    try:
        os.makedirs(os.path.dirname(filename))
    except:
        pass

    rrdtool('create %(filename)s --start %(start)s --step 60 \
DS:sum:%(dstype)s:600:U:U \
RRA:AVERAGE:0.5:1:1500 \
RRA:AVERAGE:0.5:5:2304 \
RRA:AVERAGE:0.5:30:4320' % {
    'filename': filename,
    'start': (data['ts'] - 1),
    'dstype': data['type'],
})


def update_rrd(filename, metric, data):
    #filename = filename.split('/var/lib/metartg/rrds/', 1)[1]
    #rrdtool('update --daemon 127.0.0.1:42217 %s %s:%s' % (filename, str(data['ts']), str(data['value'])))
    #ts = data['ts'] - (data['ts'] % 60)
    rrdtool('update %s %s:%s' % (filename, str(data['ts']), str(data['value'])))


def update_redis(host, service, metricname, metric):
    db.sadd('hosts', host)
    db.hset('metrics/%s' % host, '%s/%s' % (service, metricname), json.dumps((metric['ts'], metric['value'])))

def process_rrd_update(host, service, body):
    metrics = json.loads(body)
    for metric in metrics:
        rrdfile = RRDPATH % {
            'host': host,
            'service': service,
            'metric': metric,
        }
        rrdfullpath = '/var/lib/metartg/rrds/' + rrdfile
        if not os.path.exists(rrdfullpath):
            create_rrd(rrdfullpath, metric, metrics[metric])
        update_rrd(rrdfile, metric, metrics[metric])
        update_redis(host, service, metric, metrics[metric])
    return


def rrdupdate_worker(queue):
    while True:
        #sys.stdout.write('.')
        #sys.stdout.flush()
        metric = queue.get()
        if metric:
            process_rrd_update(*metric)

#procs = []
#for i in range(2):
#    procs.append(eventlet.spawn_n(rrdupdate_worker, rrdqueue))

#procs = []
#for i in range(4):
#    proc = multiprocessing.Process(target=rrdupdate_worker, args=(update_queue,))
#    proc.start()
#    procs.append(proc)

@bottle.get('/status')
def status():
    return dumps({
        'rrdqueue': rrdqueue.qsize(),
    })

@bottle.post('/rrd/:host/:service')
def post_rrd_update(host, service):
    rrdqueue.put((host, service, bottle.request.body.read()))
    #eventlet.spawn_n(process_rrd_update, host, service, bottle.request.body.read())
    bottle.response.status = 202
    return


def cassandra_scores_graphdef(host):
    r = []
    colors = ['FF6600', 'CC3333', '00FF00', 'FFCC00', 'DA4725', '66CC66', '6EA100', '0000FF', 'EACC00', 'D8ACE0', '4668E4', '35962B', '8D00BA']
    files = glob('%s/cassandra_scores/*.rrd' % host)
    files.sort()
    for i, filename in enumerate(files):
        peer = filename.rsplit('/', 1)[1]
        name = peer.rsplit('.', 1)[0]
        name = name.replace('.', '-')
        r += [
            'DEF:%s=%s:sum:AVERAGE' % (name, filename),
            'LINE:%s#%s:%s score\\l' % (name, colors[i % len(colors)], name),
        ]
    return r


@bottle.get('/graph/:host/:graphtype')
def get_rrd_graph(host, graphtype):
    now = int(time())
    params = bottle.request.params
    start = params.get('start', (now - 3600))
    end = params.get('end', now)
    size = params.get('size', 'large')

    cmd = ['/usr/bin/rrdtool', 'graph',
        '-',
        '--daemon', '127.0.0.1:42217',
        '--font', 'DEFAULT:7:monospace',
        '--font-render-mode', 'normal',
        '--color', 'MGRID#880000',
        '--color', 'GRID#777777',
        '--color', 'CANVAS#000000',
        '--color', 'FONT#ffffff',
        '--color', 'BACK#444444',
        '--color', 'SHADEA#000000',
        '--color', 'SHADEB#000000',
        '--color', 'FRAME#444444',
        '--color', 'ARROW#FFFFFF',
        '--imgformat', 'PNG',
        '--tabwidth', '75',
        '--start', str(start),
        '--end', str(end),
    ]

    cmd += RRD_GRAPH_OPTIONS.get(graphtype, [])
    cmd += ['--title', RRD_GRAPH_TITLE.get(graphtype, host) % {
        'host': get_clusto_name(host),
    }]

    if size == 'small':
        cmd += [
            #'--no-legend',
            '--width', '375',
            '--height', '100'
        ]
    else:
        cmd += [
            '--width', '600',
            '--height', '200',
            '--watermark', 'simplegeo'
        ]

    if graphtype == 'cassandra-scores':
        if size == 'small':
            cmd.append('--no-legend')
        cmd += cassandra_scores_graphdef(host)

    for gdef in RRD_GRAPH_DEFS.get(graphtype, []):
        cmd.append(gdef % {
            'rrdpath': '%s' % host,
        })
    #print '\n'.join(cmd)

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, env={'TZ': 'PST8PDT'})
    stdout, stderr = proc.communicate()

    bottle.response.content_type = 'image/png'
    return stdout

@bottle.get('/search')
def search():
    p = bottle.request.params
    query = p.get('q', None)
    if not query:
        bottle.abort(400, 'Parameter "q" is required')

    pools = query.replace('+', ' ').replace(',', ' ').split(' ')
    pools.sort()

    cachekey = 'search/%s' % ','.join(pools)
    result = cache.get(cachekey)
    if result:
        return dumps(json.loads(result))

    def get_contents(name):
        obj = clusto.get_by_name(name)
        return set(obj.contents())

    pools = list(gpool.imap(get_contents, pools))
    first = pools[0]
    pools = pools[1:]

    result = []
    servers = gpool.imap(lambda x: (x, x.attrs()), first.intersection(*pools))

    def get_server_info(server):
        return {
            'name': server.name,
            'parents': [x.name for x in server.parents()],
            'contents': [x.name for x in server.contents()],
            'ip': server.attr_values(key='ip', subkey='ipstring'),
            'dnsname': server.attr_values(key='ec2', subkey='public-dns'),
        }

    servers = list(gpool.imap(get_server_info, first.intersection(*pools)))
    servers.sort(key=lambda x: x['name'])
    cache.set(cachekey, json.dumps(servers))

    return dumps(servers)

@bottle.get('/static/:filename')
def serve_static(filename):
    return bottle.static_file(filename, root=STATIC_PATH)

@bottle.get('/')
def index():
    template = env.get_template('metrics.html')
    groups = {}
    for name, human_name in RRD_GRAPH_TYPES:
        group, metric = name.split('-', 1)
        if not group in groups:
            groups[group] = {}
        groups[group][metric] = human_name

    result = []
    for group in groups:
        graphs = groups[group]
        graphs = graphs.items()
        graphs.sort()
        result.append((group, graphs))
    result.sort()
    return template.render(groups=result)

if __name__ == '__main__':
    bottle.run()

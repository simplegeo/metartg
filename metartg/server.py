from xml.etree import ElementTree
from time import time
from glob import glob
#import multiprocessing
import eventlet.green.subprocess as subprocess
import socket
import os.path
import sys
import os
import re

from decimal import Decimal
import simplejson as json
import logging
import clustohttp
import eventlet
import memcache
import bottle
import jinja2

redis = eventlet.import_patched('redis')

#STATIC_PATH = '/usr/share/metartg/static'
STATIC_PATH = '/home/synack/src/metartg/static'
TEMPLATE_PATH = '/usr/share/metartg/templates'
GRAPHDEFS = '/home/synack/src/metartg/graphs'
RRDS = '/var/lib/metartg/rrds/'

# We do this for the sake of rrdcached
os.chdir(RRDS)

bottle.debug(True)
application = bottle.default_app()
env = jinja2.Environment(loader=jinja2.FileSystemLoader(TEMPLATE_PATH))
cache = memcache.Client(['127.0.0.1:11211'])
clusto = clustohttp.ClustoProxy('http://clusto.simplegeo.com/api')
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
    result = json.dumps(obj, indent=2, sort_keys=True, use_decimal=True)

    if callback:
        bottle.response.content_type = 'text/javascript'
        result = '%s(%s)' % (callback, result)
    else:
        bottle.response.content_type = 'application/json'
    return result


def parse_value(value):
    value, exp = value.rsplit('e', 1)
    value = Decimal(value)
    value = value * Decimal(str(pow(10, int(exp))))
    if value == 0:
        return 0
    return value

@bottle.get('/status')
def status():
    return dumps({
        'rrdqueue': rrdqueue.qsize(),
    })


@bottle.post('/rrd/:host/:service')
def post_rrd_update(host, service):
    rrdqueue.put((host, service, bottle.request.body.read()))
    bottle.response.status = 202
    return

@bottle.get('/graph/:host/:service/:graph#.*#.json')
def get_graph_data(host, service, graph):
    graphdefs = json.load(file('%s/%s.json' % (GRAPHDEFS, service), 'r'))
    if not graph in graphdefs:
        bottle.abort(404, 'No graph definition named %s in %s.json' % (graph, service))
    graphdef = graphdefs[graph]

    now = int(time())
    params = bottle.request.params
    start = params.get('start', (now - 3600))
    end = params.get('end', now)

    cmd = ['/usr/bin/rrdtool', 'fetch']

    for defname, rrdpath in graphdef['rrds'].items():
        rrdpath = os.path.join(RRDS, host, rrdpath)
        cmd += [rrdpath, 'AVERAGE']

    cmd += [
        '--start', str(start),
        '--end', str(end),
        #'--daemon', '127.0.0.1:42217',
    ]
    if 'resolution' in params:
        cmd += ['--resolution', params['resolution']]

    print ' '.join(cmd)

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, env={'TZ': 'PST8PDT'})
    stdout, stderr = proc.communicate()

    header, data = stdout.split('\n\n', 1)
    return dumps([(int(ts), parse_value(value)) for ts, value in [x.split(': ', 1) for x in data.split('\n') if x and not x.endswith('nan')]])


@bottle.get('/graph/:host/:service/:graph#.*#.png')
def get_graph(host, service, graph):
    graphdefs = json.load(file('%s/%s.json' % (GRAPHDEFS, service), 'r'))
    if not graph in graphdefs:
        bottle.abort(404, 'No graph definition named %s in %s.json' % (graph, service))
    graphdef = graphdefs[graph]

    now = int(time())
    params = bottle.request.params
    start = params.get('start', (now - 3600))
    end = params.get('end', now)
    size = params.get('size', 'large')

    cmd = ['/usr/bin/rrdtool', 'graph',
        '-',
        #'--daemon', '127.0.0.1:42217',
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

    if 'rrdargs' in graphdef:
        cmd += graphdef['rrdargs'].split(' ')

    if 'title' in graphdef:
        cmd += ['--title', '%s | %s' % (get_clusto_name(host), graphdef['title'])]

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

    for defname, rrdpath in graphdef['rrds'].items():
        rrdpath = os.path.join(RRDS, host, rrdpath)
        cmd.append('DEF:%s=%s:sum:AVERAGE' % (defname, rrdpath))
    cmd += graphdef['config']

    #print '\n'.join(cmd)

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, env={'TZ': 'PST8PDT'})
    stdout, stderr = proc.communicate()

    bottle.response.content_type = 'image/png'
    return stdout


def available_graphs(host):
    available = []
    rrdpath = os.path.join(RRDS, host)
    if not rrdpath.startswith(RRDS):
        raise ValueError('Directory traversal... Somebody\'s being naughty')

    for filename in os.listdir(GRAPHDEFS):
        if not filename.endswith('.json'):
            continue
        path = os.path.join(GRAPHDEFS, filename)
        try:
            graphs = json.load(file(path, 'r'))
        except ValueError, e:
            raise Exception('Error parsing graph definition file %s: %s' % (filename, str(e)))

        service = filename.rsplit('.', 1)[0]
        for name, graph in graphs.iteritems():
            if not all([os.path.exists(os.path.join(rrdpath, rrdfile)) for rrdfile in graph['rrds'].values()]):
                continue
            available.append('%s/%s' % (service, name))

    available.sort()
    return available


@bottle.get('/graph/:host')
def get_available_graphs(host):
    try:
        return dumps(available_graphs(host))
    except Exception, e:
        bottle.abort(500, str(e))


@bottle.get('/graph/')
def get_available_hosts():
    hosts = os.listdir(RRDS)
    hosts.sort()
    return dumps(hosts)


@bottle.get('/search')
def hosts_search():
    q = bottle.request.params.get('q', None)
    q = q.strip(' ')
    if not q:
        return dumps([])

    keywords = q.split(' ')
    for kw in list(keywords):
        for match in re.findall('([a-z0-9]{8})', kw):
            instance = 'i-' + match
            if not instance in keywords:
                keywords.append(instance)

    sets = []
    objects = []
    for keyword in keywords:
        try:
            candidates = clusto.get(keyword)
        except:
            continue

        for entity in clusto.get(keyword):
            if entity.type == 'pool':
                sets.append(set([x for x in entity.contents()]))
            else:
                objects.append(entity)

    if not sets:
        sets = []
    else:
        if len(sets) > 1:
            sets = sets[0].intersection(*sets[1:])
        else:
            sets = sets[0]
    result = set(list(sets) + objects)
    result = list(result)

    servers = []
    for server in result:
        server.attrs()
        servers.append({
            'name': server.name,
            'type': server.type,
            'ip': server.attr_values(key='ip', subkey='ipstring'),
            'hostname': server.attr_value(key='system', subkey='hostname'),
            'dnsname': server.attr_value(key='ec2', subkey='public-dns'),
            'parents': [x.name for x in server.parents()],
            'graphs': available_graphs(server.name),
        })
    return dumps(servers)


@bottle.get('/static/:filename#.*#')
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

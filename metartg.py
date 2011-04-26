from xml.etree import ElementTree
from time import time
import subprocess
import socket

import simplejson as json
import bottle
import jinja2

bottle.debug(True)
application = bottle.default_app()
env = jinja2.Environment(loader=jinja2.FileSystemLoader('templates/'))

RRD_GRAPH_DEFS = {
    'memory': [
        'DEF:mem_free=%(rrdpath)s/mem_free.rrd:sum:AVERAGE',
        'DEF:mem_total=%(rrdpath)s/mem_total.rrd:sum:AVERAGE',
        'DEF:mem_buffers=%(rrdpath)s/mem_buffers.rrd:sum:AVERAGE',
        'CDEF:gb_mem_free=mem_free,1024,*',
        'CDEF:gb_mem_total=mem_total,1024,*',
        'CDEF:gb_mem_buffers=mem_buffers,1024,*',
        'CDEF:t_mem_used=gb_mem_total,gb_mem_free,-',
        'CDEF:gb_mem_used=t_mem_used,gb_mem_buffers,-',
        'LINE:gb_mem_total#FFFFFF:Total memory\\l',
        'AREA:gb_mem_used#006699:Used memory\\l',
    ],
    'network': [
        'DEF:net_in=%(rrdpath)s/bytes_in.rrd:sum:AVERAGE',
        'DEF:net_out=%(rrdpath)s/bytes_out.rrd:sum:AVERAGE',
        'LINE:net_in#0000FF:Network in\\l',
        'LINE:net_out#FF0000:Network out\\l',
    ],
    'cpu': [
        'DEF:cpu_user=%(rrdpath)s/cpu_user.rrd:sum:AVERAGE',
        'DEF:cpu_system=%(rrdpath)s/cpu_system.rrd:sum:AVERAGE',
        'DEF:cpu_nice=%(rrdpath)s/cpu_nice.rrd:sum:AVERAGE',
        'DEF:cpu_wio=%(rrdpath)s/cpu_wio.rrd:sum:AVERAGE',
        'CDEF:stack_cpu_nice=cpu_system,cpu_nice,+',
        'CDEF:stack_cpu_wio=stack_cpu_nice,cpu_wio,+',
        'CDEF:stack_cpu_user=stack_cpu_wio,cpu_nice,+',
        'AREA:stack_cpu_user#00CC00:CPU user\\l',
        'AREA:stack_cpu_wio#FF0000:CPU iowait\\l',
        'AREA:stack_cpu_nice#4444FF:CPU nice\\l',
        'AREA:cpu_system#994499:CPU system\\l',
    ]
}

def get_metrics_xml(host='localhost', port=8651):
    sock = socket.socket()
    sock.connect((host, port))
    buf = ''
    while True:
        data = sock.recv(4096)
        if data == '':
            break
        buf += data
    return buf

def get_metrics_list(root):
    result = {}
    for cluster in root.findall('GRID/CLUSTER'):
        clustername = cluster.get('NAME')
        if not clustername in result:
            result[clustername] = {}

        for host in cluster.findall('HOST'):
            name = host.get('NAME')
            ip = host.get('IP')
            #pprint([x.items() for x in host.findall('METRIC')])
            result[clustername][name] = []
            for metric in host.findall('METRIC'):
                d = {
                    'name': metric.get('NAME'),
                    'graph': '/graph/%s/%s/%s' % (
                        clustername,
                        name,
                        metric.get('NAME'),
                    ),
                }
                for extra in metric.findall('EXTRA_DATA/EXTRA_ELEMENT'):
                    d[extra.get('NAME').lower()] = extra.get('VAL')
                result[clustername][name].append(d)
    return result

def dumps(obj):
    callback = bottle.request.params.get('callback', None)
    result = json.dumps(obj, indent=2)

    if callback:
        bottle.response.content_type = 'text/javascript'
        result = '%s(%s)' % (callback, result)
    else:
        bottle.response.content_type = 'application/json'
    return result

@bottle.get('/metrics')
def get_metrics():
    root = ElementTree.fromstring(get_metrics_xml())
    metrics = get_metrics_list(root)
    return dumps(metrics)

@bottle.get('/graph/:cluster/:host/')
@bottle.get('/graph/:cluster/:host')
def get_graphtypes(cluster, host):
    return dumps(RRD_GRAPH_DEFS.keys())

@bottle.get('/graph/:cluster/:host/:graphtype')
def get_graph(cluster, host, graphtype):
    now = int(time())
    params = bottle.request.params
    start = params.get('start', (now - 3600))
    end = params.get('end', now)

    cmd = ['/usr/bin/rrdtool', 'graph',
        '-',
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
        '--watermark', 'SG',
        '--start', str(start),
        '--end', str(end),
    ]

    for gdef in RRD_GRAPH_DEFS.get(graphtype, []):
        cmd.append(gdef % {
            'rrdpath': '/var/lib/ganglia/rrds/%s/%s' % (cluster, host),
            'cluster': cluster,
            'host': host,
        })
    print ' '.join(cmd)

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    stdout, stderr = proc.communicate()

    bottle.response.content_type = 'image/png'
    return stdout

@bottle.get('/')
def get_all_hosts():
    root = ElementTree.fromstring(get_metrics_xml())
    metrics = get_metrics_list(root)

    result = []
    for cluster in metrics:
        r = {
            'name': cluster,
            'hosts': []
        }
        for host in metrics[cluster]:
            r['hosts'].append(host)
        result.append(r)

    template = env.get_template('overview.html')
    bottle.response.content_type = 'text/html'
    return template.render(clusters=result)

@bottle.get('/static/:filename')
def server_static(filename):
    return bottle.static_file(filename, root='./static')

if __name__ == '__main__':
    bottle.run()

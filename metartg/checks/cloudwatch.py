from decimal import Decimal
import simplejson as json
from urllib import urlopen
from time import time
import urllib2
import socket

import dewpoint.aws
import metartg.clustohttp
from metartg import conf

region = urllib2.urlopen(urllib2.Request('http://169.254.169.254/latest/meta-data/placement/availability-zone')).read()[:-1]

clusto = metartg.clustohttp.ClustoProxy('http://clusto.simplegeo.com/api')
awscreds = conf('aws')
cloudwatch = dewpoint.aws.AWSProxy(
    key=awscreds['key'],
    secret=awscreds['secret'],
    version='2010-08-01',
    baseurl='http://monitoring.%s.amazonaws.com' % region)


def get_ebs_volumes(instance):
    try:
        volumes = json.load(file('/tmp/ebs-%s.json' % instance, 'r'))
        return volumes
    except:
        pass

    url = 'https://amazinghorse.simplegeo.com:4430/aws/ec2/%s/instance/%s' % (region, instance)
    auth = 'Basic %s' % conf('amazinghorse_auth').encode('base64').rstrip('\n')
    req = urllib2.Request(url, headers={
        'Authorization': auth,
    })
    resp = urllib2.urlopen(req)
    server = json.loads(resp.read())
    volumes = []
    for mountpoint, volume in server['block_devices'].items():
        if not volume.startswith('ebs:'):
            continue
        volume = volume.split(':', 1)[1]
        mountpoint = mountpoint.rsplit('/', 1)[1]
        volumes.append((mountpoint, volume))

    json.dump(volumes, file('/tmp/ebs-%s.json' % instance, 'w'))
    return volumes


def ebs_metrics(mountpoint, volume):
    metrics = cloudwatch.ListMetrics(Namespace='AWS/EBS', Dimensions={
        'VolumeId': volume,
    })
    metrics = [x.text for x in metrics.findall('ListMetricsResult/Metrics/member/MetricName')]

    now = int(time())
    results = {}
    for metric in metrics:
        stats = cloudwatch.GetMetricStatistics(
            Namespace='AWS/EBS',
            Dimensions={'VolumeId': volume},
            Statistics=['Minimum', 'Maximum', 'Average', 'Sum'],
            Period=600,
            StartTime=dewpoint.aws.format_time(now - 600),
            EndTime=dewpoint.aws.format_time(now),
            MetricName=metric)

        datapoints = []
        for member in stats.findall('GetMetricStatisticsResult/Datapoints/member'):
            m = {}
            for elem in member:
                value = elem.text
                if elem.tag == 'Timestamp':
                    value = dewpoint.aws.parse_time(elem.text)
                if elem.tag in ('Minimum', 'Maximum', 'Average', 'Sum'):
                    value = Decimal(value)
                m[elem.tag] = value
            datapoints.append(m)
        results[metric] = datapoints

    rmap = {
        'idle_time': ('VolumeIdleTime', 'Average', 'GAUGE'),
        'queue_length': ('VolumeQueueLength', 'Average', 'GAUGE'),
        'read_ops': ('VolumeReadOps', 'Average', 'GAUGE'),
        'write_ops': ('VolumeWriteOps', 'Average', 'GAUGE'),
    }

    r = {}
    for key, value in rmap.items():
        # as if this weren't obscure enough...
        r['%s_%s' % (mountpoint, key)] = {
            'ts': results[value[0]][0]['Timestamp'],
            'type': value[2],
            'value': results[value[0]][0][value[1]],
        }
    return r


def elb_metrics(elbname):
    metrics = cloudwatch.ListMetrics(Namespace='AWS/ELB', Dimensions={
        'LoadBalancerName': 'API',
    })
    metrics = [x.text for x in metrics.findall('ListMetricsResult/Metrics/member/MetricName')]

    now = int(time())
    results = {}
    for metric in metrics:
        stats = cloudwatch.GetMetricStatistics(
            Namespace='AWS/ELB',
            Dimensions={'LoadBalancerName': 'API'},
            Statistics=['Minimum', 'Maximum', 'Average', 'Sum'],
            Period=60,
            StartTime=dewpoint.aws.format_time(now - 60),
            EndTime=dewpoint.aws.format_time(now),
            MetricName=metric)

        datapoints = []
        for member in stats.findall('GetMetricStatisticsResult/Datapoints/member'):
            m = {}
            for elem in member:
                value = elem.text
                if elem.tag == 'Timestamp':
                    value = dewpoint.aws.parse_time(elem.text)
                if elem.tag in ('Minimum', 'Maximum', 'Average', 'Sum'):
                    value = Decimal(value)
                m[elem.tag] = value
                    
            datapoints.append(m)
        results[metric] = datapoints

    return {
        'latency_min': {
            'ts': results['Latency'][0]['Timestamp'],
            'type': 'GAUGE',
            'value': results['Latency'][0]['Minimum'],
        },
        'latency_max': {
            'ts': results['Latency'][0]['Timestamp'],
            'type': 'GAUGE',
            'value': results['Latency'][0]['Maximum'],
        },
        'latency_avg': {
            'ts': results['Latency'][0]['Timestamp'],
            'type': 'GAUGE',
            'value': results['Latency'][0]['Average'],
        },
        'request_count': {
            'ts': results['RequestCount'][0]['Timestamp'],
            'type': 'GAUGE',
            'value': results['RequestCount'][0]['Sum'],
        },
        'healthy_hosts': {
            'ts': results['HealthyHostCount'][0]['Timestamp'],
            'type': 'GAUGE',
            'value': results['HealthyHostCount'][0]['Average'],
        },
        'unhealthy_hosts': {
            'ts': results['UnHealthyHostCount'][0]['Timestamp'],
            'type': 'GAUGE',
            'value': results['UnHealthyHostCount'][0]['Average'],
        },
    }

def run_check(callback):
    instance = urllib2.urlopen(urllib2.Request('http://169.254.169.254/latest/meta-data/instance-id')).read()
    if instance == 'i-74bbbd1b':
        callback('elb', elb_metrics('API'))

    for mountpoint, volume in get_ebs_volumes(instance):
        callback('ebs', ebs_metrics(mountpoint, volume))
    #callback('ebs', ebs_metrics(instance))


if __name__ == '__main__':
    import simplejson as json
    def func(name, metrics, hostname=None):
        print json.dumps(metrics, indent=2, use_decimal=True)
    run_check(func)

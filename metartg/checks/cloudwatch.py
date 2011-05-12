from decimal import Decimal
from time import time
import socket

import dewpoint.aws
from metartg import conf

awscreds = conf('aws')
cloudwatch = dewpoint.aws.AWSProxy(
    key=awscreds['key'],
    secret=awscreds['secret'],
    version='2010-08-01',
    baseurl='http://monitoring.us-east-1.amazonaws.com')


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
    callback('elb', elb_metrics('API'))


if __name__ == '__main__':
    import simplejson as json
    def func(name, metrics, hostname=None):
        print json.dumps(metrics, indent=2, use_decimal=True)
    run_check(func)

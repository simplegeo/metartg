import dewpoint.aws
import simplejson as json
from xml.etree import ElementTree
from decimal import Decimal
from time import time, gmtime, strftime
import re

cloudwatch = dewpoint.aws.AWSProxy(
    key='AKIAISNDECTOP7SBLDDQ',
    secret='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',
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
    return results


def run_check(callback):
    metrics = elb_metrics('API')

    callback('elb', {
        'latency_min': {
            'ts': metrics['Latency']['Timestamp'],
            'type': 'GAUGE',
            'value': metrics['Latency']['Minimum'],
        },
        'latency_max': {
            'ts': metrics['Latency']['Timestamp'],
            'type': 'GAUGE',
            'value': metrics['Latency']['Maximum'],
        },
        'latency_avg': {
            'ts': metrics['Latency']['Timestamp'],
            'type': 'GAUGE',
            'value': metrics['Latency']['Average'],
        }
        'request_count': {
            'ts': metrics['RequestCount']['Timestamp'],
            'type': 'GAUGE',
            'value': metrics['RequestCount']['Sum'],
        }
        'healthy_hosts': {
            'ts': metrics['HealthyHostCount']['Timestamp'],
            'type': 'GAUGE',
            'value': metrics['HealthyHostCount']['Average'],
        }
        'unhealthy_hosts': {
            'ts': metrics['UnHealthyHostCount']['Timestamp'],
            'type': 'GAUGE',
            'value': metrics['UnHealthyHostCount']['Average'],
        }
    })

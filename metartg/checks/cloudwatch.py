from decimal import Decimal
from time import time
import dewpoint.aws

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
            'ts': metrics['Latency'][0]['Timestamp'],
            'type': 'GAUGE',
            'value': metrics['Latency'][0]['Minimum'],
        },
        'latency_max': {
            'ts': metrics['Latency'][0]['Timestamp'],
            'type': 'GAUGE',
            'value': metrics['Latency'][0]['Maximum'],
        },
        'latency_avg': {
            'ts': metrics['Latency'][0]['Timestamp'],
            'type': 'GAUGE',
            'value': metrics['Latency'][0]['Average'],
        },
        'request_count': {
            'ts': metrics['RequestCount'][0]['Timestamp'],
            'type': 'GAUGE',
            'value': metrics['RequestCount'][0]['Sum'],
        },
        'healthy_hosts': {
            'ts': metrics['HealthyHostCount'][0]['Timestamp'],
            'type': 'GAUGE',
            'value': metrics['HealthyHostCount'][0]['Average'],
        },
        'unhealthy_hosts': {
            'ts': metrics['UnHealthyHostCount'][0]['Timestamp'],
            'type': 'GAUGE',
            'value': metrics['UnHealthyHostCount'][0]['Average'],
        },
    })

if __name__ == '__main__':
    import simplejson as json
    def func(name, metrics):
        print json.dumps(metrics, indent=2, use_decimal=True)
    run_check(func)

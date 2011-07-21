from base64 import b64encode
from pprint import pprint
from traceback import format_exc
import simplejson as json
import clustohttp
import logging
import urllib2
import os.path
import sys
import os
import threading


def conf(key, default=None):
    config = json.load(file('/etc/metartg.conf', 'r'))
    return config.get(key, default)


def get_checks(checks):
    for filename in checks:
        try:
            check = __import__('metartg.checks.%s' % filename, {}, {}, ['run_check'], 0)
        except Exception, e:
            logging.error('Unable to import check %s: %s' % (filename, str(e)))
            return

        yield (filename, check)
    return


def run_checks(checks):
    metartg = Metartg()
    check_timeout = conf('check_timeout', default=20)

    for filename, check in get_checks(checks):
        def check_timedout():
            raise Exception('Timeout from %s.run_check' % (filename, ))

        timer = threading.Timer(check_timeout, check_timedout)
        timer.start()

        try:
            check.run_check(metartg.update)
        except:
            logging.error('Exception from %s.run_check: %s' % (filename, format_exc()))
            return
        finally:
          timer.cancel()


class Request(urllib2.Request):
    def __init__(self, method, url, headers={}, data=None):
        urllib2.Request.__init__(self, url, data, headers)
        self.method = method

    def get_method(self):
        return self.method


class Metartg(object):
    def __init__(self, url=None, hostname=None):
        '''
        @param url      base url for metartg api service
        @param hostname unique identifier for this host, to send metrics for
        '''
        if not url:
            url = conf('url')
        if not hostname:
            hostname = conf('hostname')
        self.url = url
        self.hostname = hostname
        self.auth = conf('auth')

    def update(self, service, metrics, hostname=None):
        '''
        @param service  Name of the service to send metrics for
        @param metrics  dict in the following form
        {
            'ActiveCount': {'type': 'GAUGE', 'ts': 1304443700, 'value': 20},
        }
        '''

        #pprint((service, metrics))
        if hostname is None:
            hostname = self.hostname

        if not metrics:
            return

        headers = {'Content-type': 'application/json'}
        if self.auth:
            headers['Authorization'] = 'Basic ' + b64encode('%(username)s:%(password)s' % self.auth)

        req = Request('POST', '%s/rrd/%s/%s' %
            (self.url, hostname, service),
            data=json.dumps(metrics, use_decimal=True), headers=headers)
        resp = urllib2.urlopen(req)
        return (resp.info().status, resp.read())

from base64 import b64encode
import simplejson as json
import urllib2


def conf(key):
    config = json.load(file('/etc/metartg.conf', 'r'))
    return config.get(key, None)


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

    def update(self, service, metrics):
        '''
        @param service  Name of the service to send metrics for
        @param metrics  dict in the following form
        {
            'ActiveCount': {'type': 'GAUGE', 'ts': 1304443700, 'value': 20},
        }
        '''

        headers = {'Content-type': 'application/json'}
        if self.auth:
            headers['Authorization'] = 'Basic ' + b64encode('%(username)s:%(password)s' % self.auth)

        req = Request('POST', '%s/rrd/%s/%s' %
            (self.url, self.hostname, service),
            data=json.dumps(metrics), headers=headers)
        resp = urllib2.urlopen(req)
        return (resp.info().status, resp.read())

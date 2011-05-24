from time import time
import redis

def metartg_metrics():
    db = redis.Redis()
    now = int(time())
    return {
        'processed': {
            'ts': now,
            'type': 'COUNTER',
            'value': db.get('processed'),
        },
        'queued': {
            'ts': now,
            'type': 'GAUGE',
            'value': db.llen('rrdqueue'),
        }
    }
    
def run_check(callback):
    callback('metartg', metartg_metrics())

if __name__ == '__main__':
    import json
    print json.dumps(metartg_metrics(), indent=2)

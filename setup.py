from setuptools import setup
from glob import glob

setup(name='metartg',
      version='0.1',
      author='Jeremy Grosser',
      packages=['metartg', 'metartg.checks', 'dewpoint', 'metartg.thrift',
                'metartg.thrift.cassandra'],
      scripts=[
        'scripts/metartg_check',
        'scripts/metartg_consumer'
      ],
      install_requires=[
        'eventlet',
        'python-memcached',
        'simplejson',
        'gunicorn',
        'bottle',
        'jinja2',
        'pyrrd',
      ]
)

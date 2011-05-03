from setuptools import setup
from glob import glob

setup(name='metartg',
      version='0.1',
      author='Jeremy Grosser',
      packages=['metartg', 'metartg.checks'],
      scripts=[
        'scripts/metartg_check'
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

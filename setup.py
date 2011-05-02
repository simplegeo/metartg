from setuptools import setup
from glob import glob

setup(name='metartg',
      version='0.1',
      author='Jeremy Grosser',
      packages=['metartg'],
      scripts=glob('checks/*'),
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

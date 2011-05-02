from setuptools import setup
from glob import glob

setup(name='metartg',
      version='0.1',
      author='Jeremy Grosser',
      packages=['metartg'],
      scripts=glob('checks/*'),
)

# -*- coding: utf-8 -*-
# Copyright 2013 Dave Carlson <thecubic@thecubic.net>
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, eithrer express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

execfile('zedswarm/version.py')
import sys


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


requirements = [
    'gevent',
    'msgpack-python',
    'pyzmq',
    'kazoo>=1.0',
]
if sys.version_info < (2, 7):
    requirements.append('argparse')


setup(
    name='zedswarm',
    version=__version__,
    description='zedswarm implements a Swarm (multiple masters, multiple workers) RPC pattern with ZMQ (pyzmq) and ZooKeeper (kazoo).',
    author=__author__,
    url='https://github.com/thecubic/zedswarm',
    packages=['zedswarm'],
    install_requires=requirements,
    zip_safe=False,    
    license='Apache 2.0',
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
    ),
)

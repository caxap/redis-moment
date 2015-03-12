#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

from moment import __version__


setup(
    name='redis-moment',
    version=__version__,
    author='Max Kamenkov',
    author_email='mkamenkov@gmail.com',
    description='A Powerful Analytics Python Library for Redis',
    url='https://github.com/caxap/redis-moment',
    packages=find_packages(),
    install_requires=['redis'],
    zip_safe=False,
    include_package_data=True,
    test_suite='moment.tests',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: System :: Distributed Computing',
        'Topic :: Software Development :: Object Brokering',
    ]
)

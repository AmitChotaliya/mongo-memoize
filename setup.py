# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='mongo-memoize',
    version='0.0.5',
    description='A Python decorator library for caching function results in MongoDB',
    long_description=open('README.rst').read(),
    author='Ikuya Yamada',
    author_email='ikuya@ikuya.net',
    url='http://github.com/ikuyamada/mongo-memoize/',
    packages=find_packages(),
    license=open('LICENSE').read(),
    include_package_data=True,
    keywords=[],
    classifiers=(
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ),
    install_requires=[
        'pymongo',
    ],
    tests_require=[
        'nose',
        'mock',
    ],
    test_suite='nose.collector'
)

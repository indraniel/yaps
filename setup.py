# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

with open('yaps/version.py') as f:
    exec(f.read())

setup(
    name='yaps',
    version=__version__,
    description='Yet Another Pipeline Setup',
    long_description=readme,
    author='Indraniel Das',
    author_email='idas@wustl.edu',
    license=license,
    url='https://github.com/indraniel/yaps',
    install_requires=[
        'ruffus',
        'drmaa',
        'click',
        'clint',
        'six',
    ],
    entry_points='''
        [console_scripts]
        yaps=yaps.cli:cli
    ''',
    packages=find_packages(exclude=('tests', 'docs')),
    package_data={
        '': ['*.md', 'LICENSE'],
        'yaps' : ['data/*'],
    },
)

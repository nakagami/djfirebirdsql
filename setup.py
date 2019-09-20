#!/usr/bin/env python3
"""
Django database backend for firebird
"""
from distutils.core import setup, Command

classifiers = [
    'Development Status :: 3 - Alpha',
    'License :: OSI Approved :: BSD License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Topic :: Database',
    'Framework :: Django',
]

setup(name='djfirebirdsql', 
        version='0.1.3',
        description='Django database backend for firebird',
        long_description=open('README.rst').read(),
        url='https://github.com/nakagami/djfirebirdsql/',
        classifiers=classifiers,
        keywords=['Django', 'Firebird', 'pyfirebirdsql'],
        license='BSD',
        author='Hajime Nakagami',
        author_email='nakagami@gmail.com',
        packages = ['djfirebirdsql'],
)

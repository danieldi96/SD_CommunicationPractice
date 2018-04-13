# MapReduce

[![GitHub](https://badge.fury.io/py/pyactor.svg)](https://github.com/pedrotgn/pyactor)

This is an implementation of [MapReduce](https://www.tutorialspoint.com/es/hadoop/hadoop_mapreduce.htm) in python using The [PyActor](https://github.com/pedrotgn/pyactor) library.

This project consists in the first SD's practice of [URV.](http://www.urv.cat/es/)

## Script's Usage
Using master:

    python master.py (num_mappers) (ip*) (program*)

ip* = If ip is "localhost" it's equal to '127.0.0.1'.
program* = It can be 'WC' or 'CW' for 'WordCount' or 'CountWord', respectively

Using mapper (many terminals as mappers you want):

    python master.py (id_mapper*) (ip*)

id_mapper* = '0 to N-1' mappers you have in master.py. 
ip* = If ip is "localhost" it's equal to '127.0.0.1'.

## Tutorial of PyActor

PyActor has many examples and a tutorial explaining all its functionalities. This examples can be found in the 'pyactor/examples' directory of the project. They are also explained in the documentation as a tutorial, hosted at [readthedocs.org.](http://pyactor.readthedocs.io/en/master/)

## Installation of PyActor
Install with:

    sudo pip install pyactor

or Install using:

    python setup.py install

Check that works executing the examples:

    cd examples
    python sample1.py
    ...



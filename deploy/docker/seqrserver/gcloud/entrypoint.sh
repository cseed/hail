#!/bin/bash

export SPARK_HOME=/usr/local/spark-2.0.2-bin-hadoop2.7

export HAIL_HOME=/hail

export PYTHONPATH="$HAIL_HOME/python:$SPARK_HOME/python:`ls $SPARK_HOME/python/lib/py4j-*-src.zip`"

export SPARK_CLASSPATH="$HAIL_HOME/lib/*"

echo 'starting seqrserver.py...'
su -c 'python seqrserver.py' seqrserver

# sleep indenfinitely to prevent container from terminating
sleep 1000000000000

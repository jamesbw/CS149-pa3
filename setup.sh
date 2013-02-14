#!/bin/tcsh
./local-hadoop/stop-local-hadoop.py
./local-hadoop/start-local-hadoop.py
source ./local-hadoop/env-local-hadoop.tcsh

hadoop --config conf fs -mkdir query
hadoop --config conf fs -put query1.txt query
hadoop --config conf fs -mkdir input
hadoop --config conf fs -put chunk_aa input
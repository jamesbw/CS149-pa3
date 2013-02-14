#!/bin/sh
hadoop --config conf fs -rmr output/
hadoop --config conf jar ngram.jar Ngram 4 query1.txt input/ output/
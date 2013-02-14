#!/bin/tcsh
hadoop --config conf fs -rmr output/
hadoop --config conf jar ngram.jar NGram 4 query/query1.txt input/ output/
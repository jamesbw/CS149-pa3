#!/bin/tcsh
hadoop --config conf fs -rmr output/
hadoop --config conf jar ngram.jar NGram -D mapred.child.java.opts=-Xmx1024M -D mapred.reduce.tasks=1 -D mapred.map.tasks=2 4 query/query3.txt input/ output/


rm -rf output
hadoop --config conf fs -get output .
cat output/*
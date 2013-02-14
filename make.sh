#! /bin/sh/
mkdir -p class_dir/
javac -cp ${HADOOP_HOME}/hadoop-core-1.1.1.jar:. -d class_dir/ Tokenizer.java NGram.java
jar -cvf ngram.jar -C class_dir/ .
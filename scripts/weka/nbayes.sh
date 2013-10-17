#!/bin/bash

DATA_DIR=$1
WORD_NUMS=(1500 3000 5000 7500 10000)
CLASSPATH=/home/oyiptong/.java/lib/weka.jar

for NUM_WORDS in ${WORD_NUMS[*]}
do
    COMMAND="java -XX:+UseConcMarkSweepGC -XX:PermSize=5g -Xmx10g -cp $CLASSPATH weka.classifiers.bayes.NaiveBayesMultinomial -t ./$DATA_DIR/$DATA_DIR.word_vector.tfidf.w$NUM_WORDS.arff -c first -d ./$DATA_DIR/$DATA_DIR.nbayes.w$NUM_WORDS.model.dat"
    OUT_PATH="./$DATA_DIR/$DATA_DIR.nbayes.results.w$NUM_WORDS.txt"
    echo $COMMAND
    nohup $COMMAND > $OUT_PATH &
done

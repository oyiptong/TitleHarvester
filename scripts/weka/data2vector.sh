#!/bin/bash

DATA_DIR=$1
REGION_CODE=$2
WORD_NUMS=(1500 3000 5000 7500 10000)
CLASSPATH=./lib/weka.jar

if [[ $REGION_CODE = "zh-CN" ]]
  then
    CLASSPATH=.:./lib/weka.jar:./WekaChineseTokenizer/dist/ChineseTokenizer.jar:./tree_split/target/tree_split-1.0.1.jar:./ansj_seg/target/ansj_seg-0.9.1.jar
fi

if [ -n "$CP_DATA2VECTOR" ]
  then
    CLASSPATH=${CP_DATA2VECTOR}
fi

for NUM_WORDS in ${WORD_NUMS[*]}
do
    COMMAND="java -XX:+UseConcMarkSweepGC -XX:PermSize=5g -Xmx10g -cp $CLASSPATH weka.filters.unsupervised.attribute.StringToWordVector -C -prune-rate 10 -T -I -N -W $NUM_WORDS -i ./$DATA_DIR/$DATA_DIR.arff -o ./$DATA_DIR/$DATA_DIR.word_vector.tfidf.w$NUM_WORDS.arff"
    if [[ $REGION_CODE = "zh-CN" ]]
      then
        COMMAND="java -XX:+UseConcMarkSweepGC -XX:PermSize=5g -Xmx10g -cp $CLASSPATH weka.filters.unsupervised.attribute.StringToWordVector -tokenizer org.mozilla.up.ChineseTokenizer -C -prune-rate 10 -T -I -N -W $NUM_WORDS -i ./$DATA_DIR/$DATA_DIR.arff -o ./$DATA_DIR/$DATA_DIR.word_vector.tfidf.w$NUM_WORDS.arff"
    fi
    echo $COMMAND
    nohup $COMMAND &
done

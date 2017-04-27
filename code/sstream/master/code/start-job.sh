#!/bin/bash

hdfs dfs -rm /clustervolume/*
hdfs dfs -copyFromLocal boontadata-sstream-job1-assembly-0.1.jar /clustervolume

spark-submit \
    --class StructuredStream.Main \
    --deploy-mode cluster \
 #   --master spark://sstreamm1:6066 \
    /clustervolume/boontadata-sstream-job1-assembly-0.1.jar 

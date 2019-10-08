#!/bin/bash

for entry in /tmp/data_grupo01/*
do
    hdfs dfs -put $entry /warehouse/raw01/ut_data
    if [ $? -eq 0 ]
    then
        echo "[INFO] HDFS IMPORT : $entry - OK" >> /tmp/log_grupo01/dir2hdfs.log
    else
        echo "[ERROR] HDFS IMPORT : $entry - KO" >> /tmp/log_grupo01/dir2hdfs.log
    fi
done
#!/bin/bash

for entry in /tmp/data_grupo01/*
do
    UT="$(echo ${entry} | cut -c 33-35)"
    hdfs dfs -test -e /user/luis.bartol/csv/arq/$UT

    if [ $? -eq 1 ] # Dir not exists
    then 
       hdfs dfs -mkdir /user/luis.bartol/csv/arq/$UT
    fi

    hdfs dfs -put $entry /user/luis.bartol/csv/arq/$UT
    
    if [ $? -eq 0 ]
    then
        echo "[INFO] HDFS IMPORT : $entry - OK" >> /tmp/log_grupo01/dir2hdfs.log
    else
        echo "[ERROR] HDFS IMPORT : $entry - KO" >> /tmp/log_grupo01/dir2hdfs.log
    fi
done
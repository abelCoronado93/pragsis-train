#!/bin/bash

LOGFILE=/tmp/grupo1/log/dir2hdfs.log
HDFS_RAW_DIR=/user/master/grupo1/raw

if [ -z $1 ]; then
	echo "Parameter does not exist"
	echo "Try one.sh -h"

elif [ $1 == "-p" ] || [ $1 == "--path" ]; then

    if [ -z $2 ]; then
        echo "Path does not exists"
        echo "Try ./dir2hdfs.sh -h"
    else
        # TODO : Decompress data in tar.gz ? Better in other script [Oozie]
        DATA_PATH=$2
        for entry in $DATA_PATH/*
        do
            NAMEDIR="$(echo ${entry} | rev | cut -d / -f 1 | rev)"
            UT="$(echo ${NAMEDIR} | cut -d - -f 2 | cut -d _ -f 1)"
            TIME="$(echo ${NAMEDIR} | cut -d _ -f 2 | cut -d T -f 1)"
            YEAR="$(echo ${TIME} | cut -c 1-4)"
            MONTH="$(echo ${TIME} | cut -c 5-6)"
            DAY="$(echo ${TIME} | cut -c 7-8)"

            hdfs dfs -test -e $HDFS_RAW_DIR/ut=$UT

            if [ $? -eq 1 ]
            then
                hdfs dfs -mkdir $HDFS_RAW_DIR/ut=$UT
                echo "[INFO] New HDFS directory - UT Level : ut=$UT" >> $LOGFILE
            fi

            hdfs dfs -test -e $HDFS_RAW_DIR/ut=$UT/year=$YEAR
            if [ $? -eq 1 ]
            then
                hdfs dfs -mkdir $HDFS_RAW_DIR/ut=$UT/year=$YEAR
                echo "[INFO] New HDFS directory - Year level : year=$YEAR" >> $LOGFILE
            fi

            hdfs dfs -test -e $HDFS_RAW_DIR/ut=$UT/year=$YEAR/month=$MONTH
            if [ $? -eq 1 ]
            then
                hdfs dfs -mkdir $HDFS_RAW_DIR/ut=$UT/year=$YEAR/month=$MONTH
                echo "[INFO] New HDFS directory - Month level : month=$MONTH" >> $LOGFILE
            fi

            hdfs dfs -test -e $HDFS_RAW_DIR/ut=$UT/year=$YEAR/month=$MONTH/day=$DAY
            if [ $? -eq 1 ]
            then
                hdfs dfs -mkdir $HDFS_RAW_DIR/ut=$UT/year=$YEAR/month=$MONTH/day=$DAY
                echo "[INFO] New HDFS directory - Day level : day=$DAY" >> $LOGFILE
            fi

            hdfs dfs -put $entry $HDFS_RAW_DIR/ut=$UT/year=$YEAR/month=$MONTH/day=$DAY # Raw

            if [ $? -eq 0 ]
            then
                echo "[INFO] HDFS RAW IMPORT : $entry - OK" >> $LOGFILE
            else
                echo "[ERROR] HDFS RAW IMPORT : $entry - KO" >> $LOGFILE
            fi
        done
    fi
elif [ $1 == "-h" ] || [ $1 == "--help" ]; then
    echo "Usage: ./dir2hdfs.sh [OPTION]"
    echo ""
    echo "Mandatory arguments"
    echo "   -p, --path                                 Process all files in path."

else
    echo "Unknown option $1"
    echo "Try ./dir2hdfs.sh -h"

fi

# TODO : with Oozie -> mv processed files to other location.
# mv $entry /tmp/processed_grupo01
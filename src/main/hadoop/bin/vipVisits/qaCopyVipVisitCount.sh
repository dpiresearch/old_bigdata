#!/bin/bash
# qaCopyVipVisitCount.sh
# Deprecated, use qaCopyFileToBatchServer.sh
#
# This script scp's hadoop output files (vip visit counts) from hdfs into
# Batch pool servers. 
#
# set defaults

DAYS_AGO=0
INPUT_DIR=/user/data/bolt/data/qa/input/search_results

# Output dir on batch pool machine
OUTPUT_DIR=/tmp

# Output file name on batch pool machine
OUTPUT_FILE=vip_visits.csv

while getopts "i:o:a:f:u:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                f) OUTPUT_FILE=${OPTARG};;
                u) USERNAME=${OPTARG};;
        esac
done

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== OUTPUT_DIR:          $OUTPUT_DIR
echo ===== OUTPUT_FILE:         $OUTPUT_FILE
echo ===== USERNAME:         	$USERNAME
echo =================================================

# How many days ago will be day zero of the decay calculation
daysago=$DAYS_AGO

# input and output directories
inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR
username=$USERNAME

currentdate=`date --date="$daysago day ago" +%Y%m%d`
echo "Using date: $currentdate"

result_dest=$RESULT_DESTINATION

# TODO: check for existence otherwise the shell will complain
# TODO:  Need to change this for the batch/kernel production pool, not just for qa

echo "hadoop fs -cat $inputdir/$currentdate/part* > /tmp/vip_visit_count.csv"
hadoop fs -cat $inputdir/$currentdate/part* > /tmp/vip_visit_count.csv

# Better to have the batch client pull this instead of pulling, then
# we don't have to guess where to put this file

echo "Copying result files to batch pool"
echo "scp /tmp/vip_visit_count.csv ${username}@bo-qabatch001:$outputdir"
scp /tmp/vip_visit_count.csv ${username}@bo-qabatch001:$outputdir
echo "/tmp/vip_visit_count.csv ${username}@bo-qabatch002:$outputdir"
scp /tmp/vip_visit_count.csv ${username}@bo-qabatch002:$outputdir
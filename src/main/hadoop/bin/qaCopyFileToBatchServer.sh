#!/bin/bash
# qaCopyFileToBatchServer.sh
#
# Generic script which scp's hadoop output files from
# hdfs into Batch pool servers.
#
# Currently being used by vipvisits, phoneclick count jobs.
#
# set defaults

DAYS_AGO=0
INPUT_DIR=/user/data/bolt/data/qa/input/phone_click

# Output dir on batch pool machine
OUTPUT_DIR=/tmp

# Output file name
FILE_NAME=overide_this.csv

while getopts "i:o:a:f:u:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                f) FILE_NAME=${OPTARG};;
                u) USERNAME=${OPTARG};;
        esac
done

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== OUTPUT_DIR:          $OUTPUT_DIR
echo ===== FILE_NAME:           $FILE_NAME
echo ===== USERNAME:         	$USERNAME
echo =================================================

# How many days ago will be day zero of the decay calculation
daysago=$DAYS_AGO

# input and output directories
inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR
filename=$FILE_NAME
username=$USERNAME

currentdate=`date --date="$daysago day ago" +%Y%m%d`
echo "Using date: $currentdate"

# TODO: check for existence otherwise the shell will complain
# TODO:  Need to change this for the batch/kernel production pool, not just for qa

echo Copying hdfs file contents into temp directory...
echo "hadoop fs -cat $inputdir/$currentdate/part* > /tmp/$FILE_NAME"
hadoop fs -cat $inputdir/$currentdate/part* > /tmp/$FILE_NAME


echo "Copying result files to batch pool.."
echo "scp /tmp/$filename ${username}@bo-qabatch001:$outputdir"
scp /tmp/$filename ${username}@bo-qabatch001:$outputdir

echo "/tmp/$filename ${username}@bo-qabatch002:$outputdir"
scp /tmp/$filename ${username}@bo-qabatch002:$outputdir
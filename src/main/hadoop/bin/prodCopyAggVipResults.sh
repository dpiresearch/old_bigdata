#!/bin/bash
#
# Kick off the script to copy results of aggregate vip calculations 
#

# set defaults
DAYS_AGO=1
INPUT_DIR=/user/data/bolt/data/qa/input/search_results
OUTPUT_DIR=/user/data/bolt/data/qa/daily/aggvip
DEPLOY_DIR=~/pig
NUM_DAYS_BACK=30

while getopts "i:o:d:a:b:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                b) NUM_DAYS_BACK=${OPTARG};;
        esac
done

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== OUTPUT_DIR:          $OUTPUT_DIR
echo ===== DEPLOY_DIR:          $DEPLOY_DIR
echo ===== NUM_DAYS_BACK:       $NUM_DAYS_BACK
echo =================================================

# How many days ago will be day zero of the decay calculation
daysago=$DAYS_AGO

# deploy dir
deploydir=$DEPLOY_DIR

# input and output directories
inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR

currentdate=`date --date="$daysago day ago" +%Y%m%d`

# TODO: check for existence otherwise the shell will complain
# TODO:  Need to change this for the batch/kernel production pool, not just for qa

echo "hadoop fs -cat $inputdir/$currentdate/aggvip/part* > /tmp/agg_vip.csv"
hadoop fs -cat $inputdir/$currentdate/aggvip/part* > /tmp/agg_vip.csv

# Better to have the batch client pull this instead of pulling, then
# we don't have to guess where to put this file

#echo "Copying ressult files to batch pool"
scp /tmp/agg_vip.csv batch001:/tmp
scp /tmp/agg_vip.csv batch002:/tmp


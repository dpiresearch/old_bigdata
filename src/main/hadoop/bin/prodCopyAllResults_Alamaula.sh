#!/bin/bash
#
# Kick off the script to calculate an aggregate of all keywords over the past 30 days
#

# set defaults
DAYS_AGO=1
INPUT_DIR=/user/data/bolt/data/qa/input/search_results
OUTPUT_DIR=/user/data/bolt/data/qa/daily/popular_search
DEPLOY_DIR=~/pig
NUM_DAYS_BACK=30
KSS_OUTPUT=/tmp/keyword_search_sum_alamaula.csv
ASW_OUTPUT=/tmp/popularsearchresults_alamaula.csv

while getopts "i:o:d:a:b:k:w:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                k) KSS_OUTPUT=${OPTARG};;
                w) ASW_OUTPUT=${OPTARG};;
        esac
done

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== OUTPUT_DIR:          $OUTPUT_DIR
echo ===== DEPLOY_DIR:          $DEPLOY_DIR
echo ===== NUM_DAYS_BACK:       $NUM_DAYS_BACK
echo ===== KSS_OUTPUT:          $KSS_OUTPUT 
echo ===== ASW_OUTPUT:          $ASW_OUTPUT
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

# echo "hadoop fs -cat $inputdir/$currentdate/sum/part* > /tmp/keyword_search_sum.csv"
# hadoop fs -cat $inputdir/$currentdate/sum/part* > /tmp/keyword_search_sum.csv

echo "hadoop fs -cat $inputdir/$currentdate/dedupe_sum/part* > $KSS_OUTPUT" 
hadoop fs -cat $inputdir/$currentdate/dedupe_sum/part* > $KSS_OUTPUT

echo "hadoop fs -cat $inputdir/$currentdate/all_search_words/part* > $ASW_OUTPUT"
hadoop fs -cat $inputdir/$currentdate/all_search_words/part* > $ASW_OUTPUT

# This is the alamaula version, no need to copy to batch pool


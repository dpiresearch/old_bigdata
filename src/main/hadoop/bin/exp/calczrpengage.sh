#!/bin/bash
#
# Calculate the number of zero result pages for X days back
# where X is determined by the -b parameter
# For now, only the NUM_DAYS_BACK parameter is used
#

DAYS_AGO=1
INPUT_DIR=/user/data/bolt/data/qa/daily/popular_search
OUTPUT_DIR=/user/data/bolt/data/qa/daily/popular_search
DEPLOY_DIR=~/pig
NUM_DAYS_BACK=30
REF_DIR=/user/data/bolt/data/ref

while getopts "i:o:d:a:b:r:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                b) NUM_DAYS_BACK=${OPTARG};;
                r) REF_DIR=${OPTARG};;
        esac
done

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== OUTPUT_DIR:          $OUTPUT_DIR
echo ===== DEPLOY_DIR:          $DEPLOY_DIR
echo ===== REF_DIR:             $REF_DIR
echo ===== NUM_DAYS_BACK:       $NUM_DAYS_BACK
echo =================================================


# How many days ago will be day zero of the decay calculation
daysago=$DAYS_AGO

# deploy dir
deploydir=$DEPLOY_DIR

# We're supposed to do this for 30 days
refdir=$REF_DIR

inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR

# Wipe out all the scores - we're going to recreate them here
# hadoop fs -rmr $outputdir/*/scores

for ((var=0;var<$NUM_DAYS_BACK;++var));
do
  setday=$((var+$daysago)) 
  thedate=`date --date="$setday day ago" +%Y%m%d`; 
  echo $thedate
  echo pig -param inputdir=/user/hadoop/data/prod/ingest -param daydate=$thedate -param deploydir=~/current -param outputdir=/user/hadoop/data/exp -f ~/pig/searchViewCount.pig
#   pig -param inputdir=/user/hadoop/data/prod/ingest -param daydate=$thedate -param deploydir=~/current -param outputdir=/user/hadoop/data/exp -f ~/pig/searchViewCount.pig

done


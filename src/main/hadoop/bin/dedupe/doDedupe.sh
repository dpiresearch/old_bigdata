#!/bin/bash
#
# Kick off the script to calculate an aggregate of all keywords over the past 30 days
#

# set defaults
DAYS_AGO=1
SEARCHLOG_INPUT_DIR=/user/hadoop/data/prod/ingest/search_results
POPSEARCH_JOB_DIR=/user/hadoop/data/prod/jobs/popular_search
OUTPUT_DIR=/user/hadoop/data/exp/dedupe
DEPLOY_DIR=~/releases/1.1.82
NUM_DAYS_BACK=30
THRESHOLD=0.2

while getopts "i:o:d:a:b:t:p:" option
do
        case "${option}" in

                i) SEARCHLOG_INPUT_DIR=${OPTARG};;
                p) POPSEARCH_INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                b) NUM_DAYS_BACK=$OPTARG;;
                t) THRESHOLD=$OPTARG;;
        esac
done

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== SEARCHLOG_INPUT_DIR: $SEARCHLOG_INPUT_DIR
echo ===== POPSEARCH_INPUT_DIR: $POPSEARCH_INPUT_DIR
echo ===== OUTPUT_DIR:          $OUTPUT_DIR
echo ===== DEPLOY_DIR:          $DEPLOY_DIR
echo ===== NUM_DAYS_BACK:       $NUM_DAYS_BACK
echo ===== THRESHOLD:           $THRESHOLD
echo =================================================

# How many days ago will be day zero of the decay calculation
daysago=$DAYS_AGO

# deploy dir
deploydir=$DEPLOY_DIR

# input and output directories
searchlog_inputdir=$SEARCHLOG_INPUT_DIR
popsearch_inputdir=$POPSEARCH_INPUT_DIR
outputdir=$OUTPUT_DIR

currentdate=`date --date="$daysago day ago" +%Y%m%d`
alldate="{$currentdate"
# alldate="{"

threshold=$THRESHOLD

# We're supposed to do this for 30 days
for ((var=1;var<$NUM_DAYS_BACK;++var));
do
  setday=$((var+$daysago)) 
  thedate=`date --date="$setday day ago" +%Y%m%d`; 
  

  alldate="$alldate,$thedate";

  # TODO: check for existence otherwise the shell will complain

done

alldate="$alldate}"
echo $alldate

# Remove the previous results
echo hadoop fs -rmr $outputdir/$currentdate/rejects
hadoop fs -rmr $outputdir/$currentdate/rejects
echo hadoop fs -rmr $outputdir/$currentdate/dedupe_sum
hadoop fs -rmr $outputdir/$currentdate/dedupe_sum

# Execute the script
pig -param popsearch_inputdir=$popsearch_inputdir -param searchlog_inputdir=$searchlog_inputdir -param outputdir=$outputdir -param alldate=$alldate -param thedate=$currentdate -param threshold=$threshold -param deploydir=$deploydir -f $deploydir/pig/dedupe/searchadid.pig

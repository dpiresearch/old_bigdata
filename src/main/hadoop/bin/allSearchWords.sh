#!/bin/bash
#
# Kick off the script to calculate an aggregate of all keywords over the past 30 days
#

# set defaults
DAYS_AGO=6
INPUT_DIR=/user/data/bolt/data/qa/input/search_results
OUTPUT_DIR=/user/data/bolt/data/qa/daily/popular_search
DEPLOY_DIR=~/pig
NUM_DAYS_BACK=30

while getopts "i:o:d:a:b:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                b) NUM_DAYS_BACK=$OPTARG;;
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
alldate="{$currentdate"
# alldate="{"

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
# /user/data/bolt/data/popular_search/20130618/all_search_words
echo hadoop fs -rmr $outputdir/$currentdate/all_search_words
hadoop fs -rmr $outputdir/$currentdate/all_search_words

# Execute the script
echo pig -param alldate="$alldate" -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$currentdate -f ~/pig/searchAllWords.pig
pig -param alldate="$alldate" -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$currentdate -f $deploydir/searchAllWords.pig


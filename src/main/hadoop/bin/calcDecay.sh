#!/bin/bash
#
# Calculate the decay for a set of keywords based on the past 30 days
#
# Need input for offset
# Need to create paths to store intermediate calculations
# Call another pig script to perform summation
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
hadoop fs -rmr $outputdir/*/scores

for ((var=0;var<$NUM_DAYS_BACK;++var));
do
  setday=$((var+$daysago)) 
  thedate=`date --date="$setday day ago" +%Y%m%d`; 
  echo $thedate

  echo pig -param deploydir=$deploydir -param refdir=$refdir -param inputdir=$inputdir -param outputdir=$outputdir -param day=$var -param daydate=$thedate -f $deploydir/scorekeywords.pig
  pig -param deploydir=$deploydir -param refdir=$refdir -param inputdir=$inputdir -param outputdir=$outputdir -param day=$var -param daydate=$thedate -f $deploydir/pig/scorekeywords.pig
done


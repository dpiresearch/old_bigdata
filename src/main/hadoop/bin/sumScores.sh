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
DEPLOY_DIR=~
NUM_DAYS_BACK=30
REF_DIR=/user/data/bolt/data/ref

while getopts "i:o:d:a:b:r:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                b) NUM_DAYS_BACK=$OPTARG;;
                r) REF_DIR=$OPTARG;;
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

refdir=$REF_DIR

inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR

# We're assuming here that all the scores we need to sum are in $INPUT_DIR
# and any extraneous score directories were wiped out in the previous process: calcDecay.sh
# By default we're going to do this for 30 days

setday=$((var+$daysago)) 
thedate=`date --date="$setday day ago" +%Y%m%d`; 
echo $thedate

# TODO: check for existence otherwise the shell will complain
echo hadoop fs -rmr $outputdir/$thedate/sum
hadoop fs -rmr $outputdir/$thedate/sum

pig -param deploydir=$deploydir -param inputdir=$inputdir -param outputdir=$outputdir -param refdir=$refdir -param daydate=$thedate -f $DEPLOY_DIR/pig/sumkeywords.pig


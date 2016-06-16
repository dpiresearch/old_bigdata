#!/bin/bash
#
# Kick off the script to copy results of aggregate vip calculations 
#

# set defaults
DAYS_AGO=6
INPUT_DIR=/user/data/bolt/data/qa/tests/data
OUTPUT_DIR=/user/data/bolt/data/qa/daily/aggvip
DEPLOY_DIR=~/pig
NUM_DAYS_BACK=30
RESULT_DESTINATION=dapang@bo-qabatch001


while getopts "i:o:d:a:b:r:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                b) NUM_DAYS_BACK=${OPTARG};;
                r) RESULT_DESTINATION=$OPTARG;;
        esac
done

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== OUTPUT_DIR:          $OUTPUT_DIR
echo ===== DEPLOY_DIR:          $DEPLOY_DIR
echo ===== NUM_DAYS_BACK:       $NUM_DAYS_BACK
echo ===== RESULT_DESTINATION:  $RESULT_DESTINATION
echo =================================================

# How many days ago will be day zero of the decay calculation
daysago=$DAYS_AGO

# deploy dir
deploydir=$DEPLOY_DIR

# input and output directories
inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR

currentdate=`date --date="$daysago day ago" +%Y%m%d`
echo $currentdate

result_dest=$RESULT_DESTINATION

# TODO: check for existence otherwise the shell will complain
# TODO:  Need to change this for the batch/kernel production pool, not just for qa

echo rm -fr popular_search
rm -fr popular_search

echo hadoop fs -copyToLocal $inputdir/popular_search .
hadoop fs -copyToLocal $inputdir/popular_search .


cat popular_search/*/scores/part* > ~/keyword_scores.csv
cat popular_search/${currentdate}/sum/part* > ~/keyword_sum.csv

# Better to have the batch client pull this instead of pulling, then
# we don't have to guess where to put this file

echo "Copying result files to batch pool"
echo scp ~/keyword_scores.csv ${username}@bo-qabatch001:/tmp
scp ~/keyword_scores.csv ${username}@bo-qabatch001:/tmp
scp ~/keyword_scores.csv ${username}@bo-qabatch002:/tmp

scp ~/keyword_sum.csv ${username}@bo-qabatch001:/tmp
scp ~/keyword_sum.csv ${username}@bo-qabatch002:/tmp


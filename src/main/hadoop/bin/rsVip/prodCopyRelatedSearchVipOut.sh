#!/bin/bash
#
# Kick off the script to copy related search for vip
#

# set defaults
DAYS_AGO=1
INPUT_DIR=/user/dapang/data/prod/popular_search
OUTPUT_DIR=/user/dapang/data/prod/popular_search
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

echo "hadoop fs -cat $inputdir/$currentdate/rs_sum/part* > /tmp/rs_vip.csv"
hadoop fs -cat $inputdir/$currentdate/rs_sum/part* > /tmp/rs_vip.csv

# Better to have the batch client pull this instead of pulling, then
# we don't have to guess where to put this file

#echo "Copying ressult files to batch pool"
scp /tmp/rs_vip.csv batch001:/tmp
scp /tmp/rs_vip.csv batch002:/tmp
# scp /tmp/rs_vip.csv batch003:/tmp


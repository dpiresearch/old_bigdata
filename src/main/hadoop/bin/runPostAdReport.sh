#!/bin/bash

# Kick off the script to calculate an aggregate of all keywords over the past 30 days
# dapang@hadname001:~/bin$ pig -param thedate=20131215 -f postad.pig
#
#

# How many days ago will be day zero of the decay calculation
DAYS_AGO=1

# Deploy dir
DEPLOY_DIR=/home/hadoop/current

# output dir
OUTPUT_DIR=/user/hadoop/data/exp/post_ad

# input dir
INPUT_DIR=/user/hadoop/data/prod/ingest/post_ad

MESSAGE_FILE=/home/hadoop/report/post_ad/runPostAd.msg

while getopts "a:d:e:i:o:m:" option
do
        case "${option}" in
                a) DAYS_AGO=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                e) EMAIL_LIST=${OPTARG};;
                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                m) MESSAGE_FILE=${OPTARG};;
        esac
done

daysago=$DAYS_AGO
deploydir=$DEPLOY_DIR
outputdir=$OUTPUT_DIR
inputdir=$INPUT_DIR
messagefile=$MESSAGE_FILE

displaydate=`date --date="$daysago day ago" +%m-%d-%Y`
thedate=`date --date="$daysago day ago" +%Y%m%d`

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== OUTPUT_DIR:          $OUTPUT_DIR
echo ===== DEPLOY_DIR:          $DEPLOY_DIR
echo ===== EMAIL_LIST:          $EMAIL_LIST
echo ===== Displaydate:         $displaydate
echo =================================================



# disable removing for now to avoid accidentally removing directories

# echo hadoop fs -rmr $outputdir/$thedate/asw
hadoop fs -rmr $outputdir/$thedate/attr_errors_by_ccle
hadoop fs -rmr $outputdir/$thedate/field_errors_by_ccle
hadoop fs -rmr $outputdir/$thedate/other_errors_by_ccle

pig -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$thedate -f $deploydir/pig/postad.pig

hadoop fs -cat $outputdir/$thedate/attr_errors_by_ccle/part* > /tmp/attr_post_ad_errors.csv
hadoop fs -cat $outputdir/$thedate/field_errors_by_ccle/part* > /tmp/field_post_ad_errors.csv
hadoop fs -cat $outputdir/$thedate/other_errors_by_ccle/part* > /tmp/other_post_ad_errors.csv

mutt -s "Post Ad report for $displaydate" -a /tmp/attr_post_ad_errors.csv -a /tmp/field_post_ad_errors.csv -a /tmp/other_post_ad_errors.csv  -- $EMAIL_LIST  < $messagefile


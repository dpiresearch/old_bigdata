#!/bin/bash

# Kick off the script to aggregate counts of all reply emails
# by country, email, category, location
#
# Todo: make the input and output directories configurable via shell parameters
#

daysago=1

displaydate=`date --date="$daysago day ago" +%m-%d-%Y`
thedate=`date --date="$daysago day ago" +%Y%m%d`
year=`date  --date="$daysago day ago" +%Y`
moday=`date --date="$daysago day ago" +%m%d`

filebase="Replylog_AMS_"

# Deploy dir
deploydir=/home/hadoop/current

# output dir
outputdir=/user/hadoop/data/prod/jobs/fordwh/reply_ad

# input dir
inputdir=/user/hadoop/data/prod/ingest
# inputdir=/user/hadoop/data/prod/ingest/reply_ad

# job output dir
jobdir=/user/hadoop/data/prod/jobs

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $daysago
echo ===== INPUT_DIR:           $inputdir
echo ===== OUTPUT_DIR:          $outputdir
echo ===== JOB_DIR:             $jobdir
echo ===== DEPLOY_DIR:          $deployodir
echo ===== Displaydate:         $displaydate
echo =================================================

# disable removing for now to avoid accidentally removing directories

echo hadoop fs -rmr $outputdir/$thedate/fordwh
hadoop fs -rmr $outputdir/$thedate/fordwh
hadoop fs -rmr $outputdir/$thedate/fordwh_old
hadoop fs -rmr $outputdir/$thedate/fordwh_complete

echo pig -param inputdir=$inputdir -param outputdir=$outputdir -param jobdir=$jobdir -param thedate=$thedate -f $deploydir/pig/reply_ad/reply_ad.pig
pig -param inputdir=$inputdir -param outputdir=$outputdir -param jobdir=$jobdir -param thedate=$thedate -f $deploydir/pig/reply_ad/reply_ad.pig

echo hadoop fs -cat $outputdir/$thedate/fordwh/part* > /tmp/$filebase$thedate.csv
hadoop fs -cat $outputdir/$thedate/fordwh/part* > /tmp/$filebase$thedate.csv

cd /tmp
rm /tmp/$filebase$thedate.csv.gz
gzip /tmp/$filebase$thedate.csv

touch /tmp/$filebase$thedate.touchfile

#
# Marketo output files
#
# extract the data
echo "hadoop fs -cat $jobdir/replyObjects/$thedate/replies/part* > /tmp/reply_object_body.csv"
hadoop fs -cat $jobdir/replyObjects/$thedate/replies/part* > /tmp/reply_object_body.csv

echo "cat $deploydir/ref/boomi_header.csv /tmp/ad_object_body.csv > /tmp/Post-$year.$moday.csv"
cat $deploydir/ref/boomi_reply_header.csv /tmp/reply_object_body.csv > /tmp/Replies-$year.$moday.csv


#!/bin/bash

# Kick off the script to calculate an aggregate of all keywords over the past 30 days
# dapang@hadname001:~/bin$ pig -param thedate=20131215 -f postad.pig 
#
#

# How many days ago will be day zero of the decay calculation
daysago=1

displaydate=`date --date="$daysago day ago" +%m-%d-%Y`
thedate=`date --date="$daysago day ago" +%Y%m%d`

# Deploy dir
deploydir=/home/hadoop/pig/reports

# output dir
outputdir=/user/hadoop/data/exp/reply_ad

# input dir
inputdir=/user/hadoop/data/prod/ingest/reply_ad

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $daysago
echo ===== INPUT_DIR:           $inputdir
echo ===== OUTPUT_DIR:          $outputdir
echo ===== DEPLOY_DIR:          $deployodir
echo ===== Displaydate:         $displaydate
echo =================================================

# disable removing for now to avoid accidentally removing directories

echo hadoop fs -rmr $outputdir/$thedate/reply_ad_by_cat
hadoop fs -rmr $outputdir/$thedate/reply_ad_by_cat

pig -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$thedate -f $deploydir/replyad.pig

hadoop fs -cat $outputdir/$thedate/reply_ad_by_cat/part* > /tmp/reply_ad_by_cat.csv

mutt -s "Reply Ad report for $displaydate" -a /tmp/reply_ad_by_cat.csv -- dapang@ebay.com olemmers@ebay.com sseamon@ebay.com < ~/report/reply_ad/runReplyAd.msg

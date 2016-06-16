#!/bin/bash

# Kick off the script to aggregate counts of all reply emails
# by country, email, category, location
#

DAYS_AGO=1
INPUT_DIR=/user/hadoop/data/pp/ingest/reply_ad
OUTPUT_DIR=/user/hadoop/data/pp/jobs/fordwh/reply_ad
DEPLOY_DIR=/home/hadoop/current
REF_DIR=/user/hadoop/data/pp/ref
LOC_FILE=locnames
CAT_FILE=catnames
FILEBASE=replylog_PHX_

while getopts "i:o:d:r:l:c:a:f:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                r) REF_DIR=${OPTARG};;
                l) LOC_FILE=${OPTARG};;
                c) CAT_FILE=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                f) FILEBASE=${OPTARG};;
        esac
done

deploydir=$DEPLOY_DIR
inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR
refdir=$REF_DIR
locfile=$LOC_FILE
catfile=$CAT_FILE
daysago=$DAYS_AGO

daysago=$DAYS_AGO

displaydate=`date --date="$daysago day ago" +%m-%d-%Y`
thedate=`date --date="$daysago day ago" +%Y%m%d`
year=`date  --date="$daysago day ago" +%Y`
moday=`date --date="$daysago day ago" +%m%d`

filebase=$FILEBASE
catnames=$CAT_FILE
locnames=$LOC_FILE

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $daysago
echo ===== INPUT_DIR:           $inputdir
echo ===== OUTPUT_DIR:          $outputdir
echo ===== DEPLOY_DIR:          $deployodir
echo ===== REF_DIR:             $refdir
echo ===== Displaydate:         $displaydate
echo ===== locfile:             $locfile
echo ===== catfile:             $catfile
echo ===== FILEBASE:            $filebase
echo =================================================

# disable removing for now to avoid accidentally removing directories

echo hadoop fs -rmr $outputdir/$thedate/fordwh
hadoop fs -rmr $outputdir/$thedate/fordwh
hadoop fs -rmr $outputdir/$thedate/forMarketo

echo pig -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$thedate -param refdir=$refdir -param catfile=catnames -param locfile=locnames -f $deploydir/pig/reply_ad/reply_ad.pig
pig -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$thedate -param refdir=$refdir -param catfile=$catnames -param locfile=$locnames -f $deploydir/pig/reply_ad/reply_ad.pig

#
# Extract Reply Log data for dwh
#
echo hadoop fs -cat $outputdir/$thedate/fordwh/part* > /tmp/$filebase$thedate.csv
hadoop fs -cat $outputdir/$thedate/fordwh/part* > /tmp/$filebase$thedate.csv

cd /tmp
rm /tmp/$filebase$thedate.csv.gz
gzip /tmp/$filebase$thedate.csv

touch /tmp/$filebase$thedate.touchfile

#
# Extract the data for Marketo
#
echo hadoop fs -cat $outputdir/$thedate/forMarketo/part* > /tmp/reply_object_body.csv
hadoop fs -cat $outputdir/$thedate/forMarketo/part* > /tmp/reply_object_body.csv

echo cat $deploydir/ref/boomi_header_reply.csv /tmp/reply_object_body.csv > /tmp/Reply-$year.$moday.csv
cat $deploydir/ref/boomi_header_reply.csv /tmp/reply_object_body.csv > /tmp/Reply-$year.$moday.csv


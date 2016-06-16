#!/bin/bash
#
# Calculate the decay for a set of keywords based on the past 30 days
#
# Need input for offset
# Need to create paths to store intermediate calculations
# Call another pig script to perform summation
#

INPUT_DIR=/user/hadoop/data/prod/ingest/ad_events
OUTPUT_DIR=/user/hadoop/data/prod/ingest/ad_events
DEPLOY_DIR=~/current/pig/adEvents
REF_DIR=/user/hadoop/data/prod/ref
LOC_FILE=mx_loc.csv
DB_DUMP_DATE=20140213

while getopts "i:o:d:p:r:l:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                r) REF_DIR=${OPTARG};;
                l) LOC_FILE=${OPTARG};;
                p) DB_DUMP_DATE=${OPTARG};;
        esac
done

echo ===== Running with the following parameters =====
echo ===== Hdfs Input Directory:  $INPUT_DIR
echo ===== Hdfs Output Directory: $OUTPUT_DIR
echo ===== Deploy Directory       $DEPLOY_DIR
echo ===== Reference Directory    $REF_DIR
echo ===== Location File          $LOC_FILE
echo ===== Database Dump Date:    $DB_DUMP_DATE
echo =================================================

deploydir=$DEPLOY_DIR
inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR
refdir=$REF_DIR
dumpdate=$DB_DUMP_DATE
locfile=$LOC_FILE

# TODO: check for existence otherwise the shell will complain
echo hadoop fs -rmr $outputdir/order_by_start_date
hadoop fs -rmr $outputdir/order_by_start_date
echo hadoop fs -rmr $outputdir/active_ads_no_start_date
hadoop fs -rmr $outputdir/active_ads_no_start_date

echo pig -param inputdir=$inputdir -param outputdir=$outputdir -param dumpdate=$dumpdate -param refdir=$refdir -param locfile=$locfile -f $DEPLOY_DIR/pig/adEvents/adEvents.pig
pig -param inputdir=$inputdir -param outputdir=$outputdir -param dumpdate=$dumpdate -param refdir=$refdir -param locfile=$locfile -f $DEPLOY_DIR/pig/adEvents/adEvents.pig


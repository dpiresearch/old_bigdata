#!/bin/bash
#
# Aggregate the popularity score across all combinations of
# category and location nodes 
#
DAYS_AGO=1
INPUT_DIR=/user/hadoop/data/prod/jobs/popular_search
OUTPUT_DIR=/user/hadoop/data/prod/jobs/popular_search
DEPLOY_DIR=~/current/
REF_DIR=/user/hadoop/data/prod/ref
LOC_FILE=za_loc.csv
CAT_FILE=categories.csv
DB_DUMP_DATE=20140213

while getopts "i:o:d:p:r:l:c:a:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                r) REF_DIR=${OPTARG};;
                l) LOC_FILE=${OPTARG};;
                p) DB_DUMP_DATE=${OPTARG};;
                c) CAT_FILE=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
        esac
done

echo ===== Running with the following parameters =====
echo ===== Hdfs Input Directory:  $INPUT_DIR
echo ===== Hdfs Output Directory: $OUTPUT_DIR
echo ===== Deploy Directory       $DEPLOY_DIR
echo ===== Reference Directory    $REF_DIR
echo ===== Location File          $LOC_FILE
echo ===== Categories File        $CAT_FILE
echo ===== Database Dump Date:    $DB_DUMP_DATE
echo ===== Days Ago:              $DAYS_AGO
echo =================================================

deploydir=$DEPLOY_DIR
inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR
refdir=$REF_DIR
dumpdate=$DB_DUMP_DATE
locfile=$LOC_FILE
catfile=$CAT_FILE
daysago=$DAYS_AGO

thedate=`date --date="$daysago day ago" +%Y%m%d`;

echo "Calculating for date: $thedate"

# TODO: check for existence otherwise the shell will complain
echo hadoop fs -rmr $outputdir/$thedate/aggsums
hadoop fs -rmr $outputdir/$thedate/aggsums

echo pig -param inputdir=$inputdir -param thedate=$thedate -param outputdir=$outputdir -param refdir=$refdir -param locfile=$locfile -param catfile=$catfile -f $deploydir/pig/aggScores.pig
pig -param inputdir=$inputdir -param thedate=$thedate -param outputdir=$outputdir -param refdir=$refdir -param locfile=$locfile -param catfile=$catfile -f $deploydir/pig/aggScores.pig


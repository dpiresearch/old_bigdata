#!/bin/bash

DAYS_AGO=1
INPUT_DIR=/user/hadoop/data/pp
OUTPUT_DIR=/user/hadoop/data/pp/jobs/adObjects
DEPLOY_DIR=~/current
REF_DIR=/user/hadoop/data/pp/ref
LOC_FILE=za_loc.csv
CAT_FILE=categories.csv

while getopts "i:o:d:r:l:c:a:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                r) REF_DIR=${OPTARG};;
                l) LOC_FILE=${OPTARG};;
                c) CAT_FILE=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
        esac
done

echo ===== Running with the following parameters =====
echo ===== Hdfs Input Directory:  -i $INPUT_DIR
echo ===== Hdfs Output Directory: -o $OUTPUT_DIR
echo ===== Deploy Directory       -d $DEPLOY_DIR
echo ===== Reference Directory    -r $REF_DIR
echo ===== Location File          -l $LOC_FILE
echo ===== Categories File        -c $CAT_FILE
echo ===== Days ago               -a $DAYS_AGO
echo =================================================

deploydir=$DEPLOY_DIR
inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR
refdir=$REF_DIR
locfile=$LOC_FILE
catfile=$CAT_FILE
daysago=$DAYS_AGO

thedate=`date --date="$daysago day ago" +%Y%m%d`
year=`date  --date="$daysago day ago" +%Y`
moday=`date --date="$daysago day ago" +%m%d`

echo "thedate=$thedate"
#echo $year
#echo $moday

# TODO: check for existence otherwise the shell will complain
echo hadoop fs -rmr $outputdir/$thedate
hadoop fs -rmr $outputdir/$thedate/ad_obj
hadoop fs -rmr $outputdir/$thedate/reply_count

echo pig -param inputdir=$inputdir -param outputdir=$outputdir -param deploydir=$deploydir -param refdir=$refdir -param catfile=$catfile -param locfile=$locfile -param thedate=$thedate -f $deploydir/pig/postad/adObjects.pig

pig -param inputdir=$inputdir -param outputdir=$outputdir -param deploydir=$deploydir -param refdir=$refdir -param catfile=$catfile -param locfile=$locfile -param thedate=$thedate -f $deploydir/pig/postad/adObjects.pig

# extract the data
echo "hadoop fs -cat $outputdir/$thedate/ad_obj/part* > /tmp/ad_object_body.csv"
hadoop fs -cat $outputdir/$thedate/ad_obj/part* > /tmp/ad_object_body.csv

echo "cat $deploydir/ref/boomi_header.csv /tmp/ad_object_body.csv > /tmp/Post-$year.$moday.csv"
cat $deploydir/ref/boomi_header.csv /tmp/ad_object_body.csv > /tmp/Post-$year.$moday.csv

rm /tmp/ad_object_body.csv


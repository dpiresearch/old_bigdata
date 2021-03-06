#!/bin/bash
# phoneClickCount.sh
#
# Execute pig script to calculate phone click count for all ads

INPUT_DIR=/user/data/bolt/data/events
OUTPUT_DIR=/user/data/bolt/data/output/phone_click
DEPLOY_DIR=~/pig
DAYS_AGO=1

while getopts "i:o:d:a:c:" option
do
        case "${option}" in
                i) INPUT_DIR=${OPTARG};;
                c) CLICKS_OLD=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
        esac
done

echo ===== Running with the following parameters =====
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== CLICKS_OLD:          $CLICKS_OLD
echo ===== OUTPUT_DIR:          $OUTPUT_DIR
echo ===== DEPLOY_DIR:          $DEPLOY_DIR
echo ===== DAYS_AGO:            $DAYS_AGO
echo =================================================

deploydir=$DEPLOY_DIR
inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR
daysago=$DAYS_AGO

thedate=`date --date="$daysago day ago" +%Y%m%d`;
echo Date:$thedate

# Delete only specific date subfolder
#if [ !-z $outputdir ]
#then
    echo Deleting output directory...
    echo hadoop fs -rm -r -skipTrash $outputdir/$thedate
    hadoop fs -rm -r -skipTrash $outputdir/$thedate
#fi

echo Executing pig script ...
echo pig -param inputdir=$inputdir -param oldClicks=$CLICKS_OLD -param outputdir=$outputdir -param thedate=$thedate -f $DEPLOY_DIR/pig/events/phoneClickCount.pig
pig -param inputdir=$inputdir -param oldClicks=$CLICKS_OLD -param outputdir=$outputdir -param thedate=$thedate -param deploydir=$deploydir -f $DEPLOY_DIR/pig/events/phoneClickCount.pig

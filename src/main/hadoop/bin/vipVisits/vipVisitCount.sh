#!/bin/bash
# vipVisitCount.sh
#
# Execute pig script to calculate view ad count regardless of
# referrer, duplicates etc.

INPUT_DIR=/user/data/bolt/data/viewad
OUTPUT_DIR=/user/data/bolt/data/viewad/vip_visits
DEPLOY_DIR=~/pig
DAYS_AGO=1

while getopts "i:o:d:a:v:" option
do
        case "${option}" in
                i) INPUT_DIR=${OPTARG};;
                v) VIEWAD_FILE=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
        esac
done

echo ===== Running with the following parameters =====
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== VIEWAD_FILE:         $VIEWAD_FILE
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
echo pig -param inputdir=$inputdir -param oldViews=$VIEWAD_FILE -param outputdir=$outputdir -param thedate=$thedate -param deploydir=$deploydir -f $DEPLOY_DIR/pig/vipVisits/vipVisitCount.pig
pig -param inputdir=$inputdir -param oldViews=$VIEWAD_FILE -param outputdir=$outputdir -param thedate=$thedate -param deploydir=$deploydir -f $DEPLOY_DIR/pig/vipVisits/vipVisitCount.pig

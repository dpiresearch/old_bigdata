#!/bin/bash
# dedupeAggVip.sh
#
# Execute pig script to deduplicate aggregate exernal vip visits
#
INPUT_DIR=/user/data/bolt/data/events
OUTPUT_DIR=/user/data/bolt/data/output/agg_vip
DEPLOY_DIR=~/pig
DAYS_AGO=1

while getopts "i:o:d:a:" option
do
        case "${option}" in
                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
        esac
done

echo ===== Running with the following parameters =====
echo ===== INPUT_DIR:           $INPUT_DIR
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

olddate=`date --date="$((daysago+1)) day ago" +%Y%m%d`;
echo Date:$olddate

# Delete only specific date subfolder
#if [ !-z $outputdir ]
#then
    echo Deleting output directory...
    echo hadoop fs -rm -r -skipTrash $outputdir/$thedate
    hadoop fs -rm -r -skipTrash $outputdir/$thedate/aggVip
    hadoop fs -rm -r -skipTrash $outputdir/$thedate/aggDeltaVip
#fi

echo Executing pig script ...

pig -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$thedate -param olddate=$olddate -param deploydir=$deploydir -f $DEPLOY_DIR/pig/aggVip/dedupeAggVip.pig



#!/bin/bash
#
# Calculate the decay for a set of keywords based on the past 30 days
#
# Need input for offset
# Need to create paths to store intermediate calculations
# Call another pig script to perform summation
#

INPUT_DIR=/user/data/bolt/data/viewad
OUTPUT_DIR=/user/data/bolt/data/viewad/agg_vip
DEPLOY_DIR=~

while getopts "i:o:d:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
        esac
done

echo ===== Running with the following parameters =====
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== OUTPUT_DIR:          $OUTPUT_DIR
echo ===== DEPLOY_DIR:          $DEPLOY_DIR
echo =================================================

deploydir=$DEPLOY_DIR

inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR

thedate=`date --date="1 day ago" +%Y%m%d`; 
echo $thedate

# TODO: check for existence otherwise the shell will complain
echo hadoop fs -rmr $outputdir/$thedate/aggvip
hadoop fs -rmr $outputdir/$thedate/aggvip

echo pig -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$thedate -f $DEPLOY_DIR/aggVip.pig
pig -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$thedate -param deploydir=$deploydir -f $DEPLOY_DIR/pig/aggVip.pig


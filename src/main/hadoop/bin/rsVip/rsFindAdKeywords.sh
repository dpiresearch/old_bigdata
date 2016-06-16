#!/bin/bash
#
# Calculate the keyword counts for the past X days
# Usually 30 days
#

DAYS_AGO=1
INPUT_DIR=/user/data/bolt/data/qa/input
OUTPUT_DIR=/user/data/bolt/data/qa/daily/popular_search
DEPLOY_DIR=~
NUM_DAYS_BACK=30

while getopts "i:o:d:a:b:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                b) NUM_DAYS_BACK=$OPTARG;;
        esac
done

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== OUTPUT_DIR:          $OUTPUT_DIR
echo ===== DEPLOY_DIR:          $DEPLOY_DIR
echo ===== NUM_DAYS_BACK:       $NUM_DAYS_BACK
echo =================================================

# Set how many days ago you want to calculate from today
daysago=$DAYS_AGO

# Base directory where everything is read or stored
inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR

# Base deployo dir for all pig scripts
deploydir=$DEPLOY_DIR

# Calculate for NUM_DAYS_BACK days
for ((var=0;var<$NUM_DAYS_BACK;++var));
do
  setday=$((var + $daysago))
  thedate=`date --date="$setday day ago" +%Y%m%d`;
  echo $thedate

  # TODO - check for existence, otherwise it'll throw an error
  echo hadoop fs -rmr $outputdir/$thedate/rs_kwad 
  hadoop fs -rmr $outputdir/$thedate/rs_kwad 

  echo pig -param deploydir=$deploydir -param inputdir=$inputdir -param outputdir=$outputdir -param daydate=$thedate -f $deploydir/rsVip/rsFindAdKeywords.pig
  pig -param deploydir=$deploydir -param inputdir=$inputdir -param outputdir=$outputdir -param daydate=$thedate -f $deploydir/pig/rsVip/rsFindAdKeywords.pig

done


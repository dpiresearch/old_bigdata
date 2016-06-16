#!/bin/bash
#
# Calculate the decay for a set of keywords and adids based on the past 90 days
#
# Need input for offset
# Need to create paths to store intermediate calculations
# Call another pig script to perform summation
#

DAYS_AGO=1
INPUT_DIR=/user/data/bolt/data/qa/input/viewad
OUTPUT_DIR=/user/data/bolt/data/qa/daily/bestads
DEPLOY_DIR=~/pig
NUM_DAYS_BACK=7
REF_DIR=/user/data/bolt/data/ref

while getopts "i:o:d:a:b:r:" option
do
        case "${option}" in

                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                b) NUM_DAYS_BACK=${OPTARG};;
                r) REF_DIR=${OPTARG};;
        esac
done

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== OUTPUT_DIR:          $OUTPUT_DIR
echo ===== DEPLOY_DIR:          $DEPLOY_DIR
echo ===== REF_DIR:             $REF_DIR
echo ===== NUM_DAYS_BACK:       $NUM_DAYS_BACK
echo =================================================


# How many days ago will be day zero of the decay calculation
daysago=$DAYS_AGO

# deploy dir
deploydir=$DEPLOY_DIR

# We're supposed to do this for 30 days
refdir=$REF_DIR

inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR

# TODO Check for empty output dir
currentdate=`date --date="$daysago day ago" +%Y%m%d`
echo Job Run Date:$currentdate
echo Deleting output directory...
echo hadoop fs -rm -r -skipTrash $outputdir/$currentdate
hadoop fs -rm -r -skipTrash $outputdir/$currentdate

# Calculate dates(input folder) against which the pig script needs to run.
alldate="{$currentdate"

# We're supposed to do this for 30 days
for ((var=1;var<$NUM_DAYS_BACK;++var));
do
  setday=$((var+$daysago))
  thedate=`date --date="$setday day ago" +%Y%m%d`;

  alldate="$alldate,$thedate";

  # TODO: check for existence otherwise the shell will complain
done

alldate="$alldate}"
echo $alldate

echo pig -param alldate="$alldate" -param refdir=$refdir -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$currentdate -f $deploydir/pig/bestAds/bestAds.pig
pig -param alldate="$alldate" -param refdir=$refdir -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$currentdate -f $deploydir/pig/bestAds/bestAds.pig

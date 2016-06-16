#!/bin/bash
#
# Used to copy the qa sv_filtered count ninety days back for related search for vip testing
# This is hard coded and only exists as an example
#

DAYS_AGO=1
NUM_DAYS_BACK=5

while getopts "a:b:" option
do
        case "${option}" in

                a) DAYS_AGO=${OPTARG};;
                b) NUM_DAYS_BACK=$OPTARG;;
        esac
done

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== NUM_DAYS_BACK:       $NUM_DAYS_BACK
echo =================================================

daysago=$DAYS_AGO

#today=`date +"%Y%m%d"`;
today=`date --date="$daysago day ago" +%Y%m%d`;
echo Today:$today;
processDate=$today;

#for var in {0..90}
for ((var=1;var<$NUM_DAYS_BACK;++var));
do
  setday=$((var+$daysago)) 
  thedate=`date --date="$setday day ago" +%Y%m%d`; 
  echo $thedate
  hadoop fs -rmr /user/data/bolt/data/qa/tests/data/popular_search/$thedate/rs_kwad
  hadoop fs -mkdir /user/data/bolt/data/qa/tests/data/popular_search/$thedate/rs_kwad
  
  echo "Copying $processDate data to $thedate"
  hadoop fs -cp /user/data/bolt/data/qa/tests/data/popular_search/$processDate/rs_kwad /user/data/bolt/data/qa/tests/data/popular_search/$thedate/
done


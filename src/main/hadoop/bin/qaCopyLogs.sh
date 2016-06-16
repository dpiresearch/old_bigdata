#!/bin/bash
#
# Used to copy the qa search logs thirty days back.
# This is hard coded and only exists as an example.
# Use this as a reference if you want to copy logs for testing
#
#

today=`date +"%Y%m%d"`;
echo Today:$today;
processDate=$today;

for var in {0..30}
do
  setday=$((var+1)) 
  thedate=`date --date="$setday day ago" +%Y%m%d`; 
  echo $thedate
  
  # Be careful here, you're removing stuff
  hadoop fs -rmr /user/data/bolt/data/qa/tests/data/search_results/$thedate

  hadoop fs -mkdir /user/data/bolt/data/qa/tests/data/search_results/$thedate
  hadoop fs -cp /user/data/bolt/data/qa/tests/data/search_results/$processDate/* /user/data/bolt/data/qa/tests/data/search_results/$thedate
done


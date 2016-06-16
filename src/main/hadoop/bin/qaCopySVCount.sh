#!/bin/bash
#
# Used to copy the qa searchview count thirty days back.
# This is hard coded and only exists as an example
#

today=`date +"%Y%m%d"`;
echo Today:$today;
processDate=$today;

for var in {0..30}
do
  setday=$((var+1)) 
  thedate=`date --date="$setday day ago" +%Y%m%d`; 
  echo $thedate
  hadoop fs -rmr /user/data/bolt/data/qa/tests/data/popular_search/$thedate/searchview_kwcount
  hadoop fs -mkdir /user/data/bolt/data/qa/tests/data/popular_search/$thedate/searchview_kwcount
  hadoop fs -cp /user/data/bolt/data/qa/tests/data/popular_search/$processDate/searchview_kwcount /user/data/bolt/data/qa/tests/data/popular_search/$thedate/
done


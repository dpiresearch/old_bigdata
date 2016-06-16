#!/bin/bash
#
# Kick off the script to calculate an aggregate of all keywords over the past 7 days
# dapang@hadname001:~/bin$ pig --param refdir=/user/dapang/data/prod/ref --param thedate=20130915 --param deploydir=/me
#
# /home/dapang/current --param inputdir=/user/dapang/data/prod/popular_search -f asv.pig
#

# How many days ago will be day zero of the decay calculation
daysago=1

# deploy dir
deploydir=/home/hadoop/current

refdir=/user/hadoop/data/prod/ref
rawinputdir=/user/hadoop/data/prod/ingest/search_results
inputdir=/user/hadoop/data/prod/jobs/popular_search
outputdir=/user/hadoop/data/exp

displaydate=`date --date="$daysago day ago" +%m-%d-%y`
currentdate=`date --date="$daysago day ago" +%Y%m%d`
alldate="{$currentdate"

# We're supposed to do this for 7 days
for var in {0..7}
do
  setday=$((var+$daysago))
  thedate=`date --date="$setday day ago" +%Y%m%d`;


  alldate="$alldate,$thedate";

  # TODO: check for existence otherwise the shell will complain

done

alldate="$alldate}"
echo $alldate

# Remove the previous results
# /user/data/bolt/data/popular_search/20130618/all_search_words

# disable removing for now to avoid accidentally removing directories
# echo hadoop fs -rmr $outputdir/$currentdate/asw
#hadoop fs -rmr $outputdir/$currentdate/asw

# echo pig -param refdir=$refdir -param deploydir=$deploydir -param daydate=$currentdate -param alldate="$alldate" -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$currentdate -f $deploydir/pig/asw.pig
#pig -param refdir=$refdir -param deploydir=$deploydir -param daydate=$currentdate -param alldate="$alldate" -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$currentdate -f $deploydir/pig/asw.pig

echo pig -param refdir=$refdir -param deploydir=$deploydir -param daydate=$currentdate -param alldate="$alldate" -param inputdir=$rawinputdir -param outputdir=$outputdir -param thedate=$currentdate -f $deploydir/pig/reports/asw1.pig
pig -param refdir=$refdir -param deploydir=$deploydir -param daydate=$currentdate -param alldate="$alldate" -param inputdir=$rawinputdir -param outputdir=$outputdir -param thedate=$currentdate -f $deploydir/pig/reports/asw1.pig


# disable removing for now to avoid accidentally removing directories
# echo hadoop fs -rmr $outputdir/$currentdate/asv
#hadoop fs -rmr $outputdir/$currentdate/asv

#echo pig -param refdir=$refdir -param deploydir=$deploydir -param daydate=$currentdate -param alldate="$alldate" -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$currentdate -f $deploydir/pig/asv.pig
#pig -param refdir=$refdir -param deploydir=$deploydir -param daydate=$currentdate -param alldate="$alldate" -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$currentdate -f $deploydir/pig/asv.pig


hadoop fs -cat $outputdir/$currentdate/asw/part* > /tmp/searchReport$currentdate.csv


cd /tmp
gzip searchReport$currentdate.csv

scp searchReport$currentdate.csv.gz staticweb001:/var/www/SR 
scp searchReport$currentdate.csv.gz staticweb002:/var/www/SR 

echo "Search World Report for South Africa" > /tmp/searchWord.msg
echo " " > /tmp/searchWorld.msg
echo "File can be downloaded at" >> /tmp/searchWord.msg
echo " " > /tmp/searchWorld.msg
echo "http://inc.t9.classistatic.com/SR/searchReport$currentdate.csv.gz" >> /tmp/searchWord.msg

cd -


mutt -s "South Africa search word report for $displaydate" dapang@ebay.com sseamon@ebay.com tkudla@ebay.com anpark@ebay.com lsvieta@ebay.com olemmers@ebay.com < /tmp/searchWord.msg
#mutt -s "South Africa search word report for $displaydate" dapang@ebay.com < /tmp/searchWord.msg




#!/bin/bash
#
#
#

# set defaults
DAYS_AGO=7
INPUT_DIR=/user/dapang/data/prod/save_ad
OUTPUT_DIR=/user/dapang/data/prod/report/savead_hist
DEPLOY_DIR=/media/home/dapang/current
DAYS_RANGE=7
MESSAGE_FILE=/home/hadoop/report/save_ad/saveAdHist.msg
EMAIL_LIST=grangaswamy@ebay.com

while getopts "i:o:d:e:a:b:" option
do
        case "${option}" in
                i) INPUT_DIR=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                d) DEPLOY_DIR=${OPTARG};;
                e) EMAIL_LIST=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                b) DAYS_RANGE=$OPTARG;;
        esac
done

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== DAYS_RANGE:          $DAYS_RANGE
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== OUTPUT_DIR:          $OUTPUT_DIR
echo ===== DEPLOY_DIR:          $DEPLOY_DIR
echo ===== EMAIL_LIST:          $EMAIL_LIST
echo =================================================

messagefile=$MESSAGE_FILE

daysago=$DAYS_AGO

# deploy dir
deploydir=$DEPLOY_DIR
echo Deploy Dir:$deploydir

# input and output directories
inputdir=$INPUT_DIR
outputdir=$OUTPUT_DIR

currentdate=`date --date="$daysago day ago" +%Y%m%d`
echo $currentdate

alldate="{$currentdate"
# alldate="{"

# We're supposed to do this for 30 days
for ((var=1;var<$DAYS_RANGE;var++));
do
  setday=$(($daysago-$var))
  thedate=`date --date="$setday day ago" +%Y%m%d`;
  alldate="$alldate,$thedate";

  # TODO: check for existence otherwise the shell will complain
done

alldate="$alldate}"
echo $alldate

# Remove the previous results
echo hadoop fs -rmr $outputdir/$currentdate
hadoop fs -rmr $outputdir/$currentdate

# Execute the script
echo pig -param deploydir=$deploydir -param alldate="$alldate" -param refdir=/user/dapang/data/prod/ref -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$currentdate -f /media/home/dapang/current/pig/reports/savead_pending_e.pig
pig -param deploydir=$deploydir -param daterange="$alldate" -param refdir=/user/dapang/data/prod/ref -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$currentdate -f /media/home/dapang/current/pig/reports/savead_pending_e.pig

echo pig -param deploydir=$deploydir -param alldate="$alldate" -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$currentdate -f /media/home/dapang/current/pig/reports/savead_completed.pig
pig -param deploydir=$deploydir -param daterange="$alldate" -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$currentdate -f /media/home/dapang/current/pig/reports/savead_completed.pig

echo pig -param deploydir=$deploydir -param daterange="$alldate" -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$currentdate -f /media/home/dapang/current/pig/reports/savead_postsperdraft.pig
pig -param deploydir=$deploydir -param daterange="$alldate" -param inputdir=$inputdir -param outputdir=$outputdir -param thedate=$currentdate -f $deploydir/pig/reports/savead_postsperdraft.pig


# Email message
cat $messagefile > /tmp/draft_report.msg

post_clk_count=`hadoop fs -cat $outputdir/$currentdate/post_clk/part* | awk -F"," '{ sum += $2 } END { print sum }'`
echo PostAd Clicks :$post_clk_count >> /tmp/draft_report.msg

created_count=`hadoop fs -cat $outputdir/$currentdate/created/part* | wc -l`
echo Drafts Created:$created_count >> /tmp/draft_report.msg

completed_count=`hadoop fs -cat $outputdir/$currentdate/completed/part* | wc -l`
echo Drafts Posted :$completed_count >> /tmp/draft_report.msg

pending_count=`hadoop fs -cat $outputdir/$currentdate/pending/part* | wc -l`
echo Drafts Pending:$pending_count >> /tmp/draft_report.msg

# Generate csv attachment files
hadoop fs -cat $outputdir/$currentdate/pending/part* > /tmp/pending_$currentdate.csv
hadoop fs -cat $outputdir/$currentdate/comp_time/part* > /tmp/completed_$currentdate.csv
hadoop fs -cat $outputdir/$currentdate/draft_ua/part* > /tmp/draft_ua_$currentdate.txt
hadoop fs -cat $outputdir/$currentdate/draft_posts/part* > /tmp/draft_posts_$currentdate.csv
hadoop fs -cat $outputdir/$currentdate/pending_emails/part* > /tmp/pending_emails_$currentdate.csv

# Email Report
echo mutt -s "Save Ad report [PHX] for $currentdate" -a /tmp/draft_posts_$currentdate.csv -a /tmp/pending_$currentdate.csv -a /tmp/pending_emails_$currentdate.csv -a /tmp/completed_$currentdate.csv -a /tmp/draft_ua_$currentdate.txt -- $EMAIL_LIST  < /tmp/draft_report.msg
mutt -s "Save Ad report [PHX] for $currentdate" -a /tmp/draft_posts_$currentdate.csv -a /tmp/pending_$currentdate.csv -a /tmp/pending_emails_$currentdate.csv -a /tmp/completed_$currentdate.csv -a /tmp/draft_ua_$currentdate.txt -- $EMAIL_LIST  < /tmp/draft_report.msg
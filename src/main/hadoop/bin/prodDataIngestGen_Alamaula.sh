#!/bin/bash

DAYS_AGO=1

today=`date +"%Y%m%d"`;
echo Today:$today;
ydate=$(date --date yesterday "+%Y%m%d" );
yday=$(date --date yesterday "+%a" )

# now the date can be configured by DAYS_AGO
ydate=`date --date="$DAYS_AGO day ago" +%Y%m%d`
echo Y: $ydate;
yday=`date --date="$DAYS_AGO day ago" +%a`
echo Yesterday Day: $yday;

processDate=$ydate;
processDay=$yday

# TODO Make this configurable
tomcatBaseDir=/var/log/tomcat7multi/frontend;
srFileName=search_results.log;
vFileName=viewed_ad.log;

# TODO Make this configurable
hdSrBaseDir=/user/hadoop/am/prod/ingest/search_results
hdVAdBaseDir=/user/hadoop/am/prod/ingest/viewad;

# ========================================================================================================
# Handle search_results files....
echo Handle search_results files....

# Cleanup Folder on Hadoop if needed
#hadoop fs -rm -r -skipTrash $hdSrBaseDir/$processDate

echo Creating folders in hadoop..
echo hadoop fs -mkdir $hdSrBaseDir/$processDate
hadoop fs -mkdir $hdSrBaseDir/$processDate

SR_DIR=/home/hadoop/alamaula/search_results/$processDate

# Compress and store
if [ ! -d "$SR_DIR" ]; then
  echo "Doing compression on a nonexistent file, exiting"
  exit
fi


echo cd /home/hadoop/alamaula/search_results/$processDate
cd /home/hadoop/alamaula/search_results/$processDate

for i in `ls`; 
do
  echo $i
  if [ -d $i ]; then
    cd $i
    bunzip2 *.log.tar.bz2
    tar xvf *.log.tar
    cd ..
  fi
done

# Just in case, so we don't do any damage
cd /home/hadoop/alamaula/search_results/$processDate

cat */*.log > search_results.log
bzip2 search_results.log
mkdir all
mv search_results.log.bz2 ./all

hadoop fs -put all $hdSrBaseDir/$processDate

hadoop fs -chmod -R 777 $hdSrBaseDir/*

# ========================================================================================================
# Handle viewed_ad files........
echo Handle viewed_ad files........

# Cleanup Folder on Hadoop if needed
#hadoop fs -rm -r -skipTrash $hdVAdBaseDir/$processDate

echo Creating folders in hadoop..
echo hadoop fs -mkdir $hdVAdBaseDir/$processDate
hadoop fs -mkdir $hdVAdBaseDir/$processDate

VA_DIR=/home/hadoop/alamaula/viewad/$processDate

# Compress and store
if [ ! -d "$VA_DIR" ]; then
  echo "Doing compression on a nonexistent file, exiting"
  exit
fi

echo cd /home/hadoop/alamaula/viewad/$processDate
cd /home/hadoop/alamaula/viewad/$processDate

for i in `ls`; 
do
  echo $i
  if [ -d $i ]; then
    cd $i
    bunzip2 *.log.tar.bz2
    tar xvf *.log.tar
    cd ..
  fi
done

# Just in case, so we don't do any damage
cd /home/hadoop/alamaula/viewad/$processDate

cat */*.log > viewed_ad.log
bzip2 viewed_ad.log
mkdir all
mv viewed_ad.log.bz2 ./all

hadoop fs -put all $hdVAdBaseDir/$processDate
hadoop fs -chmod -R 777 $hdVAdBaseDir/*

#========================================================================================================


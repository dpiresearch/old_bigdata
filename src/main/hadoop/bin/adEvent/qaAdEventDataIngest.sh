#!/bin/bash
#
# Script to move ad event data from QA app servers (main pool & tns pool) 
# into HDFS (QA Cluster) 
#

OUTPUT_DIR=/user/data/bolt/data/qa/tests/data/

while getopts "o:" option
do
        case "${option}" in
                o) OUTPUT_DIR=${OPTARG};;
        esac
done

today=`date +"%Y%m%d"`;
echo Today:$today;
processDate=$today;

tomcatBaseDir=/data/bolt/b1rt/tomcat/logs;
hdAdEvtBaseDir=$OUTPUT_DIR/ad_events;
aeFileName=ad_event.log

stgBaseDir=~/data/qatest/;

# --------------------------------------------------------------------------

echo Creating Dir: $stgBaseDir
mkdir -p $stgBaseDir;
echo Creating Dir: $stgBaseDir/$processDate
mkdir -p $stgBaseDir/$processDate;
echo Creating Dir: $stgBaseDir/$processDate/1
mkdir -p $stgBaseDir/$processDate/1;
echo Creating Dir: $stgBaseDir/$processDate/2
mkdir -p $stgBaseDir/$processDate/2;
echo Creating Dir: $stgBaseDir/$processDate/31
mkdir -p $stgBaseDir/$processDate/31;
echo Creating Dir: $stgBaseDir/$processDate/32
mkdir -p $stgBaseDir/$processDate/32;

echo =======================================================================

echo Handle ad_event.log files.......
echo Copying QA Data over to temp dir ....
echo scp bo-qamain001:$tomcatBaseDir/$srFileName $stgBaseDir/$processDate/11/
scp bo-qamain001:$tomcatBaseDir/$aeFileName $stgBaseDir/$processDate/11/
echo scp bo-qamain002:$tomcatBaseDir/$srFileName $stgBaseDir/$processDate/12/
scp bo-qamain002:$tomcatBaseDir/$aeFileName $stgBaseDir/$processDate/12/
echo scp bo-qatns001:$tomcatBaseDir/$srFileName $stgBaseDir/$processDate/31/
scp bo-qatns001:$tomcatBaseDir/$aeFileName $stgBaseDir/$processDate/31/
echo scp bo-qatns002:$tomcatBaseDir/$srFileName $stgBaseDir/$processDate/32/
scp bo-qatns002:$tomcatBaseDir/$aeFileName $stgBaseDir/$processDate/32/

echo Deleting output directory on HDFS ...
echo hadoop fs -rm -r -skipTrash $hdAdEvtBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdAdEvtBaseDir/$processDate

echo Creating folders in hadoop..
echo hadoop fs -mkdir $hdAdEvtBaseDir/$processDate
hadoop fs -mkdir $hdAdEvtBaseDir/$processDate
echo hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/11
hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/11
echo hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/12
hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/12
echo hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/31
hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/31
echo hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/32
hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/32

echo Copying files to HDFS ...
echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$aeFileName $hdAdEvtBaseDir/$processDate/11/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$aeFileName $hdAdEvtBaseDir/$processDate/11/
echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$aeFileName $hdAdEvtBaseDir/$processDate/12/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$aeFileName $hdAdEvtBaseDir/$processDate/12/
echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$aeFileName $hdAdEvtBaseDir/$processDate/31/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$aeFileName $hdAdEvtBaseDir/$processDate/31/
echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$aeFileName $hdAdEvtBaseDir/$processDate/32/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$aeFileName $hdAdEvtBaseDir/$processDate/32/

hadoop fs -chmod 777 $hdAdEvtBaseDir/$processDate
hadoop fs -chmod 777 $hdAdEvtBaseDir/$processDate/*
hadoop fs -chmod 777 $hdAdEvtBaseDir/$processDate/*/*

echo =======================================================================


# Cleanup temp folder and files
echo Deleting Dir: $stgBaseDir
rm -rf $stgBaseDir;


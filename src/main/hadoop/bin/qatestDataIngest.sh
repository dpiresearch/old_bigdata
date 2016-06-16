#!/bin/bash
#
# Script to move data from QA main pool servers 1 & 2 into QA Cluster
# Version 2: Added client_events.log, ingestion output dir configurable
#
#

OUTPUT_DIR=/user/data/bolt/data/qa/tests/data/

while getopts "o:" option
do
        case "${option}" in
                o) OUTPUT_DIR=${OPTARG};;
        esac
done

hdSrBaseDir=$OUTPUT_DIR/search_results
hdSBaseDir=$OUTPUT_DIR/search_page
hdVAdBaseDir=$OUTPUT_DIR/viewad;
hdEvtBaseDir=$OUTPUT_DIR/events;

today=`date +"%Y%m%d"`;
echo Today:$today;
processDate=$today;

tomcatBaseDir=/data/bolt/b1rt/tomcat/logs;
srFileName=search_results.log;
sFileName=search_page.log;
vFileName=viewed_ad.log;
eFileName=client_events.log;

stgBaseDir=~/data/qatest/;
# -------------------------------------------------------------------
echo Creating Dir: $stgBaseDir
mkdir -p $stgBaseDir;
echo Creating Dir: $stgBaseDir/$processDate
mkdir -p $stgBaseDir/$processDate;
echo Creating Dir: $stgBaseDir/$processDate/1
mkdir -p $stgBaseDir/$processDate/1;
echo Creating Dir: $stgBaseDir/$processDate/2
mkdir -p $stgBaseDir/$processDate/2;

echo =====================================================================================================
echo Handle search_results.log files.......
echo Copying QA Data over....
echo scp bo-qamain001:$tomcatBaseDir/$srFileName $stgBaseDir/$processDate/1/
scp bo-qamain001:$tomcatBaseDir/$srFileName $stgBaseDir/$processDate/1/
echo scp bo-qamain002:$tomcatBaseDir/$srFileName $stgBaseDir/$processDate/2/
scp bo-qamain002:$tomcatBaseDir/$srFileName $stgBaseDir/$processDate/2/

echo Deleting output directory...
echo hadoop fs -rm -r -skipTrash $hdSrBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdSrBaseDir/$processDate


echo Creating folders in hadoop..
echo hadoop fs -mkdir $hdSrBaseDir/$processDate
hadoop fs -mkdir $hdSrBaseDir/$processDate
echo hadoop fs -mkdir $hdSrBaseDir/$processDate/1
hadoop fs -mkdir $hdSrBaseDir/$processDate/1
echo hadoop fs -mkdir $hdSrBaseDir/$processDate/2
hadoop fs -mkdir $hdSrBaseDir/$processDate/2

echo Copying files to hadoop..
echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$srFileName $hdSrBaseDir/$processDate/1/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$srFileName $hdSrBaseDir/$processDate/1/
echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$srFileName $hdSrBaseDir/$processDate/2/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$srFileName $hdSrBaseDir/$processDate/2/

hadoop fs -chmod 777 $hdSrBaseDir/$processDate
hadoop fs -chmod 777 $hdSrBaseDir/$processDate/*
hadoop fs -chmod 777 $hdSrBaseDir/$processDate/*/*

echo ========================================================================================================
echo Handle search_page.log files.......

echo Copying QA Data over....
echo scp bo-qamain001:$tomcatBaseDir/$sFileName $stgBaseDir/$processDate/1/
scp bo-qamain001:$tomcatBaseDir/$sFileName $stgBaseDir/$processDate/1/
echo scp bo-qamain002:$tomcatBaseDir/$sFileName $stgBaseDir/$processDate/2/
scp bo-qamain002:$tomcatBaseDir/$sFileName $stgBaseDir/$processDate/2/

# Cleanup Folder on Hadoop if needed
echo Deleting output directory...
echo hadoop fs -rm -r -skipTrash $hdSBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdSBaseDir/$processDate

echo Creating folders in hadoop..
echo hadoop fs -mkdir $hdSBaseDir/$processDate
hadoop fs -mkdir $hdSBaseDir/$processDate
echo hadoop fs -mkdir $hdSBaseDir/$processDate/1
hadoop fs -mkdir $hdSBaseDir/$processDate/1
echo hadoop fs -mkdir $hdSBaseDir/$processDate/2
hadoop fs -mkdir $hdSBaseDir/$processDate/2

echo Copying files to hadoop..
echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$sFileName $hdSBaseDir/$processDate/1/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$sFileName $hdSBaseDir/$processDate/1/
echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$sFileName $hdSBaseDir/$processDate/2/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$sFileName $hdSBaseDir/$processDate/2/

hadoop fs -chmod 777 $hdSBaseDir/$processDate
hadoop fs -chmod 777 $hdSBaseDir/$processDate/*
hadoop fs -chmod 777 $hdSBaseDir/$processDate/*/*

echo ========================================================================================================;
echo Handle viewed_ad.log files........
echo Copying QA Data over....
scp bo-qamain001:$tomcatBaseDir/$vFileName $stgBaseDir/$processDate/1/
scp bo-qamain002:$tomcatBaseDir/$vFileName $stgBaseDir/$processDate/2/

echo Copying files to QA NameNode..

# Cleanup Folder on Hadoop if needed
echo Deleting output directory...
echo hadoop fs -rm -r -skipTrash $hdVAdBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdVAdBaseDir/$processDate

echo Creating folders in hadoop..
echo hadoop fs -mkdir $hdVAdBaseDir/$processDate
hadoop fs -mkdir $hdVAdBaseDir/$processDate
echo hadoop fs -mkdir $hdVAdBaseDir/$processDate/1
hadoop fs -mkdir $hdVAdBaseDir/$processDate/1
echo hadoop fs -mkdir $hdVAdBaseDir/$processDate/2
hadoop fs -mkdir $hdVAdBaseDir/$processDate/2

echo Copying files to hadoop..
echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$vFileName $hdVAdBaseDir/$processDate/1/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$vFileName $hdVAdBaseDir/$processDate/1/
echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$vFileName $hdVAdBaseDir/$processDate/2/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$vFileName $hdVAdBaseDir/$processDate/2/

hadoop fs -chmod 777 $hdVAdBaseDir/$processDate
hadoop fs -chmod 777 $hdVAdBaseDir/$processDate/*
hadoop fs -chmod 777 $hdVAdBaseDir/$processDate/*/*

echo ========================================================================================================
echo Handle client_events.log files........
echo Copying QA Data over....
scp bo-qamain001:$tomcatBaseDir/$eFileName $stgBaseDir/$processDate/1/
scp bo-qamain002:$tomcatBaseDir/$eFileName $stgBaseDir/$processDate/2/

echo Copying files to QA NameNode..

# Cleanup Folder on Hadoop if needed
echo Deleting output directory...
echo hadoop fs -rm -r -skipTrash $hdEvtBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdEvtBaseDir/$processDate

echo Creating folders in hadoop..
echo hadoop fs -mkdir $hdEvtBaseDir/$processDate
hadoop fs -mkdir $hdEvtBaseDir/$processDate
echo hadoop fs -mkdir $hdEvtBaseDir/$processDate/1
hadoop fs -mkdir $hdEvtBaseDir/$processDate/1
echo hadoop fs -mkdir $hdEvtBaseDir/$processDate/2
hadoop fs -mkdir $hdEvtBaseDir/$processDate/2

echo Copying files to hadoop..
echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$eFileName $hdEvtBaseDir/$processDate/1/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$eFileName $hdEvtBaseDir/$processDate/1/
echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$eFileName $hdEvtBaseDir/$processDate/2/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$eFileName $hdEvtBaseDir/$processDate/2/

hadoop fs -chmod 777 $hdEvtBaseDir/$processDate
hadoop fs -chmod 777 $hdEvtBaseDir/$processDate/*
hadoop fs -chmod 777 $hdEvtBaseDir/$processDate/*/*

#========================================================================================================

# Cleanup temp folder and files
echo Deleting Dir: $stgBaseDir
rm -rf $stgBaseDir;


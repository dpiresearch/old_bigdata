#!/bin/bash
#
# Script to move data from QA main pool servers 1 & 2 into QA Cluster
#
#

today=`date +"%Y%m%d"`;
echo Today:$today;
processDate=$today;

tomcatBaseDir=/data/bolt/b1rt/tomcat/logs;
srFileName=search_results.log;
sFileName=search_page.log;
vFileName=viewed_ad.log;
stgBaseDir=~/data/qatest/;
nnBaseDir=~/data/qatest;

# TODO make this configurable
#hdSrBaseDir=/user/data/bolt/data/qatest/input/search_results
#hdSBaseDir=/user/data/bolt/data/qatest/input/search_page
#hdVAdBaseDir=/user/data/bolt/data/qatest/input/viewad;
hdSrBaseDir=/user/data/bolt/data/qa/tests/data/search_results
hdSBaseDir=/user/data/bolt/data/qa/tests/data/search_page
hdVAdBaseDir=/user/data/bolt/data/qa/tests/data/viewad;

hdSrvName=bo-qanamenode001

# -------------------------------------------------------------------


# Handle search_results files....
echo Creating Dir: $stgBaseDir
mkdir $stgBaseDir;
echo Creating Dir: $stgBaseDir/$processDate
mkdir $stgBaseDir/$processDate;
echo Creating Dir: $stgBaseDir/$processDate/1
mkdir $stgBaseDir/$processDate/1;
echo Creating Dir: $stgBaseDir/$processDate/2
mkdir $stgBaseDir/$processDate/2;

#echo Creating Dir on $hdSrvName $nnBaseDir
#ssh $hdSrvName mkdir $nnBaseDir;
#echo Creating Dir on $hdSrvName $nnBaseDir/$processDate
#ssh $hdSrvName mkdir $nnBaseDir/$processDate;
#echo Creating Dir on $hdSrvName $nnBaseDir/$processDate/1
#ssh $hdSrvName mkdir $nnBaseDir/$processDate/1;
#echo Creating Dir on $hdSrvName $nnBaseDir/$processDate/2
#ssh $hdSrvName mkdir $nnBaseDir/$processDate/2;

# =====================================================================================================
# Handle search_results files.......

echo Copying QA Data over....
scp bo-qamain001:$tomcatBaseDir/$srFileName $stgBaseDir/$processDate/1/
scp bo-qamain002:$tomcatBaseDir/$srFileName $stgBaseDir/$processDate/2/

#echo Copying files to QA NameNode..
#scp $stgBaseDir/$processDate/1/$srFileName  $hdSrvName:$nnBaseDir/$processDate/1/
#scp $stgBaseDir/$processDate/2/$srFileName  $hdSrvName:$nnBaseDir/$processDate/2/

# Cleanup Folder on Hadoop if needed
#ssh $hdSrvName hadoop fs -rm -r -skipTrash $hdSrBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdSrBaseDir/$processDate

echo Creating folders in hadoop..
echo hadoop fs -mkdir $hdSrBaseDir/$processDate
#ssh $hdSrvName hadoop fs -mkdir $hdSrBaseDir/$processDate
hadoop fs -mkdir $hdSrBaseDir/$processDate
echo hadoop fs -mkdir $hdSrBaseDir/$processDate/1
#ssh $hdSrvName hadoop fs -mkdir $hdSrBaseDir/$processDate/1
hadoop fs -mkdir $hdSrBaseDir/$processDate/1
echo hadoop fs -mkdir $hdSrBaseDir/$processDate/2
#ssh $hdSrvName hadoop fs -mkdir $hdSrBaseDir/$processDate/2
hadoop fs -mkdir $hdSrBaseDir/$processDate/2

echo Copying files to hadoop..
echo hadoop fs -copyFromLocal $nnBaseDir/$processDate/1/$srFileName $hdSrBaseDir/$processDate/1/
#ssh $hdSrvName hadoop fs -copyFromLocal $nnBaseDir/$processDate/1/$srFileName $hdSrBaseDir/$processDate/1/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$srFileName $hdSrBaseDir/$processDate/1/
echo hadoop fs -copyFromLocal $nnBaseDir/$processDate/1/$srFileName $hdSrBaseDir/$processDate/2/
#ssh $hdSrvName hadoop fs -copyFromLocal $nnBaseDir/$processDate/2/$srFileName $hdSrBaseDir/$processDate/2/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$srFileName $hdSrBaseDir/$processDate/2/

#ssh $hdSrvName hadoop fs -chmod 777 $hdSrBaseDir/*
hadoop fs -chmod 777 $hdSrBaseDir/*
#ssh $hdSrvName hadoop fs -chmod 777 $hdSrBaseDir/*/*
hadoop fs -chmod 777 $hdSrBaseDir/*/*
#ssh $hdSrvName hadoop fs -chmod 777 $hdSrBaseDir/*/*/*
hadoop fs -chmod 777 $hdSrBaseDir/*/*/*

# ========================================================================================================

# Hadle viewed_ad files........
echo Copying QA Data over....
scp bo-qamain001:$tomcatBaseDir/$vFileName $stgBaseDir/$processDate/1/
scp bo-qamain002:$tomcatBaseDir/$vFileName $stgBaseDir/$processDate/2/

echo Copying files to QA NameNode..
#scp $stgBaseDir/$processDate/1/$vFileName  $hdSrvName:$nnBaseDir/$processDate/1/
#scp $stgBaseDir/$processDate/2/$vFileName  $hdSrvName:$nnBaseDir/$processDate/2/

# Cleanup Folder on Hadoop if needed
#ssh $hdSrvName hadoop fs -rm -r -skipTrash $hdVAdBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdVAdBaseDir/$processDate

echo Creating folders in hadoop..
echo hadoop fs -mkdir $hdVAdBaseDir/$processDate
#ssh $hdSrvName hadoop fs -mkdir $hdVAdBaseDir/$processDate
hadoop fs -mkdir $hdVAdBaseDir/$processDate
echo hadoop fs -mkdir $hdVAdBaseDir/$processDate/1
#ssh $hdSrvName hadoop fs -mkdir $hdVAdBaseDir/$processDate/1
hadoop fs -mkdir $hdVAdBaseDir/$processDate/1
echo hadoop fs -mkdir $hdVAdBaseDir/$processDate/2
#ssh $hdSrvName hadoop fs -mkdir $hdVAdBaseDir/$processDate/2
hadoop fs -mkdir $hdVAdBaseDir/$processDate/2

echo Copying files to hadoop..
echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$vFileName $hdVAdBaseDir/$processDate/1/
#ssh $hdSrvName hadoop fs -copyFromLocal $nnBaseDir/$processDate/1/$vFileName $hdVAdBaseDir/$processDate/1/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$vFileName $hdVAdBaseDir/$processDate/1/
echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$vFileName $hdVAdBaseDir/$processDate/2/
#ssh $hdSrvName hadoop fs -copyFromLocal $nnBaseDir/$processDate/2/$vFileName $hdVAdBaseDir/$processDate/2/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$vFileName $hdVAdBaseDir/$processDate/2/

#ssh $hdSrvName hadoop fs -chmod 777 $hdVAdBaseDir/*
hadoop fs -chmod 777 $hdVAdBaseDir/*
#ssh $hdSrvName hadoop fs -chmod 777 $hdVAdBaseDir/*/*
hadoop fs -chmod 777 $hdVAdBaseDir/*/*
#ssh $hdSrvName hadoop fs -chmod 777 $hdVAdBaseDir/*/*/*
hadoop fs -chmod 777 $hdVAdBaseDir/*/*/*
#========================================================================================================

# Cleanup temp folder and files
echo Deleting Dir: $stgBaseDir
rm -rf $stgBaseDir;

#ssh $hdSrvName rm -rf $nnBaseDir
#ssh $hdSrvName rm -rf $nnBaseDir
echo Deleting Dir: $nnBaseDir on $hdSrvName

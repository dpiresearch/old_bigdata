#!/bin/bash

today=`date +"%Y%m%d"`;
echo Today:$today;
ydate=$(date --date yesterday "+%Y%m%d" );
#ydate=20130811;
echo Y: $ydate;
yday=$(date --date yesterday "+%a" )
#yday="Sat";
echo Yesterday Day: $yday;

processDate=$ydate;
processDay=$yday

# TODO Make this configurable
tomcatBaseDir=/var/log/tomcat7multi/frontend;
srFileName=search_results.log;
sFileName=search_page.log;
vFileName=viewed_ad.log;
eFileName=client_events.log;

# TODO Make this configurable
stgBaseDir=/media/home/dapang/data/prod/;
nnBaseDir=/media/home/dapang/data/prod/;

# TODO make this configurable
hdSrBaseDir=/user/dapang/data/prod/search_results
hdSBaseDir=/user/dapang/data/prod/search_page
hdVAdBaseDir=/user/dapang/data/prod/viewad;
hdEvtBaseDir=/user/dapang/data/prod/events;


# TODO make main pool hosts configurable
echo Creating Dir: $stgBaseDir
mkdir $stgBaseDir;
echo Creating Dir: $stgBaseDir/$processDate
mkdir $stgBaseDir/$processDate
echo Creating Dir: $stgBaseDir/$processDate/1
mkdir $stgBaseDir/$processDate/1
echo Creating Dir: $stgBaseDir/$processDate/2
mkdir $stgBaseDir/$processDate/2
echo Creating Dir: $stgBaseDir/$processDate/3
mkdir $stgBaseDir/$processDate/3
echo Creating Dir: $stgBaseDir/$processDate/4
mkdir $stgBaseDir/$processDate/4
echo Creating Dir: $stgBaseDir/$processDate/5
mkdir $stgBaseDir/$processDate/5
echo Creating Dir: $stgBaseDir/$processDate/6
mkdir $stgBaseDir/$processDate/6


# ========================================================================================================
# Handle search_results files....
echo Handle search_results files....

# TODO make main pool hosts configurable
scp web001:$tomcatBaseDir/$srFileName.$processDay $stgBaseDir/$processDate/1/
scp web002:$tomcatBaseDir/$srFileName.$processDay $stgBaseDir/$processDate/2/
scp web003:$tomcatBaseDir/$srFileName.$processDay $stgBaseDir/$processDate/3/
scp web004:$tomcatBaseDir/$srFileName.$processDay $stgBaseDir/$processDate/4/
scp web005:$tomcatBaseDir/$srFileName.$processDay $stgBaseDir/$processDate/5/
scp web006:$tomcatBaseDir/$srFileName.$processDay $stgBaseDir/$processDate/6/

# Cleanup Folder on Hadoop if needed
hadoop fs -rm -r -skipTrash $hdSrBaseDir/$processDate

echo Creating folders in hadoop..
echo hadoop fs -mkdir $hdSrBaseDir/$processDate
hadoop fs -mkdir $hdSrBaseDir/$processDate
echo hadoop fs -mkdir $hdSrBaseDir/$processDate/1
hadoop fs -mkdir $hdSrBaseDir/$processDate/1
echo hadoop fs -mkdir $hdSrBaseDir/$processDate/2
hadoop fs -mkdir $hdSrBaseDir/$processDate/2
echo hadoop fs -mkdir $hdSrBaseDir/$processDate/3
hadoop fs -mkdir $hdSrBaseDir/$processDate/3
echo hadoop fs -mkdir $hdSrBaseDir/$processDate/4
hadoop fs -mkdir $hdSrBaseDir/$processDate/4
echo hadoop fs -mkdir $hdSrBaseDir/$processDate/5
hadoop fs -mkdir $hdSrBaseDir/$processDate/5
echo hadoop fs -mkdir $hdSrBaseDir/$processDate/6
hadoop fs -mkdir $hdSrBaseDir/$processDate/6

echo Copying files to hadoop..
hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$srFileName.$processDay $hdSrBaseDir/$processDate/1/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$srFileName.$processDay $hdSrBaseDir/$processDate/2/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/3/$srFileName.$processDay $hdSrBaseDir/$processDate/3/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/4/$srFileName.$processDay $hdSrBaseDir/$processDate/4/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/5/$srFileName.$processDay $hdSrBaseDir/$processDate/5/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/6/$srFileName.$processDay $hdSrBaseDir/$processDate/6/

hadoop fs -chmod -R 777 $hdSrBaseDir/*
# ========================================================================================================
# Handle viewed_ad files........
echo Handle viewed_ad files........

scp web001:$tomcatBaseDir/$vFileName.$processDay $stgBaseDir/$processDate/1/
scp web002:$tomcatBaseDir/$vFileName.$processDay $stgBaseDir/$processDate/2/
scp web003:$tomcatBaseDir/$vFileName.$processDay $stgBaseDir/$processDate/3/
scp web004:$tomcatBaseDir/$vFileName.$processDay $stgBaseDir/$processDate/4/
scp web005:$tomcatBaseDir/$vFileName.$processDay $stgBaseDir/$processDate/5/
scp web006:$tomcatBaseDir/$vFileName.$processDay $stgBaseDir/$processDate/6/

# Cleanup Folder on Hadoop if needed
hadoop fs -rm -r -skipTrash $hdVAdBaseDir/$processDate

echo Creating folders in hadoop..
hadoop fs -mkdir $hdVAdBaseDir/$processDate
hadoop fs -mkdir $hdVAdBaseDir/$processDate/1
hadoop fs -mkdir $hdVAdBaseDir/$processDate/2
hadoop fs -mkdir $hdVAdBaseDir/$processDate/3
hadoop fs -mkdir $hdVAdBaseDir/$processDate/4
hadoop fs -mkdir $hdVAdBaseDir/$processDate/5
hadoop fs -mkdir $hdVAdBaseDir/$processDate/6

echo Copying files to hadoop..
hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$vFileName.$processDay $hdVAdBaseDir/$processDate/1/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$vFileName.$processDay $hdVAdBaseDir/$processDate/2/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/3/$vFileName.$processDay $hdVAdBaseDir/$processDate/3/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/4/$vFileName.$processDay $hdVAdBaseDir/$processDate/4/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/5/$vFileName.$processDay $hdVAdBaseDir/$processDate/5/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/6/$vFileName.$processDay $hdVAdBaseDir/$processDate/6/

hadoop fs -chmod -R 777 $hdVAdBaseDir/*
#========================================================================================================
# Handle search page files ...
echo Handle search page files ...

# TODO make main pool hosts configurable
scp web001:$tomcatBaseDir/$sFileName.$processDay $stgBaseDir/$processDate/1/
scp web002:$tomcatBaseDir/$sFileName.$processDay $stgBaseDir/$processDate/2/
scp web003:$tomcatBaseDir/$sFileName.$processDay $stgBaseDir/$processDate/3/
scp web004:$tomcatBaseDir/$sFileName.$processDay $stgBaseDir/$processDate/4/
scp web005:$tomcatBaseDir/$sFileName.$processDay $stgBaseDir/$processDate/5/
scp web006:$tomcatBaseDir/$sFileName.$processDay $stgBaseDir/$processDate/6/

# Cleanup Folder on Hadoop if needed
hadoop fs -rm -r -skipTrash $hdSBaseDir/$processDate

echo Creating folders in hadoop..
echo hadoop fs -mkdir $hdSBaseDir/$processDate
hadoop fs -mkdir $hdSBaseDir/$processDate
echo hadoop fs -mkdir $hdSBaseDir/$processDate/1
hadoop fs -mkdir $hdSBaseDir/$processDate/1
echo hadoop fs -mkdir $hdSBaseDir/$processDate/2
hadoop fs -mkdir $hdSBaseDir/$processDate/2
echo hadoop fs -mkdir $hdSBaseDir/$processDate/3
hadoop fs -mkdir $hdSBaseDir/$processDate/3
echo hadoop fs -mkdir $hdSBaseDir/$processDate/4
hadoop fs -mkdir $hdSBaseDir/$processDate/4
echo hadoop fs -mkdir $hdSBaseDir/$processDate/5
hadoop fs -mkdir $hdSBaseDir/$processDate/5
echo hadoop fs -mkdir $hdSBaseDir/$processDate/6
hadoop fs -mkdir $hdSBaseDir/$processDate/6

echo Copying files to hadoop..
hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$sFileName.$processDay $hdSBaseDir/$processDate/1/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$sFileName.$processDay $hdSBaseDir/$processDate/2/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/3/$sFileName.$processDay $hdSBaseDir/$processDate/3/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/4/$sFileName.$processDay $hdSBaseDir/$processDate/4/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/5/$sFileName.$processDay $hdSBaseDir/$processDate/5/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/6/$sFileName.$processDay $hdSBaseDir/$processDate/6/

hadoop fs -chmod -R 777 $hdSBaseDir/*
# ========================================================================================================
# Handle client_event files....
echo Handle client_event files....

# TODO make main pool hosts configurable
scp web001:$tomcatBaseDir/$eFileName.$processDay $stgBaseDir/$processDate/1/
scp web002:$tomcatBaseDir/$eFileName.$processDay $stgBaseDir/$processDate/2/
scp web003:$tomcatBaseDir/$eFileName.$processDay $stgBaseDir/$processDate/3/
scp web004:$tomcatBaseDir/$eFileName.$processDay $stgBaseDir/$processDate/4/
scp web005:$tomcatBaseDir/$eFileName.$processDay $stgBaseDir/$processDate/5/
scp web006:$tomcatBaseDir/$eFileName.$processDay $stgBaseDir/$processDate/6/

# Cleanup Folder on Hadoop if needed
hadoop fs -rm -r -skipTrash $hdEvtBaseDir/$processDate

echo Creating folders in hadoop..
echo hadoop fs -mkdir $hdEvtBaseDir/$processDate
hadoop fs -mkdir $hdEvtBaseDir/$processDate
echo hadoop fs -mkdir $hdEvtBaseDir/$processDate/1
hadoop fs -mkdir $hdEvtBaseDir/$processDate/1
echo hadoop fs -mkdir $hdEvtBaseDir/$processDate/2
hadoop fs -mkdir $hdEvtBaseDir/$processDate/2
echo hadoop fs -mkdir $hdEvtBaseDir/$processDate/3
hadoop fs -mkdir $hdEvtBaseDir/$processDate/3
echo hadoop fs -mkdir $hdEvtBaseDir/$processDate/4
hadoop fs -mkdir $hdEvtBaseDir/$processDate/4
echo hadoop fs -mkdir $hdEvtBaseDir/$processDate/5
hadoop fs -mkdir $hdEvtBaseDir/$processDate/5
echo hadoop fs -mkdir $hdEvtBaseDir/$processDate/6
hadoop fs -mkdir $hdEvtBaseDir/$processDate/6

echo Copying files to hadoop..
hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$eFileName.$processDay $hdEvtBaseDir/$processDate/1/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$eFileName.$processDay $hdEvtBaseDir/$processDate/2/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/3/$eFileName.$processDay $hdEvtBaseDir/$processDate/3/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/4/$eFileName.$processDay $hdEvtBaseDir/$processDate/4/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/5/$eFileName.$processDay $hdEvtBaseDir/$processDate/5/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/6/$eFileName.$processDay $hdEvtBaseDir/$processDate/6/

hadoop fs -chmod -R 777 $hdEvtBaseDir/*
# ========================================================================================================

# Cleanup temp folder and files
echo Deleting Dir: $stgBaseDir
rm -rf $stgBaseDir;


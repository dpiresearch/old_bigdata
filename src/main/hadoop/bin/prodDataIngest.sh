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
sFileName=search_page.log;
vFileName=viewed_ad.log;
eFileName=client_events.log;
hpFileName=home_page.log;
paFileName=post_ad.log;
payFileName=payment.log;
opFileName=order_payment.log;
raFileName=reply_ad.log;
tnsReplyFileName=tns_send_reply_ad.log;
saFileName=save_ad.log;
svdSrchFileName=saved_search.log;

# TODO Make this configurable
stgBaseDir=/media/home/dapang/data/prod/;
nnBaseDir=/media/home/dapang/data/prod/;

# TODO make this configurable
hdSrBaseDir=/user/dapang/data/prod/search_results
hdSBaseDir=/user/dapang/data/prod/search_page
hdVAdBaseDir=/user/dapang/data/prod/viewad;
hdEvtBaseDir=/user/dapang/data/prod/events;
hdHpBaseDir=/user/dapang/data/prod/home_page;
hdPaBaseDir=/user/dapang/data/prod/post_ad;
hdPayBaseDir=/user/dapang/data/prod/payment;
hdOpBaseDir=/user/dapang/data/prod/order_payment;
hdRaBaseDir=/user/dapang/data/prod/reply_ad;
hdTnsReplyBaseDir=/user/dapang/data/prod/tns_send_reply_ad;
hdSaveAdBaseDir=/user/dapang/data/prod/save_ad; 
hdSvdSrchBaseDir=/user/dapang/data/prod/saved_search; 

# TODO make main pool hosts configurable
echo Creating Staging Dir: $stgBaseDir
mkdir $stgBaseDir;
echo Creating Staging Dir: $stgBaseDir/$processDate
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

echo Copying files to hadoop..

# Compress and store
cat $stgBaseDir/$processDate/*/$srFileName.$processDay > $srFileName
bzip2 $srFileName
hadoop fs -mkdir $hdSrBaseDir/$processDate/all
hadoop fs -put $srFileName.bz2 $hdSrBaseDir/$processDate/all
rm $srFileName.bz2

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

# Compress and store
cat $stgBaseDir/$processDate/*/$vFileName.$processDay > $vFileName
bzip2 $vFileName
hadoop fs -mkdir $hdVAdBaseDir/$processDate/all
hadoop fs -put $vFileName.bz2 $hdVAdBaseDir/$processDate/all
rm $vFileName.bz2

echo Copying files to hadoop..

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
hadoop fs -mkdir $hdSBaseDir/$processDate

# Compress and store
cat $stgBaseDir/$processDate/*/$sFileName.$processDay > $sFileName
bzip2 $sFileName
hadoop fs -mkdir $hdSBaseDir/$processDate/all
hadoop fs -put $sFileName.bz2 $hdSBaseDir/$processDate/all
rm $sFileName.bz2

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
# Handle home_page files....
echo Handle home_page files....

# TODO make main pool hosts configurable
scp web001:$tomcatBaseDir/$hpFileName.$processDay $stgBaseDir/$processDate/1/
scp web002:$tomcatBaseDir/$hpFileName.$processDay $stgBaseDir/$processDate/2/
scp web003:$tomcatBaseDir/$hpFileName.$processDay $stgBaseDir/$processDate/3/
scp web004:$tomcatBaseDir/$hpFileName.$processDay $stgBaseDir/$processDate/4/
scp web005:$tomcatBaseDir/$hpFileName.$processDay $stgBaseDir/$processDate/5/
scp web006:$tomcatBaseDir/$hpFileName.$processDay $stgBaseDir/$processDate/6/

# Cleanup Folder on Hadoop if needed
hadoop fs -rm -r -skipTrash $hdHpBaseDir/$processDate

echo Creating folders in Hadoop..
echo hadoop fs -mkdir $hdHpBaseDir/$processDate

# Compress and store
cat $stgBaseDir/$processDate/*/$hpFileName.$processDay > $hpFileName
bzip2 $hpFileName
hadoop fs -mkdir $hdHpBaseDir/$processDate/all
hadoop fs -put $hpFileName.bz2 $hdHpBaseDir/$processDate/all
rm $hpFileName.bz2

echo Finished Copying files to hadoop..

hadoop fs -chmod -R 777 $hdHpBaseDir/*

# ========================================================================================================
# Handle post_ad files....
echo Handle post_ad files....

# TODO make main pool hosts configurable
scp web001:$tomcatBaseDir/$paFileName.$processDay $stgBaseDir/$processDate/1/
scp web002:$tomcatBaseDir/$paFileName.$processDay $stgBaseDir/$processDate/2/
scp web003:$tomcatBaseDir/$paFileName.$processDay $stgBaseDir/$processDate/3/
scp web004:$tomcatBaseDir/$paFileName.$processDay $stgBaseDir/$processDate/4/
scp web005:$tomcatBaseDir/$paFileName.$processDay $stgBaseDir/$processDate/5/
scp web006:$tomcatBaseDir/$paFileName.$processDay $stgBaseDir/$processDate/6/

# Cleanup Folder on Hadoop if needed
hadoop fs -rm -r -skipTrash $hdPaBaseDir/$processDate

echo Creating folders in Hadoop..
echo hadoop fs -mkdir $hdPaBaseDir/$processDate
hadoop fs -mkdir $hdPaBaseDir/$processDate

# Compress and store
cat $stgBaseDir/$processDate/*/$paFileName.$processDay > $paFileName
bzip2 $paFileName
hadoop fs -mkdir $hdPaBaseDir/$processDate/all
hadoop fs -put $paFileName.bz2 $hdPaBaseDir/$processDate/all
rm $paFileName.bz2

echo Copying files to hadoop..

hadoop fs -chmod -R 777 $hdPaBaseDir/*


# ========================================================================================================
# Handle saved_search files....
echo Handle saved_search files....

# TODO make main pool hosts configurable
scp web001:$tomcatBaseDir/$svdSrchFileName.$processDay $stgBaseDir/$processDate/1/
scp web002:$tomcatBaseDir/$svdSrchFileName.$processDay $stgBaseDir/$processDate/2/
scp web003:$tomcatBaseDir/$svdSrchFileName.$processDay $stgBaseDir/$processDate/3/
scp web004:$tomcatBaseDir/$svdSrchFileName.$processDay $stgBaseDir/$processDate/4/
scp web005:$tomcatBaseDir/$svdSrchFileName.$processDay $stgBaseDir/$processDate/5/
scp web006:$tomcatBaseDir/$svdSrchFileName.$processDay $stgBaseDir/$processDate/6/

# Cleanup Folder on Hadoop if needed
hadoop fs -rm -r -skipTrash $hdSvdSrchBaseDir/$processDate

echo Creating folders in Hadoop..
echo hadoop fs -mkdir $hdSvdSrchBaseDir/$processDate
hadoop fs -mkdir $hdSvdSrchBaseDir/$processDate

# Compress and store
cat $stgBaseDir/$processDate/*/$svdSrchFileName.$processDay > $svdSrchFileName
bzip2 $svdSrchFileName
hadoop fs -mkdir $hdSvdSrchBaseDir/$processDate/all
hadoop fs -put $svdSrchFileName.bz2 $hdSvdSrchBaseDir/$processDate/all
rm $svdSrchFileName.bz2

echo Copying files to hadoop..

hadoop fs -chmod -R 777 $hdSvdSrchBaseDir/*
# ========================================================================================================

# ========================================================================================================
# Handle reply_ad files....
echo Handle reply_ad files....

# TODO make main pool hosts configurable
scp web001:$tomcatBaseDir/$raFileName.$processDay $stgBaseDir/$processDate/1/
scp web002:$tomcatBaseDir/$raFileName.$processDay $stgBaseDir/$processDate/2/
scp web003:$tomcatBaseDir/$raFileName.$processDay $stgBaseDir/$processDate/3/
scp web004:$tomcatBaseDir/$raFileName.$processDay $stgBaseDir/$processDate/4/
scp web005:$tomcatBaseDir/$raFileName.$processDay $stgBaseDir/$processDate/5/
scp web006:$tomcatBaseDir/$raFileName.$processDay $stgBaseDir/$processDate/6/

# Cleanup Folder on Hadoop if needed
hadoop fs -rm -r -skipTrash $hdRaBaseDir/$processDate

echo Creating folders in Hadoop..
echo hadoop fs -mkdir $hdRaBaseDir/$processDate
hadoop fs -mkdir $hdRaBaseDir/$processDate

# Compress and store
cat $stgBaseDir/$processDate/*/$raFileName.$processDay > $raFileName
bzip2 $raFileName
hadoop fs -mkdir $hdRaBaseDir/$processDate/all
hadoop fs -put $raFileName.bz2 $hdRaBaseDir/$processDate/all
rm $raFileName.bz2

echo Copying files to hadoop..

hadoop fs -chmod -R 777 $hdRaBaseDir/*

# ========================================================================================================

# Handle save_ad files........
echo Copying QA Data over....
scp web001:$tomcatBaseDir/$saFileName.$processDay $stgBaseDir/$processDate/1/
scp web002:$tomcatBaseDir/$saFileName.$processDay $stgBaseDir/$processDate/2/
scp web003:$tomcatBaseDir/$saFileName.$processDay $stgBaseDir/$processDate/3/
scp web004:$tomcatBaseDir/$saFileName.$processDay $stgBaseDir/$processDate/4/
scp web005:$tomcatBaseDir/$saFileName.$processDay $stgBaseDir/$processDate/5/
scp web006:$tomcatBaseDir/$saFileName.$processDay $stgBaseDir/$processDate/6/

# Cleanup Folder on Hadoop if needed
hadoop fs -rm -r -skipTrash $hdSaveAdBaseDir/$processDate

echo Creating folders in hadoop..
hadoop fs -mkdir $hdSaveAdBaseDir/$processDate
hadoop fs -mkdir $hdSaveAdBaseDir/$processDate/1
hadoop fs -mkdir $hdSaveAdBaseDir/$processDate/2
hadoop fs -mkdir $hdSaveAdBaseDir/$processDate/3
hadoop fs -mkdir $hdSaveAdBaseDir/$processDate/4
hadoop fs -mkdir $hdSaveAdBaseDir/$processDate/5
hadoop fs -mkdir $hdSaveAdBaseDir/$processDate/6

echo Copying files to hadoop..
hadoop fs -copyFromLocal $stgBaseDir/$processDate/1/$saFileName.$processDay $hdSaveAdBaseDir/$processDate/1/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/2/$saFileName.$processDay $hdSaveAdBaseDir/$processDate/2/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/3/$saFileName.$processDay $hdSaveAdBaseDir/$processDate/3/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/4/$saFileName.$processDay $hdSaveAdBaseDir/$processDate/4/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/5/$saFileName.$processDay $hdSaveAdBaseDir/$processDate/5/
hadoop fs -copyFromLocal $stgBaseDir/$processDate/6/$saFileName.$processDay $hdSaveAdBaseDir/$processDate/6/

# TODO Make this recursive
hadoop fs -chmod 777 $hdSaveAdBaseDir/*
hadoop fs -chmod 777 $hdSaveAdBaseDir/*/*
hadoop fs -chmod 777 $hdSaveAdBaseDir/*/*/*
#========================================================================================================


# ========================================================================================================

# Handle tns send reply ad files........
echo Copying QA Data over....
scp batch001:$tomcatBaseDir/$tnsReplyFileName.$processDay $stgBaseDir/$processDate/1/
scp batch002:$tomcatBaseDir/$tnsReplyFileName.$processDay $stgBaseDir/$processDate/2/
scp batch003:$tomcatBaseDir/$tnsReplyFileName.$processDay $stgBaseDir/$processDate/3/
scp batch004:$tomcatBaseDir/$tnsReplyFileName.$processDay $stgBaseDir/$processDate/4/

# Cleanup Folder on Hadoop if needed
hadoop fs -rm -r -skipTrash $hdTnsReplyBaseDir/$processDate

echo Creating folders in hadoop..
echo hadoop fs -mkdir $hdTnsReplyBaseDir/$processDate
hadoop fs -mkdir $hdTnsReplyBaseDir/$processDate

# Compress and store
cat $stgBaseDir/$processDate/*/$tnsReplyFileName.$processDay > $tnsReplyFileName
bzip2 $tnsReplyFileName
hadoop fs -mkdir $hdTnsReplyBaseDir/$processDate/all
hadoop fs -put $tnsReplyFileName.bz2 $hdTnsReplyBaseDir/$processDate/all
rm $tnsReplyFileName.bz2

# TODO Make this recursive
hadoop fs -chmod -R 777 $hdTnsReplyBaseDir/*

# ========================================================================================================

# ========================================================================================================

# Handle payment files........
echo Copying Payment and Order Payment Data over....
scp secweb001:$tomcatBaseDir/$payFileName.$processDay $stgBaseDir/$processDate/1/
scp secweb002:$tomcatBaseDir/$payFileName.$processDay $stgBaseDir/$processDate/2/

# Cleanup Folder on Hadoop if needed
hadoop fs -rm -r -skipTrash $hdPayBaseDir/$processDate

echo Creating folders in hadoop..
hadoop fs -mkdir $hdPayBaseDir/$processDate

# Compress and store
cat $stgBaseDir/$processDate/*/$payFileName.$processDay > $payFileName
bzip2 $payFileName
hadoop fs -mkdir $hdPayBaseDir/$processDate/all
hadoop fs -put $payFileName.bz2 $hdPayBaseDir/$processDate/all
rm $payFileName.bz2

# TODO Make this recursive
hadoop fs -chmod -R 777 $hdPayBaseDir/*
# ========================================================================================================

# ========================================================================================================

# Handle order_payment files........
echo Copying Order Payment over....
scp secweb001:$tomcatBaseDir/$opFileName.$processDay $stgBaseDir/$processDate/1/
scp secweb002:$tomcatBaseDir/$opFileName.$processDay $stgBaseDir/$processDate/2/

# Cleanup Folder on Hadoop if needed
echo hadoop fs -rm -r -skipTrash $hdOpBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdOpBaseDir/$processDate

echo Creating folders in hadoop..
echo hadoop fs -mkdir $hdOpBaseDir/$processDate
hadoop fs -mkdir $hdOpBaseDir/$processDate

# Compress and store
cat $stgBaseDir/$processDate/*/$opFileName.$processDay > $opFileName
bzip2 $opFileName
hadoop fs -mkdir $hdOpBaseDir/$processDate/all
hadoop fs -put $opFileName.bz2 $hdOpBaseDir/$processDate/all
rm $opFileName.bz2

# TODO Make this recursive
hadoop fs -chmod -R 777 $hdOpBaseDir/*

# ========================================================================================================

# Cleanup temp folder and files
echo Deleting Dir: $stgBaseDir
rm -rf $stgBaseDir;


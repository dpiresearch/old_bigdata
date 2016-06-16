#!/bin/bash
#
# Generic production script to take care of ingesting data files regardless of production 
# infrastructure setup
#
# Takes care of hostnames and number of hosts
#

#
# Defaults
#
DAYS_AGO=1
OUTPUT_DIR=/user/dapang/data/prod/
TOMCAT_BASE_DIR=/var/log/tomcat7multi/frontend
STG_BASE_DIR=/media/home/dapang/data/prod/
NUM_HOSTS=10
ROOT_HOST=web
NUM_HOSTS_TNS=2
ROOT_HOST_TNS=tnsweb
NUM_HOSTS_BATCH=2
ROOT_HOST_BATCH=batch
ROOT_HOST_SEC=secweb
NUM_HOSTS_SEC=3

while getopts "a:o:n:r:h:q:s:t:b:g:" option
do
        case "${option}" in
                a) DAYS_AGO=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                n) NUM_HOSTS=${OPTARG};;
                r) ROOT_HOST=${OPTARG};;
                h) NUM_HOSTS_TNS=${OPTARG};;
                q) ROOT_HOST_TNS=${OPTARG};;                
                b) NUM_HOSTS_BATCH=${OPTARG};;
                g) ROOT_HOST_BATCH=${OPTARG};;                
                s) STG_BASE_DIR=${OPTARG};;
                t) TOMCAT_BASE_DIR=${OPTARG};;
        esac
done

roothst=$ROOT_HOST
tnshst=$ROOT_HOST_TNS
batchhst=$ROOT_HOST_BATCH
secwebhst=$ROOT_HOST_SEC
outputdir=$OUTPUT_DIR
tomcatBaseDir=$TOMCAT_BASE_DIR;
stgBaseDir=$STG_BASE_DIR;

today=`date +"%Y%m%d"`;
echo Today:$today;
currentdate=`date --date="$DAYS_AGO day ago" +%Y%m%d`
currentday=`date --date="$DAYS_AGO day ago" +%a`

processDate=$currentdate;
processDay=$currentday;

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== ROOT_HOST:           $roothst
echo ===== NUM_HOSTS:           $NUM_HOSTS
echo ===== ROOT_HOST_TNS:       $tnshst
echo ===== NUM_HOSTS_TNS:       $NUM_HOSTS_TNS
echo ===== ROOT_HOST_SEC:       $secwebhst
echo ===== NUM_HOSTS_SEC:       $NUM_HOSTS_SEC
echo ===== TOMCAT_BASE_DIR:     $tomcatBaseDir
echo ===== STG_BASE_DIR:        $stgBaseDir
echo ===== OUTPUT_DIR:          $outputdir
echo ===== Process Day:         $processDay
echo ===== Process Date:        $processDate
echo =================================================

# Log files we're looking for
srFileName=search_results.log;
sFileName=search_page.log;
vFileName=viewed_ad.log;
eFileName=client_events.log;
paFileName=post_ad.log;
hpFileName=home_page.log;
raFileName=reply_ad.log;
tnsReplyFileName=tns_send_reply_ad.log;
aeFileName=ad_event.log;
payFileName=payment.log;
opFileName=order_payment.log;
svdSrchFileName=saved_search.log;

# Directory where log files live
hdSrBaseDir=$outputdir/search_results
hdSBaseDir=$outputdir/search_page
hdVAdBaseDir=$outputdir/viewad;
hdEvtBaseDir=$outputdir/events;
hdPaBaseDir=$outputdir/post_ad;
hdHpBaseDir=$outputdir/home_page;
hdRaBaseDir=$outputdir/reply_ad;
hdTnsSendBaseDir=$outputdir/tns_send_reply_ad;
hdAdEvtBaseDir=$outputdir/ad_events;
hdPayBaseDir=$outputdir/payment;
hdOpBaseDir=$outputdir/order_payment;
hdSvdSrchBaseDir=$outputdir/saved_search;

# Prep file systems

# Sanity check for empty processDate, which may
# wipe out every in the hadoop directory
if [[ -z $processDate ]]; then echo "*** processDate HAS NO VALUE! Exiting to prevent possible data wipeout ***"; exit; else echo "*** processDate has value, OK to proceed ***"; fi

# Cleanup Folder on Hadoop if needed
echo hadoop fs -rm -r -skipTrash $hdSrBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdSrBaseDir/$processDate
echo hadoop fs -rm -r -skipTrash $hdVAdBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdVAdBaseDir/$processDate
echo hadoop fs -rm -r -skipTrash $hdSBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdSBaseDir/$processDate
echo hadoop fs -rm -r -skipTrash $hdEvtBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdEvtBaseDir/$processDate
echo hadoop fs -rm -r -skipTrash $hdPaBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdPaBaseDir/$processDate
echo hadoop fs -rm -r -skipTrash $hdHpBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdHpBaseDir/$processDate
echo hadoop fs -rm -r -skipTrash $hdRaBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdRaBaseDir/$processDate
echo hadoop fs -rm -r -skipTrash $hdAdEvtBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdAdEvtBaseDir/$processDate
echo hadoop fs -rm -r -skipTrash $hdOpBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdOpBaseDir/$processDate
echo hadoop fs -rm -r -skipTrash $hdPayBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdPayBaseDir/$processDate
echo hadoop fs -rm -r -skipTrash $hdSvdSrchBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdSvdSrchBaseDir/$processDate
echo hadoop fs -rm -r -skipTrash $hdTnsSendBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdTnsSendBaseDir/$processDate

echo == START Creating folders in hadoop..

echo hadoop fs -mkdir $hdSrBaseDir/$processDate
hadoop fs -mkdir $hdSrBaseDir/$processDate

echo hadoop fs -mkdir $hdVAdBaseDir/$processDate
hadoop fs -mkdir $hdVAdBaseDir/$processDate

echo hadoop fs -mkdir $hdSBaseDir/$processDate
hadoop fs -mkdir $hdSBaseDir/$processDate

echo hadoop fs -mkdir $hdEvtBaseDir/$processDate
hadoop fs -mkdir $hdEvtBaseDir/$processDate

echo hadoop fs -mkdir $hdPaBaseDir/$processDate
hadoop fs -mkdir $hdPaBaseDir/$processDate

echo hadoop fs -mkdir $hdHpBaseDir/$processDate
hadoop fs -mkdir $hdHpBaseDir/$processDate

echo hadoop fs -mkdir $hdRaBaseDir/$processDate
hadoop fs -mkdir $hdRaBaseDir/$processDate

echo hadoop fs -mkdir $hdAdEvtBaseDir/$processDate
hadoop fs -mkdir $hdAdEvtBaseDir/$processDate

echo hadoop fs -mkdir $hdPayBaseDir/$processDate
hadoop fs -mkdir $hdPayBaseDir/$processDate

echo hadoop fs -mkdir $hdOpBaseDir/$processDate
hadoop fs -mkdir $hdOpBaseDir/$processDate

echo hadoop fs -mkdir $hdSvdSrchBaseDir/$processDate
hadoop fs -mkdir $hdSvdSrchBaseDir/$processDate

echo hadoop fs -mkdir $hdTnsSendBaseDir/$processDate
hadoop fs -mkdir $hdTnsSendBaseDir/$processDate

# ========================================================================================================

echo Creating Dir: $stgBaseDir
mkdir $stgBaseDir;
echo Creating Dir: $stgBaseDir/$processDate
mkdir $stgBaseDir/$processDate

for ((hst=1;hst<=$NUM_HOSTS;++hst));
do
  echo Creating Staging Dir: $stgBaseDir/$processDate/$hst
  mkdir $stgBaseDir/$processDate/$hst

  hststr=`printf %03d $hst`

  echo " -- process search results files -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$srFileName.$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$srFileName.$processDay $stgBaseDir/$processDate/$hst/

  echo " -- process view files -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$vFileName.$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$vFileName.$processDay $stgBaseDir/$processDate/$hst/
  
  echo " -- process search page files -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$sFileName.$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$sFileName.$processDay $stgBaseDir/$processDate/$hst/

  echo " -- process event files -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$eFileName.$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$eFileName.$processDay $stgBaseDir/$processDate/$hst/
  echo hadoop fs -mkdir $hdEvtBaseDir/$processDate/$hst
  hadoop fs -mkdir $hdEvtBaseDir/$processDate/$hst
  echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$eFileName.$processDay $hdEvtBaseDir/$processDate/$hst/
  hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$eFileName.$processDay $hdEvtBaseDir/$processDate/$hst/

# ==== ENABLED WHEN 6.10 goes out ===
  echo " -- process post_ad files -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$paFileName.$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$paFileName.$processDay $stgBaseDir/$processDate/$hst/
# ==== ENABLED WHEN 6.10 goes out ===

  echo " -- process home_page files -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$hpFileName.$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$hpFileName.$processDay $stgBaseDir/$processDate/$hst/

  echo " -- process reply_ad files -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$raFileName.$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$raFileName.$processDay $stgBaseDir/$processDate/$hst/
  
  echo " -- process ad event files from web pool -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$aeFileName.$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$aeFileName.$processDay $stgBaseDir/$processDate/$hst/
  
  echo " -- process saved search event files from web pool -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$svdSrchFileName.$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$svdSrchFileName.$processDay $stgBaseDir/$processDate/$hst/
done

# Handle TNS Pool data files
for ((hst=1;hst<=$NUM_HOSTS_TNS;++hst));
do
  echo Creating Staging Dir: $stgBaseDir/$processDate/3$hst
  mkdir $stgBaseDir/$processDate/3$hst

  hststr=`printf %03d $hst`

  echo " -- process ad event files from tns pool -- "
  echo scp $tnshst$hststr:$tomcatBaseDir/$aeFileName.$processDay $stgBaseDir/$processDate/3$hst/
  scp $tnshst$hststr:$tomcatBaseDir/$aeFileName.$processDay $stgBaseDir/$processDate/3$hst/
done

# Handle Batch Pool data files
for ((hst=1;hst<=$NUM_HOSTS_BATCH;++hst));
do
  echo Creating Staging Dir: $stgBaseDir/$processDate/4$hst
  mkdir $stgBaseDir/$processDate/4$hst

  hststr=`printf %03d $hst`

  echo " -- process ad event files from batch pool -- "
  echo scp $batchhst$hststr:$tomcatBaseDir/$aeFileName.$processDay $stgBaseDir/$processDate/4$hst/
  scp $batchhst$hststr:$tomcatBaseDir/$aeFileName.$processDay $stgBaseDir/$processDate/4$hst/

  echo " -- process tns send batch job reply files from batch pool -- "
  echo scp $batchhst$hststr:$tomcatBaseDir/$tnsReplyFileName.$processDay $stgBaseDir/$processDate/$hst/
  scp $batchhst$hststr:$tomcatBaseDir/$tnsReplyFileName.$processDay $stgBaseDir/$processDate/$hst/
done

# Handle Secweb pool data files
# Currently waiting for siteops to allow passwordless access
for ((hst=1;hst<=$NUM_HOSTS_SEC;++hst));
do
  
  echo Creating Staging Dir: $stgBaseDir/$processDate/$hst
  mkdir $stgBaseDir/$processDate/$hst

  hststr=`printf %03d $hst`
  echo $hststr
  echo "== process payment files from secweb pool -- "
  echo scp $secwebhst$hststr:$tomcatBaseDir/$payFileName.$processDay $stgBaseDir/$processDate/$hst/
  scp $secwebhst$hststr:$tomcatBaseDir/$payFileName.$processDay $stgBaseDir/$processDate/$hst/

  echo "== process order payment files from secweb pool -- "
  echo scp $secwebhst$hststr:$tomcatBaseDir/$opFileName.$processDay $stgBaseDir/$processDate/$hst/
  scp $secwebhst$hststr:$tomcatBaseDir/$opFileName.$processDay $stgBaseDir/$processDate/$hst/

done

hadoop fs -chmod -R 777 $hdSrBaseDir/*
hadoop fs -chmod -R 777 $hdVAdBaseDir/*
hadoop fs -chmod -R 777 $hdSBaseDir/*
hadoop fs -chmod -R 777 $hdEvtBaseDir/*
hadoop fs -chmod -R 777 $hdPaBaseDir/*
hadoop fs -chmod -R 777 $hdHpBaseDir/*
hadoop fs -chmod -R 777 $hdRaBaseDir/*
hadoop fs -chmod -R 777 $hdAdEvtBaseDir/*
hadoop fs -chmod -R 777 $hdPayBaseDir/*
hadoop fs -chmod -R 777 $hdOpBaseDir/*
hadoop fs -chmod -R 777 $hdSvdSrchBaseDir/*
hadoop fs -chmod -R 777 $hdTnsSendBaseDir/*
#
# Compress
#
# Compress search page
echo "producing $sFileName.$processDay.bz2"
cat $stgBaseDir/$processDate/*/$sFileName.$processDay > $sFileName.$processDay
bzip2 $sFileName.$processDay
hadoop fs -mkdir $hdSBaseDir/$processDate/all
hadoop fs -put $sFileName.$processDay.bz2 $hdSBaseDir/$processDate/all
rm $sFileName.$processDay.bz2

# Compress home page
echo "producing $hpFileName.$processDay.bz2"
cat $stgBaseDir/$processDate/*/$hpFileName.$processDay > $hpFileName.$processDay
bzip2 $hpFileName.$processDay
hadoop fs -mkdir $hdHpBaseDir/$processDate/all
hadoop fs -put $hpFileName.$processDay.bz2 $hdHpBaseDir/$processDate/all
rm $hpFileName.$processDay.bz2

# Compress ad events
echo "producing $aeFileName.$processDay.bz2"
cat $stgBaseDir/$processDate/*/$aeFileName.$processDay > $aeFileName.$processDay
bzip2 $aeFileName.$processDay
hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/all
hadoop fs -put $aeFileName.$processDay.bz2 $hdAdEvtBaseDir/$processDate/all
rm $aeFileName.$processDay.bz2

# Compress post_ad events
echo "producing $paFileName.$processDay.bz2"
cat $stgBaseDir/$processDate/*/$paFileName.$processDay > $paFileName.$processDay
bzip2 $paFileName.$processDay
hadoop fs -mkdir $hdPaBaseDir/$processDate/all
hadoop fs -put $paFileName.$processDay.bz2 $hdPaBaseDir/$processDate/all
rm $paFileName.$processDay.bz2

# Compress reply_ad events
echo "producing $raFileName.$processDay.bz2"
cat $stgBaseDir/$processDate/*/$raFileName.$processDay > $raFileName.$processDay
bzip2 $raFileName.$processDay
hadoop fs -mkdir $hdRaBaseDir/$processDate/all
hadoop fs -put $raFileName.$processDay.bz2 $hdRaBaseDir/$processDate/all
rm $raFileName.$processDay.bz2

# Compress tns send batch job reply_ad events
echo "producing $tnsReplyFileName.$processDay.bz2"
cat $stgBaseDir/$processDate/*/$tnsReplyFileName.$processDay > $tnsReplyFileName.$processDay
bzip2 $tnsReplyFileName.$processDay
hadoop fs -mkdir $hdTnsSendBaseDir/$processDate/all
hadoop fs -put $tnsReplyFileName.$processDay.bz2 $hdTnsSendBaseDir/$processDate/all
rm $tnsReplyFileName.$processDay.bz2

# Compress search_results events
echo "producing $srFileName.$processDay.bz2"
cat $stgBaseDir/$processDate/*/$srFileName.$processDay > $srFileName.$processDay
bzip2 $srFileName.$processDay
hadoop fs -mkdir $hdSrBaseDir/$processDate/all
hadoop fs -put $srFileName.$processDay.bz2 $hdSrBaseDir/$processDate/all
rm $srFileName.$processDay.bz2

# Compress view_ad events
echo "producing $vFileName.$processDay.bz2"
cat $stgBaseDir/$processDate/*/$vFileName.$processDay > $vFileName.$processDay
bzip2 $vFileName.$processDay
hadoop fs -mkdir $hdVAdBaseDir/$processDate/all
hadoop fs -put $vFileName.$processDay.bz2 $hdVAdBaseDir/$processDate/all
rm $vFileName.$processDay.bz2

# Compress payment events
echo "producing $payFileName.$processDay.bz2"
cat $stgBaseDir/$processDate/*/$payFileName.$processDay > $payFileName.$processDay
bzip2 $payFileName.$processDay
hadoop fs -mkdir $hdPayBaseDir/$processDate/all
hadoop fs -put $payFileName.$processDay.bz2 $hdPayBaseDir/$processDate/all
rm $payFileName.$processDay.bz2

# Compress order payment events
echo "producing $opFileName.$processDay.bz2"
cat $stgBaseDir/$processDate/*/$opFileName.$processDay > $opFileName.$processDay
bzip2 $opFileName.$processDay
hadoop fs -mkdir $hdOpBaseDir/$processDate/all
hadoop fs -put $opFileName.$processDay.bz2 $hdOpBaseDir/$processDate/all
rm $opFileName.$processDay.bz2

# Compress saved_search events
echo "producing $svdSrchFileName.$processDay.bz2"
cat $stgBaseDir/$processDate/*/$svdSrchFileName.$processDay > $svdSrchFileName.$processDay
bzip2 $svdSrchFileName.$processDay
hadoop fs -mkdir $hdSvdSrchBaseDir/$processDate/all
hadoop fs -put $svdSrchFileName.$processDay.bz2 $hdSvdSrchBaseDir/$processDate/all
rm $svdSrchFileName.$processDay.bz2
# ========================================================================================================

# Cleanup temp folder and files
echo Deleting Dir: $stgBaseDir
rm -rf $stgBaseDir;


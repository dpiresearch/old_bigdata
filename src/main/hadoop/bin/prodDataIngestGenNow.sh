#!/bin/bash
#
# Modified version of prodDataIngestGen.sh that pulls in logs now.
#
# Generic production script to take care of ingesting data files regardless of production 
# infrastructure setup
#
# Takes care of hostnames and number of hosts
#

#
# Defaults
#
DAYS_AGO=0
OUTPUT_DIR=/user/dapang/data/exp/
TOMCAT_BASE_DIR=/var/log/tomcat7multi/frontend
STG_BASE_DIR=/media/home/dapang/data/exp/
NUM_HOSTS=10
ROOT_HOST=web

while getopts "a:o:n:r:s:t:" option
do
        case "${option}" in
                a) DAYS_AGO=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                n) NUM_HOSTS=${OPTARG};;
                r) ROOT_HOST=${OPTARG};;
                s) STG_BASE_DIR=${OPTARG};;
                t) TOMCAT_BASE_DIR=${OPTARG};;
        esac
done

roothst=$ROOT_HOST
outputdir=$OUTPUT_DIR
tomcatBaseDir=$TOMCAT_BASE_DIR;
stgBaseDir=$STG_BASE_DIR;

today=`date +"%Y%m%d"`;
echo Today:$today;
currentdate=`date --date="$DAYS_AGO day ago" +%Y%m%d`
currentday=`date --date="$DAYS_AGO day ago" +%a`

processDate=$currentdate;
# processDay=$currentday;
processDay=

echo ===== Running with the following parameters =====
echo ===== ROOT_HOST:           $roothst
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== NUM_HOSTS:           $NUM_HOSTS
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

# Directory where log files live
hdSrBaseDir=$outputdir/search_results
hdSBaseDir=$outputdir/search_page
hdVAdBaseDir=$outputdir/viewad;
hdEvtBaseDir=$outputdir/events;
hdPaBaseDir=$outputdir/post_ad;
hdHpBaseDir=$outputdir/home_page;
hdRaBaseDir=$outputdir/reply_ad;

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
  echo scp $roothst$hststr:$tomcatBaseDir/$srFileName$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$srFileName$processDay $stgBaseDir/$processDate/$hst/
  echo hadoop fs -mkdir $hdSrBaseDir/$processDate/$hst
  hadoop fs -mkdir $hdSrBaseDir/$processDate/$hst
  echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$srFileName$processDay $hdSrBaseDir/$processDate/$hst/
  hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$srFileName$processDay $hdSrBaseDir/$processDate/$hst/

  echo " -- process view files -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$vFileName$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$vFileName$processDay $stgBaseDir/$processDate/$hst/
  echo hadoop fs -mkdir $hdVAdBaseDir/$processDate/$hst
  hadoop fs -mkdir $hdVAdBaseDir/$processDate/$hst
  echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$vFileName$processDay $hdVAdBaseDir/$processDate/$hst/
  hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$vFileName$processDay $hdVAdBaseDir/$processDate/$hst/
  
  echo " -- process search page files -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$sFileName$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$sFileName$processDay $stgBaseDir/$processDate/$hst/
  echo hadoop fs -mkdir $hdSBaseDir/$processDate/$hst
  hadoop fs -mkdir $hdSBaseDir/$processDate/$hst
  echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$sFileName$processDay $hdSBaseDir/$processDate/$hst/
  hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$sFileName$processDay $hdSBaseDir/$processDate/$hst/

  echo " -- process event files -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$eFileName$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$eFileName$processDay $stgBaseDir/$processDate/$hst/
  echo hadoop fs -mkdir $hdEvtBaseDir/$processDate/$hst
  hadoop fs -mkdir $hdEvtBaseDir/$processDate/$hst
  echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$eFileName$processDay $hdEvtBaseDir/$processDate/$hst/
  hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$eFileName$processDay $hdEvtBaseDir/$processDate/$hst/

# ==== ENABLED WHEN 6.10 goes out ===
  echo " -- process post_ad files -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$paFileName$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$paFileName$processDay $stgBaseDir/$processDate/$hst/
  echo hadoop fs -mkdir $hdPaBaseDir/$processDate/$hst
  hadoop fs -mkdir $hdPaBaseDir/$processDate/$hst
  echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$paFileName$processDay $hdPaBaseDir/$processDate/$hst/
  hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$paFileName$processDay $hdPaBaseDir/$processDate/$hst/
# ==== ENABLED WHEN 6.10 goes out ===

  echo " -- process home_page files -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$hpFileName$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$hpFileName$processDay $stgBaseDir/$processDate/$hst/
  echo hadoop fs -mkdir $hdHpBaseDir/$processDate/$hst
  hadoop fs -mkdir $hdHpBaseDir/$processDate/$hst
  echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$hpFileName$processDay $hdHpBaseDir/$processDate/$hst/
  hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$hpFileName$processDay $hdHpBaseDir/$processDate/$hst/

  echo " -- process reply_ad files -- "
  echo scp $roothst$hststr:$tomcatBaseDir/$raFileName$processDay $stgBaseDir/$processDate/$hst/
  scp $roothst$hststr:$tomcatBaseDir/$raFileName$processDay $stgBaseDir/$processDate/$hst/
  echo hadoop fs -mkdir $hdRaBaseDir/$processDate/$hst
  hadoop fs -mkdir $hdRaBaseDir/$processDate/$hst
  echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$raFileName$processDay $hdRaBaseDir/$processDate/$hst/
  hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$raFileName$processDay $hdRaBaseDir/$processDate/$hst/
done

hadoop fs -chmod -R 777 $hdSrBaseDir/*
hadoop fs -chmod -R 777 $hdVAdBaseDir/*
hadoop fs -chmod -R 777 $hdSBaseDir/*
hadoop fs -chmod -R 777 $hdEvtBaseDir/*
hadoop fs -chmod -R 777 $hdPaBaseDir/*
hadoop fs -chmod -R 777 $hdHpBaseDir/*
hadoop fs -chmod -R 777 $hdRaBaseDir/*

# ========================================================================================================

# Cleanup temp folder and files
echo Deleting Dir: $stgBaseDir
rm -rf $stgBaseDir;


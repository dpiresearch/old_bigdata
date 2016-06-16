#!/bin/bash
#
# Based off prodDataIngestGenNow.sh that pulls in logs now.
# Limited to pulling only ad_events.log files
#
# Generic production script to take care of ingesting data files regardless of production 
# infrastructure setup
#
# Takes care of hostnames and number of hosts
#
# -o /user/dapang/data/prod -s /user/dapang/data/prod -n 6 -h 2
#
# Defaults
#
DAYS_AGO=0
OUTPUT_DIR=/user/hadoop/data/exp
TOMCAT_BASE_DIR=/var/log/tomcat7multi/frontend
# STG_BASE_DIR=/media/home/dapang/data/exp/
STG_BASE_DIR=/home/hadoop/data/exp/viewad
NUM_HOSTS=14
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

webhst=$ROOT_HOST
outputdir=$OUTPUT_DIR
tomcatBaseDir=$TOMCAT_BASE_DIR;
stgBaseDir=$STG_BASE_DIR;

currentdate=`date --date="$DAYS_AGO day ago" +%Y%m%d`
currentday=`date --date="$DAYS_AGO day ago" +%a`

processDate=$currentdate;
processDay=

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== ROOT_HOST:           $webhst
echo ===== NUM_HOSTS:           $NUM_HOSTS
echo ===== TOMCAT_BASE_DIR:     $tomcatBaseDir
echo ===== STG_BASE_DIR:        $stgBaseDir
echo ===== OUTPUT_DIR:          $outputdir
echo ===== Process Day:         $processDay
echo ===== Process Date:        $processDate
echo =================================================

# Log files we're looking for
vFileName=viewed_ad.log;

# Directory where log files live
hdViewAdBaseDir=$outputdir/viewad;

# Prep file systems

# Sanity check for empty processDate, which may
# wipe out every in the hadoop directory
if [[ -z $processDate ]]; then echo "*** processDate HAS NO VALUE! Exiting to prevent possible data wipeout ***"; exit; else echo "*** processDate has value, OK to proceed ***"; fi

# Cleanup Folder on Hadoop if needed
echo hadoop fs -rm -r -skipTrash $hdViewAdBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdViewAdBaseDir/$processDate

echo == START Creating folders in hadoop..

echo hadoop fs -mkdir $hdViewAdBaseDir/$processDate
hadoop fs -mkdir $hdViewAdBaseDir/$processDate

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


  echo " -- process view ad files from web pool -- "
  echo scp $webhst$hststr:$tomcatBaseDir/$vFileName $stgBaseDir/$processDate/$hst/
  scp $webhst$hststr:$tomcatBaseDir/$vFileName $stgBaseDir/$processDate/$hst/
#  echo hadoop fs -mkdir $hdViewAdBaseDir/$processDate/$hst
#  hadoop fs -mkdir $hdViewAdBaseDir/$processDate/$hst
#  echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$vFileName $hdViewAdBaseDir/$processDate/$hst/
#  hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$vFileName $hdViewAdBaseDir/$processDate/$hst/

done

# Compress view_ad events
echo "producing $vFileName.bz2"
cat $stgBaseDir/$processDate/[0-9]*/$vFileName > $vFileName
bzip2 $vFileName
hadoop fs -mkdir $hdViewAdBaseDir/$processDate/all
hadoop fs -put $vFileName.bz2 $hdViewAdBaseDir/$processDate/all
rm $vFileName.bz2

hadoop fs -chmod -R 777 $hdViewAdBaseDir/*

# ========================================================================================================

# Cleanup temp folder and files
echo Deleting Dir: $stgBaseDir
rm -rf $stgBaseDir;


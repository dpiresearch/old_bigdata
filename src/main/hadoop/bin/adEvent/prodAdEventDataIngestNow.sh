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
OUTPUT_DIR=/user/hadoop/data/exp/
TOMCAT_BASE_DIR=/var/log/tomcat7multi/frontend
# STG_BASE_DIR=/media/home/dapang/data/exp/
STG_BASE_DIR=/home/hadoop/data/exp/ad_event
NUM_HOSTS=14
NUM_HOSTS_TNS=2
NUM_HOSTS_BATCH=2
ROOT_HOST=web
ROOT_HOST_TNS=tnsweb
ROOT_HOST_BATCH=batch

while getopts "a:o:n:h:q:r:s:t:b:g:" option
do
        case "${option}" in
                a) DAYS_AGO=${OPTARG};;
                o) OUTPUT_DIR=${OPTARG};;
                n) NUM_HOSTS=${OPTARG};;
                h) NUM_HOSTS_TNS=${OPTARG};;
                b) NUM_HOSTS_BATCH=${OPTARG};;
                g) ROOT_HOST_BATCH=${OPTARG};;
                q) ROOT_HOST_TNS=${OPTARG};;
                r) ROOT_HOST=${OPTARG};;
                s) STG_BASE_DIR=${OPTARG};;
                t) TOMCAT_BASE_DIR=${OPTARG};;
        esac
done

webhst=$ROOT_HOST
tnshst=$ROOT_HOST_TNS
batchhst=$ROOT_HOST_BATCH
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
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== ROOT_HOST:           $webhst
echo ===== NUM_HOSTS:           $NUM_HOSTS
echo ===== ROOT_HOST_TNS:       $tnshst
echo ===== NUM_HOSTS_TNS:       $NUM_HOSTS_TNS
echo ===== ROOT_HOST_BATCH:     $batchhst
echo ===== NUM_HOSTS_BATCH:     $NUM_HOSTS_BATCH
echo ===== TOMCAT_BASE_DIR:     $tomcatBaseDir
echo ===== STG_BASE_DIR:        $stgBaseDir
echo ===== OUTPUT_DIR:          $outputdir
echo ===== Process Day:         $processDay
echo ===== Process Date:        $processDate
echo =================================================

# Log files we're looking for
aeFileName=ad_event.log;

# Directory where log files live
hdAdEvtBaseDir=$outputdir/ad_events;

# Prep file systems

# Sanity check for empty processDate, which may
# wipe out every in the hadoop directory
if [[ -z $processDate ]]; then echo "*** processDate HAS NO VALUE! Exiting to prevent possible data wipeout ***"; exit; else echo "*** processDate has value, OK to proceed ***"; fi

# Cleanup Folder on Hadoop if needed
echo hadoop fs -rm -r -skipTrash $hdAdEvtBaseDir/$processDate
hadoop fs -rm -r -skipTrash $hdAdEvtBaseDir/$processDate

echo == START Creating folders in hadoop..

echo hadoop fs -mkdir $hdAdEvtBaseDir/$processDate
hadoop fs -mkdir $hdAdEvtBaseDir/$processDate
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


  echo " -- process ad event files from web pool -- "
  echo scp $webhst$hststr:$tomcatBaseDir/$aeFileName$processDay $stgBaseDir/$processDate/$hst/
  scp $webhst$hststr:$tomcatBaseDir/$aeFileName$processDay $stgBaseDir/$processDate/$hst/
  echo hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/$hst
  hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/$hst
  echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$aeFileName$processDay $hdAdEvtBaseDir/$processDate/$hst/
  hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$aeFileName$processDay $hdAdEvtBaseDir/$processDate/$hst/

done

for ((tnshstnum=1;tnshstnum<=$NUM_HOSTS_TNS;++tnshstnum));
do
  hst=$((30+$tnshstnum))
  echo Creating Staging Dir: $stgBaseDir/$processDate/$hst
  mkdir $stgBaseDir/$processDate/$hst

  dirstr=$hst
  hststr=`printf %03d $tnshstnum`

  echo " -- process ad event files from tns pool -- "
  echo scp $tnshst$hststr:$tomcatBaseDir/$aeFileName$processDay $stgBaseDir/$processDate/$hst/
  scp $tnshst$hststr:$tomcatBaseDir/$aeFileName$processDay $stgBaseDir/$processDate/$hst/
  echo hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/$hst
  hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/$hst
  echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$aeFileName$processDay $hdAdEvtBaseDir/$processDate/$hst/
  hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$aeFileName$processDay $hdAdEvtBaseDir/$processDate/$hst/

done

for ((batchhstnum=1;batchhstnum<=$NUM_HOSTS_BATCH;++batchhstnum));
do
  hst=$((40+$batchhstnum))
  echo Creating Staging Dir: $stgBaseDir/$processDate/$hst
  mkdir $stgBaseDir/$processDate/$hst

  dirstr=$hst
  hststr=`printf %03d $batchhstnum`

  echo " -- process ad event files from tns pool -- "
  echo scp $batchhst$hststr:$tomcatBaseDir/$aeFileName$processDay $stgBaseDir/$processDate/$hst/
  scp $batchhst$hststr:$tomcatBaseDir/$aeFileName$processDay $stgBaseDir/$processDate/$hst/
  echo hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/$hst
  hadoop fs -mkdir $hdAdEvtBaseDir/$processDate/$hst
  echo hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$aeFileName$processDay $hdAdEvtBaseDir/$processDate/$hst/
  hadoop fs -copyFromLocal $stgBaseDir/$processDate/$hst/$aeFileName$processDay $hdAdEvtBaseDir/$processDate/$hst/

done

hadoop fs -chmod -R 777 $hdAdEvtBaseDir/*

# ========================================================================================================

# Cleanup temp folder and files
echo Deleting Dir: $stgBaseDir
rm -rf $stgBaseDir;


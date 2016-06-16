#!/bin/sh
#
#   This script checks if the input folders exist. To
#   be used prior to executing any pig scripts.
#

# Ingest log file names
srFileName=search_results.log;
sFileName=search_page.log;
vFileName=viewed_ad.log;
eFileName=client_events.log;

# Ingest sub direcotry names
srBaseDir=search_results
sBaseDir=search_page
vBaseDir=viewad;
eBaseDir=events;

# How many days ago will be day zero for this script
DAYS_AGO=0

#Base dir where ingest subfolders reside
INPUT_DIR=/user/hadoop/data/prod/input

NUM_HOSTS=10

CHECK_ALL_FILES=false

while getopts "i:a:n:f:" option
do
        case "${option}" in
                i) INPUT_DIR=${OPTARG};;
                a) DAYS_AGO=${OPTARG};;
                n) NUM_HOSTS=${OPTARG};;
                f) CHECK_ALL_FILES=${OPTARG};;
        esac
done

echo ===== Running with the following parameters =====
echo ===== DAYS_AGO:            $DAYS_AGO
echo ===== INPUT_DIR:           $INPUT_DIR
echo ===== NUM_HOSTS:        	$NUM_HOSTS
echo ===== CHECK_ALL_FILES:     $CHECK_ALL_FILES
echo =================================================

inputDir=$INPUT_DIR
should_exit=false

today=`date +"%Y%m%d"`;
echo Today:$today;
now=`date --date="$DAYS_AGO day ago" +%Y%m%d`
echo "Using date: $now"


# Check the existence of folders first
# 1) Search Results
srFolder=$INPUT_DIR/$srBaseDir/$now
echo Checking folder:$srFolder ...
hadoop fs -test -d $srFolder
if [ "$?" = "0" ]; then
    echo "Success"
else
    echo "Folder does not exist" 1>&2
    exit 1
fi

# 2) Search Page
sFolder=$INPUT_DIR/$sBaseDir/$now
echo Checking folder:$sFolder ...
hadoop fs -test -d $sFolder
if [ "$?" = "0" ]; then
    echo "Success"
else
    echo "Folder does not exist" 1>&2
    exit 1
fi

# 3) View Ads
vFolder=$INPUT_DIR/$vBaseDir/$now
echo Checking folder:$vFolder ...
hadoop fs -test -d $vFolder
if [ "$?" = "0" ]; then
    echo "Success"
else
    echo "Folder does not exist" 1>&2
    exit 1
fi

#  4) Client Events
eFolder=$INPUT_DIR/$eBaseDir/$now
echo Checking folder:$eFolder ...
hadoop fs -test -d $eFolder
if [ "$?" = "0" ]; then
    echo "Success"
else
    echo "Folder does not exist" 1>&2
    exit 1
fi

# Now check for existence of files under each host sub folders
found_sr=false
found_s=false
found_v=false
found_c=false
for hst in `seq 1 $NUM_HOSTS`
do
#   1) Search Results File
    srFile=$srFolder/$hst/$srFileName*
    echo Checking file:$srFile ...
    hadoop fs -test -e $srFile
    if [ "$?" = "0" ]; then
        found_sr=true
    	echo "Success"
    else
    	echo "File does not exist" 1>&2
    	if $CHECK_ALL_FILES ; then
        	should_exit=true
        fi
    fi

#   2) Search Page File
    sFile=$sFolder/$hst/$sFileName*
    echo Checking file:$sFile ...
    hadoop fs -test -e $sFile
    if [ "$?" = "0" ]; then
        found_s=true
    	echo "Success"
    else
    	echo "File does not exist" 1>&2
    	if $CHECK_ALL_FILES ; then
        	should_exit=true
        fi
    fi

#   3) View Ad File
    vFile=$vFolder/$hst/$vFileName*
    echo Checking file:$vFile ...
    hadoop fs -test -e $vFile
    if [ "$?" = "0" ]; then
        found_v=true
    	echo "Success"
    else
    	echo "File does not exist" 1>&2
    	if $CHECK_ALL_FILES ; then
        	should_exit=true
        fi
    fi

#   4) Client Events File
    eFile=$eFolder/$hst/$eFileName*
    echo Checking file:$eFile ...
    hadoop fs -test -e $eFile
    if [ "$?" = "0" ]; then
        found_c=true
    	echo "Success"
    else
    	echo "File does not exist" 1>&2
    	if $CHECK_ALL_FILES ; then
        	should_exit=true
        fi
    fi
done

if $should_exit ; then
	exit 1;
fi


if ! $CHECK_ALL_FILES && ! $found_sr || ! $found_s || ! $found_v || ! $found_c ; then
    exit 1;
fi
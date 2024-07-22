#!/bin/ksh

echo "$0 is starting"
rm -f /home/hduser/hivepart/Praveen_partload.hql

#1. validated the invocation of the script

if [ $# -ne 2 ]
then
echo "$0 requires source data path and the target table name to load, usage : bash hivepart.sh /home/hduser/hivepart curated.txnrecsbydtreg"
exit 10
fi

echo "$1 is the path"
echo "$2 is the dbname.tablename"

#if atleast one file presents -> create the table for the first time then -> 
# continue looping the path to handle all files -> every iteration of the loop -> 

if ls $1/txns_*_* &>/dev/null
then
  echo " Creating the table for the first time "
  hive -e "create table if not exists $2 (txnno INT, txndate STRING, custno INT, amount DOUBLE, category STRING,product STRING, city STRING, state STRING, spendby STRING) 
  partitioned by (datadt date,region string) row format delimited fields terminated by ',' stored as textfile;"
 
  for i in $1/txns_*_*
  do
   echo "file with path name is $i"
#identify filename /home/hduser/praveen_hive_Partition/txns_20230319_PADE -> txns_20201007_PADE
   filenm=$(basename $i)
   echo "file name is $filenm"
   dirnm=$(dirname $i)
   echo "directory name is $dirnm"

#identify date from the filename 20201007 -> 2020-10-07
   dt=`echo $filenm |awk -F'_' '{print $2}'`
   echo "raw date is $dt"
   dtfmt=`date -d $dt +'%Y-%m-%d'`
   echo "formatted date is $dtfmt"

   reg=`echo $filenm |awk -F'_' '{print $3}'`
   echo "region is $reg" 

echo "LOAD DATA LOCAL INPATH '$1/$filenm' OVERWRITE INTO TABLE $2 PARTITION (datadt='$dtfmt',region='$reg');"

   echo "LOAD DATA LOCAL INPATH '$1/$filenm' OVERWRITE INTO TABLE $2 PARTITION (datadt='$dtfmt',region='$reg');" >> /home/hduser/praveen_hive_Partition/Praveen_partload.hql

  echo "show partitions $2 ;" >> /home/hduser/praveen_hive_Partition/Praveen_partload.hql
  echo "select datadt,region,count(1) from $2  group by datadt,region;" >> /home/hduser/praveen_hive_Partition/Praveen_partload.hql
 
done

  echo "triggering the hql script once for all to load data, show partition after load is completed, showing some data output..."

echo "show partitions $2 ;" >> /home/hduser/praveen_hive_Partition/Praveen_partload.hql
  echo "select datadt,region,count(1) from $2  group by datadt,region;" >> /home/hduser/praveen_hive_Partition/Praveen_partload.hql

  echo "triggering the hql script once for all to load data, show partition after load is completed, showing some data output..."
  hive -f /home/hduser/praveen_hive_Partition/Praveen_partload.hql


  echo "Cleanup - archiving the files"
  mkdir -p /home/hduser/praveen_hivepartarchive/
  gzip $1/txns_*_*
  mv $1/txns_*_*.gz /home/hduser/praveen_hivepartarchive/
else
  echo "`date` There is no files present in the given source location $1"
fi
echo "Script finished

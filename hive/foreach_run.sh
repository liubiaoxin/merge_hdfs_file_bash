#!/bin/bash
script_path=/data/qding_etl/bash/merge_hdfs_file_bash/hive

s_date=$1
s_date=${s_date:=`date +%Y-%m-%d  -d "1 days ago"`}


#days list
minday=`date +%Y%m%d  -d "2017-12-31"`
maxday=`date +%Y%m%d  -d "2018-11-09"`
pdt=""
echo $minday  $maxday

for((i=1;i<=365;i++))
do
dt=`date -d "${i} days ago" +%Y%m%d`
if [ $dt -gt $minday -a $dt -lt $maxday ]; then
   dt=`date -d "$dt"  +%Y-%m-%d`
   #echo $dt
   pdt="$pdt $dt"
fi
done

for line in `cat $script_path/merch_tables.conf`
do
   array=(${line//,/ })  
   schema="${array[0]}"
   table_name=${array[1]}
   for dt in $pdt
       do
           s_date=$dt
           echo "sh $script_path/merch_hive_files.sh  $schema  $table_name  $s_date"
           sh $script_path/merch_hive_files.sh  $schema  $table_name  $s_date
        done
done



#!/bin/bash
script_path=/data/qding_etl/bash/merge_hdfs_file_bash/impala

s_date=$1
s_date=${s_date:=`date +%Y-%m-%d  -d "2 days ago"`}

for line in `cat $script_path/merch_tables.conf`
do
   array=(${line//,/ })
   schema="${array[0]}"
   table_name=${array[1]}
   echo "sh $script_path/merch_impala_files.sh  $schema  $table_name  $s_date"
   sh $script_path/merch_impala_files.sh  $schema  $table_name  $s_date
done


#!/bin/bash
schema=$1
table_name=$2
s_date=$3

echo "table======$schema.$table_name   s_date======$s_date"


script_path=/data/qding_etl/bash/merge_hdfs_file_bash/hive
dt_path=/qding/db/${schema}/${table_name}/dt=${s_date}

hadoop  fs -test -e  $dt_path
if [ $? -eq 0 ] ;then
	echo "path  exist:$dt_path"
	file_num=`hdfs dfs -count -q $dt_path | awk '{print $6}'`
        echo "file_num=======$file_num"
	if [ $file_num -gt 1 ];then
		file_size=`hdfs dfs -du -s $dt_path | awk '{print $1}'`
		echo "original_file_size=========$file_size B"

		size_mb=`expr $file_size / 1024 / 1024`
		echo "final_size_mb=====$size_mb MB"
		reduce_num=$((($size_mb/128) + ($size_mb % 128> 0)))

		if [ $reduce_num = 0 ];then
			reduce_num=1
		fi

		echo "reduct_num======$reduce_num 个"
		
		#exec hive sql
		hive -hiveconf schema=$schema -hiveconf table_name=$table_name   -hiveconf s_date=$s_date    -hiveconf reduce_num=$reduce_num  -f  $script_path/merch_file.sql

		#delete old partition data
		echo "hadoop fs -rmr -skipTrash  /qding/db/${schema}/${table_name}/dt=${s_date}"
		hadoop fs -rmr -skipTrash  /qding/db/${schema}/${table_name}/dt=${s_date}
		
		#mv tmp partition data to current partition
		echo "hadoop fs -mv /qding/db/${schema}/${table_name}/dt=tmp_${s_date}  /qding/db/${schema}/${table_name}/dt=${s_date}"
		hadoop fs -mv /qding/db/${schema}/${table_name}/dt=tmp_${s_date}  /qding/db/${schema}/${table_name}/dt=${s_date}
		
		#drop tmp partition
		echo "hive -e \"alter table ${schema}.${table_name}  drop IF EXISTS partition(dt='tmp_${s_date}') \""
		hive -e  "alter table ${schema}.${table_name}  drop IF EXISTS partition(dt='tmp_${s_date}') "
	
		#refresh table 
		echo "impala-shell -q \"refresh ${schema}.${table_name}\""
		impala-shell -q "refresh ${schema}.${table_name}"
		
	else
		echo "Skip! Directory{$dt_path} file_num is $file_num,less then  2,don't need merch!!!"
	fi
else
        echo "Error! Directory{$dt_path} is not exist"
fi

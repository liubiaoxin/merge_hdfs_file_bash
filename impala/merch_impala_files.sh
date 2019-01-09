#!/bin/bash
script_path=/data/qding_etl/bash/merge_hdfs_file_bash/impala
schema=$1
table_name=$2
s_date=$3

echo "table======$schema.$table_name   s_date======$s_date"
#impala-shell -q "describe ${schema}.${table_name};" -B --output_delimiter ";"  -o  ${script_path}/fields_tmp.txt
#fix impala获取表字段时，注释中有空格，导致字段取出不对问题
impala-shell -q "describe ${schema}.${table_name};" -B --output_delimiter ";" | awk -F ';' '{print $1}' > ${script_path}/fields_tmp.txt


#get table's fields
for line in `cat ${script_path}/fields_tmp.txt`
do
   array=(${line//;/ })
   field="${array[0]}"
   echo "$field"
   if [[ $field = "location" ]];then
     field=\`${field}\`
   fi
   fields+=${field}","
done

echo "original fields===${fields}"

#original size
size=`expr length $fields`

if [[ $fields =~ "dt" ]];then
	#after except dt field's size
        echo "fields contains 'dt',substr string!!!"
	size=`expr $size - 4`
else
        echo "fields not contain 'dt',continue!!!"
fi

echo "fields's final size is  $size!!!"

fields=${fields:0:${size}}
echo "after deal fields:$fields"

dt_path=/qding/db/${schema}/${table_name}/dt=${s_date}

hadoop  fs -test -e  $dt_path
if [ $? -eq 0 ] ;then
        sql="set num_nodes=1;insert overwrite table ${schema}.${table_name} partition(dt = 'tmp_${s_date}') select  ${fields}  from ${schema}.${table_name}  where dt = '${s_date}';"
        echo "sql=======${sql}"
	#exec sql
	impala-shell -q "${sql}"
	
	#delete old partition data
	echo "sudo  -u hadoop fs -rmr -skipTrash  /qding/db/${schema}/${table_name}/dt=${s_date}"
	sudo  -u hdfs hadoop fs -rmr -skipTrash  /qding/db/${schema}/${table_name}/dt=${s_date}
	
	#mv tmp partition data to current partition
	echo "sudo  -u hadoop fs -mv /qding/db/${schema}/${table_name}/dt=tmp_${s_date}  /qding/db/${schema}/${table_name}/dt=${s_date}"
	sudo  -u hdfs hadoop fs -mv /qding/db/${schema}/${table_name}/dt=tmp_${s_date}  /qding/db/${schema}/${table_name}/dt=${s_date}
	
	#drop tmp partition
	echo "impala-shell -q \"alter table ${schema}.${table_name}  drop IF EXISTS partition(dt='tmp_${s_date}') \""
	impala-shell -q "alter table ${schema}.${table_name}  drop IF EXISTS partition(dt='tmp_${s_date}') "
	
	#refresh table
	echo "impala-shell -q \"refresh ${schema}.${table_name}\""
	impala-shell -q "refresh ${schema}.${table_name}"
	
else
        echo "Error! Directory{$dt_path} is not exist"
fi


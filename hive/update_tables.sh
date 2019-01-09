#!/bin/bash

script_path=/data/qding_etl/bash/merge_hdfs_file_bash


svn delete http://svn.qdingnet.com/svn/qd/trunk/platform2.5/qdbigdata/qding_data_analysis/dev/bash/merge_hdfs_file_bash/merch_tables.conf  -m "delete /dev/bash/merge_hdfs_file_bash/merch_tables.conf"
sh /data/qding_etl/update_svn.sh

#获取hive所有数据库
#schemas=$(hive -e "show databases;" | grep -v "WARN")

schemas=("qding_ads" "qding_dwp" "qding_mds")

#获取数组长度
echo ${#schemas[@]}

rm -rf $script_path/merch_tables.conf
#遍历数组
for schema  in ${schemas[@]}
do
   echo "schema======"$schema
   tables=$(hive -e "use ${schema};show tables" | grep -v "WARN")
   for table in $tables
   do
      if [[ $table = "ads_finance_ribao_summary" ]];then
         echo "Skip Impala table $schema.ads_finance_ribao_summary!!!"
      else 
         echo ${schema}","${table}>>${script_path}/merch_tables.conf
      fi
   done
done

#commit memch_tables.conf
echo "cd $script_path"
cd $script_path
svn resolved merch_tables.conf

svn add merch_tables.conf
svn commit -m "update ${script_path}/merch_tables.conf" merch_tables.conf


#update svn
sh /data/qding_etl/update_svn.sh

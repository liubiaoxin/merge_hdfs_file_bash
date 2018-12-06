set mapreduce.job.queuename=root.bigdata;
set hive.exec.compress.output=true;
set hive.intermediate.compression.codec=org.apache.Hadoop.io.compress.GzipCodec;
set hive.exec.reducers.max=${hiveconf:reduce_num};
set hive.support.quoted.identifiers=None;
insert overwrite table ${hiveconf:schema}.${hiveconf:table_name} partition(dt = 'tmp_${hiveconf:s_date}')
select `(dt)?+.+` from ${hiveconf:schema}.${hiveconf:table_name} where dt = '${hiveconf:s_date}' distribute by rand()

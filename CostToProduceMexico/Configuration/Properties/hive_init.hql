set hive.vectorized.execution.enabled = true;
set hive.exec.parallel=true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions = 5500;
set hive.exec.max.dynamic.partitions.pernode = 5500;
set hive.exec.max.created.files = 200000;
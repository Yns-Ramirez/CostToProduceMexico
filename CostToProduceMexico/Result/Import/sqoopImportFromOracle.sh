#!/bin/bash

# Work Directories 
BASEDIR=$(dirname $0)

#sqoop import --connect jdbc:oracle:thin:@jedoxdb.cbow4jr7c6fx.us-east-1.rds.amazonaws.com:1521/ORCL --username Jedox2016 --password Jedox2016! --username Jedox2016 --password Jedox2016! --table cp_data_detalle_barcel --hive-import --hive-table jedox.Cp_data_detalle_barcel --m 1 --input-fields-terminated-by '\001' --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N' --verbose

#sqoop import --connect jdbc:oracle:thin:@jedoxdb.cbow4jr7c6fx.us-east-1.rds.amazonaws.com:1521/ORCL --username Jedox2016 --password Jedox2016! --username Jedox2016 --password Jedox2016! --table cp_data_sumario_barcel --hive-import --hive-table jedox.Cp_data_sumario_barcel --m 1 --input-fields-terminated-by '\001' --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N' --verbose

#sqoop import --connect jdbc:oracle:thin:@jedoxdb.cbow4jr7c6fx.us-east-1.rds.amazonaws.com:1521/ORCL --username Jedox2016 --password Jedox2016! --username Jedox2016 --password Jedox2016! --table cp_data_piezas_barcel --hive-import --hive-table jedox.Cp_data_piezas_barcel --m 1 --input-fields-terminated-by '\001' --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N' --verbose

#sqoop import --connect jdbc:oracle:thin:@jedoxdb.cbow4jr7c6fx.us-east-1.rds.amazonaws.com:1521/ORCL --username Jedox2016 --password Jedox2016! --table cp_data_detalle_bmb --hive-import --hive-table jedox.Cp_data_detalle_bmb --m 1 --input-fields-terminated-by '\001' --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N' --verbose

#sqoop import --connect jdbc:oracle:thin:@jedoxdb.cbow4jr7c6fx.us-east-1.rds.amazonaws.com:1521/ORCL --username Jedox2016 --password Jedox2016! --username Jedox2016 --password Jedox2016! --table cp_data_sumario_bmb --hive-import --hive-table jedox.Cp_data_sumario_bmb --m 1 --input-fields-terminated-by '\001' --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N' --verbose

#sqoop import --connect jdbc:oracle:thin:@jedoxdb.cbow4jr7c6fx.us-east-1.rds.amazonaws.com:1521/ORCL --username Jedox2016 --password Jedox2016! --username Jedox2016 --password Jedox2016! --table cp_data_piezas_bmb --hive-import --hive-table jedox.Cp_data_piezas_bmb --m 1 --input-fields-terminated-by '\001' --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N' --verbose


# Read the host impala
HADOOP_ENV_PROPS=~/CostToProduceMexico/Configuration/Properties/hadoop_env.properties
. $HADOOP_ENV_PROPS 2>/dev/null
echo "impala host:  $impala_host"
echo "jdbc_url:  $jdbc_url"
echo "kerberos_user= $kerberos_user"
echo "kerberos_keytab= $kerberos_keytab"

# Creating connection with Beeline
BEELINE="beeline -u '$jdbc_url' -i $HOME/CostToProduceMexico/Configuration/Properties/hive_init.hql"
echo $BEELINE

# renew kerberos ticket
kinit -k -t $kerberos_keytab $kerberos_user@CLOUDERA

sqoop import --connect jdbc:oracle:thin:@jedoxdb.cbow4jr7c6fx.us-east-1.rds.amazonaws.com:1521/ORCL --username Jedox2016 --password Jedox2016! --username Jedox2016 --password Jedox2016! --table cp_data_detalle --hive-import --hive-overwrite --hive-table jedox.cp_data_detalle_tmp --m 1 --input-fields-terminated-by '\001' --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N' --delete-target-dir --verbose

sqoop import --connect jdbc:oracle:thin:@jedoxdb.cbow4jr7c6fx.us-east-1.rds.amazonaws.com:1521/ORCL --username Jedox2016 --password Jedox2016! --username Jedox2016 --password Jedox2016! --table cp_data_piezas --hive-import --hive-overwrite --hive-table jedox.cp_data_piezas_tmp --m 1 --input-fields-terminated-by '\001' --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N' --delete-target-dir --verbose

sqoop import --connect jdbc:oracle:thin:@jedoxdb.cbow4jr7c6fx.us-east-1.rds.amazonaws.com:1521/ORCL --username Jedox2016 --password Jedox2016! --username Jedox2016 --password Jedox2016! --table cp_data_sumario --hive-import --hive-overwrite --hive-table jedox.cp_data_sumario_tmp --m 1 --input-fields-terminated-by '\001' --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N' --delete-target-dir --verbose

$BEELINE -f $BASEDIR/insertIntoFinalTables.hql

impala-shell -i "$impala_host" -q "invalidate metadata;"


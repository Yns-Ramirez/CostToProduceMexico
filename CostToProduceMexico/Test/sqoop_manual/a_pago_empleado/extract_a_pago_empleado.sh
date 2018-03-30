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

$BEELINE -e "truncate table gb_mdl_mexico_erp.a_pago_empleado;"
$BEELINE -e "drop table gb_mdl_mexico_costoproducir_wrkt.a_pago_empleado;"

nohup sqoop import --driver com.teradata.jdbc.TeraDriver --connect 'jdbc:teradata://10.1.29.15/DATABASE=DWH' --username maguliar --password MA!@gu34 --query "select * from DWH.A_PAGO_EMPLEADO where extract(year from FechaPago)='2017' and extract(month from FechaPago) = '07' and extract(day from FechaPago) between '01' and '31' and \$CONDITIONS" --hive-import --hive-table gb_mdl_mexico_costoproducir_wrkt.a_pago_empleado --m 3 --split-by Concepto_ID --target-dir hdfs://HdfsHA/user/CP/ --delete-target-dir --input-fields-terminated-by '\001' --hive-drop-import-delims --input-lines-terminated-by '\n' --input-null-string '\\N' --input-null-non-string '\\N' --verbose &

wait 
$BEELINE -f ./query.hql

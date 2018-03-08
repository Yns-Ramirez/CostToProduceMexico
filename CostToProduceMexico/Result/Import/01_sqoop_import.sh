#!/bin/bash

countToImpalaTable(){
    # get row count from datalake table
    query="select count(*) from $table_impala;"
    row_count_impala=$($BEELINE_SIMPLE --showHeader=false --outputformat=csv2 -e "$query" --verbose=true)

    if [[ -z $row_count_impala ]] ; then
        echo "FAILED: The table $table_impala not exists or is empty, the row num is null <<$row_count_impala>>."
        exit 1
    fi
    if ! [[ $row_count_impala > 0 ]] ; then 
        echo "FAILED: The table $table_impala not exists or is empty, the row num is <<$row_count_impala>>."
        exit 1
    fi
    echo "***** Row num extracted from $table_impala is: <<$row_count_impala>>"
}

countToOracleTable(){
    # get row count from source table
    queryOracle="select count(*) from \"$table_oracle\""
    SQOOP_ORA="sqoop eval --connect jdbc:oracle:thin:@jedoxdb.cbow4jr7c6fx.us-east-1.rds.amazonaws.com:1521/ORCL --username Jedox2016 --password Jedox2016! --query '$queryOracle' --verbose"
    echo "*** running sqoop..."
    echo "$SQOOP_ORA"
    row_count_ora=$(eval $SQOOP_ORA |  awk '/([0-9]+)/{print $2}')

    if [[ -z $row_count_ora ]] ; then
        echo "FAILED: The table $table_oracle not exists or is empty, the row num is null <<$row_count_ora>>."
        exit 1
    fi
    if ! [[ $row_count_ora > 0 ]] ; then 
        echo "FAILED: The table $table_oracle not exists or is empty, the row num is <<$row_count_ora>>."
        exit 1
    fi
    echo "***** Row num extracted from $table_oracle is: <<$row_count_ora>>"
}

ValRownumLakeVsOracle(){
    # Validating that the number register extracted are the same that the source.
    if ! [[ $row_count_ora == $row_count_impala ]] ; then 
        echo "FAILED: The number of rows extracted are not the same that source, $table_impala <<$row_count_impala>> $table_oracle <<$row_count_ora>>"
        exit 1
    else
        echo "SUCCESS: The number of rows extracted are the same that source, $table_impala <<$row_count_impala>> $table_oracle <<$row_count_ora>>"
    fi
}

importTable(){
    oracle_table=$1
    hive_table=$2
    table_oracle="$oracle_table"
    table_impala="$hive_db.$hive_table"
    outputDirectory="/user/"$userHome"/oracletmp/JEDOXMX/$hive_table"
    

    
    SQOOP="sqoop import --connect jdbc:oracle:thin:@jedoxdb.cbow4jr7c6fx.us-east-1.rds.amazonaws.com:1521/ORCL --username Jedox2016 --password Jedox2016! --table $oracle_table --hive-import --hive-overwrite --hive-table $table_impala --m 1 --input-fields-terminated-by '\001' --input-lines-terminated-by '\\n' --input-null-string '\\\\N' --input-null-non-string '\\\\N' --delete-target-dir --verbose"
#    SQOOP="sqoop import --connect jdbc:oracle:thin:@jedoxdb.cbow4jr7c6fx.us-east-1.rds.amazonaws.com:1521/ORCL --username Jedox2016 --password Jedox2016! --query --hive-import --hive-overwrite --hive-database $hive_db --hive-table $hive_table --m 1 --input-fields-terminated-by '\001' --input-lines-terminated-by '\\n' --null-string '\\\\N' --null-non-string '\\\\N' --target-dir $outputDirectory --delete-target-dir --verbose"

    echo "*** running sqoop..."
    echo "$SQOOP"
    if ! eval "$SQOOP" ; then
        echo "FAILED: The 'Import Table $table_oracle' process has failed."
        exit 1
    fi

    #exec functions of validation.
    countToImpalaTable
    countToOracleTable
    ValRownumLakeVsOracle
}


# initialize clients 
HADOOP_ENV_PROPS=$HOME/CostToProduceMexico/Configuration/Properties/hadoop_env.properties
HIVE_INIT=$HOME/CostToProduceMexico/Configuration/Properties/hive_init.hql

. $HADOOP_ENV_PROPS 2>/dev/null
echo "impala host:  $impala_host"
echo "jdbc_url:  $jdbc_url"
echo "kerberos_user= $kerberos_user"
echo "kerberos_keytab= $kerberos_keytab"

BEELINE="beeline -u '$jdbc_url' -i $HIVE_INIT"
BEELINE_SIMPLE="beeline -u '$jdbc_url'"
IMPALA="impala-shell -i $impala_host"

# variables
hive_db="gb_smntc_mexico_costoproducir"
userHome=`echo $HOME | awk -F'/' '{print $3}'`

tableOracle="cp_data_detalle"
tableLake="cp_data_detalle_last_exec"
importTable $tableOracle $tableLake

tableOracle="cp_data_piezas"
tableLake="cp_data_piezas_last_exec"
importTable $tableOracle $tableLake

tableOracle="cp_data_sumario"
tableLake="cp_data_sumario_last_exec"
importTable $tableOracle $tableLake



echo "SUCCESS: The 'import_cp_data' process has finished correctly."
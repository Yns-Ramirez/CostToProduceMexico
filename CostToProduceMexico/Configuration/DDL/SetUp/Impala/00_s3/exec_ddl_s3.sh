#!/bin/bash

# Work Directories 
BASEDIR=$(dirname $0)
INPUT_FILES_DIRECTORY=$BASEDIR/input_files

# initialize clients 
HADOOP_ENV_PROPS=$HOME/CostToProduceMexico/Configuration/Properties/hadoop_env.properties
BUCKETS_S3_PROPS=$HOME/CostToProduceMexico/Configuration/Properties/buckets_s3.properties
HIVE_INIT=$HOME/CostToProduceMexico/Configuration/Properties/hive_init.hql

. $HADOOP_ENV_PROPS 2>/dev/null
. $BUCKETS_S3_PROPS 2>/dev/null
echo "impala host:  $impala_host"
echo "jdbc_url:  $jdbc_url"
echo "kerberos_user= $kerberos_user"
echo "kerberos_keytab= $kerberos_keytab"
echo "buckets3_warehouse: $buckets3_warehouse"
echo "bucket_mexico_costoproducir_source: $bucket_mexico_costoproducir_source"

BEELINE="beeline -u '$jdbc_url' -i $HIVE_INIT"
BEELINE_SIMPLE="beeline -u '$jdbc_url'"
IMPALA="impala-shell -i $impala_host"

echo $BEELINE
echo $BEELINE_SIMPLE
echo $IMPALA


# Run input files
for INPUT_FILE in $INPUT_FILES_DIRECTORY/*.*
do 
    echo *****$INPUT_FILE

    if [[ $INPUT_FILE == *.hql ]]; then
        if ! $BEELINE -f $INPUT_FILE
            then echo "FAILED: Error to run the HQL file on hive $INPUT_FILE"
            exit 1;
        fi

    elif [[ $INPUT_FILE == *.sql ]]; then
        if ! $IMPALA --var=bucket_mexico_costoproducir_source=$bucket_mexico_costoproducir_source -f $INPUT_FILE
            then echo "FAILED: Error to run the SQL file on impala $INPUT_FILE"
            exit 1;
        fi

    elif [[ $INPUT_FILE == *.sh ]]; then
        if ! $INPUT_FILE $LEGALENTITY_ID; then
            echo "FAILED: Error to run the Shell $INPUT_FILE"
            exit 1;
        fi

    else
        echo "Command not valid!";
    fi
done

echo "SUCCESS: The create objects IMPALA in S3 has finished correctly."
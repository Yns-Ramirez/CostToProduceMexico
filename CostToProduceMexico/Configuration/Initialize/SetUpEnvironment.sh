#!/bin/bash

# Work directories
BASEDIR=$(dirname $0)
PATH_DDL_HIVE=$BASEDIR/../DDL/SetUp/Hive/*/*.hql
PATH_HQL_IMPALA=$BASEDIR/../DDL/SetUp/Impala/*/*.hql

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

# Variables
FLAG_SUCCESS=true

echo "========== Initializing 'Cost To Produce' Project =========="

# ========== CREATION HIVE OBJECTS ==========

for HQL in $PATH_DDL_HIVE
do 
    echo *****$HQL
    
    if [[ $HQL == *.hql ]]; then
        

        if $BEELINE -f $HQL
            then FLAG_SUCCESS=true
        else 
            echo "Execution Failed: $HQL"
            FLAG_SUCCESS=false
            break;
        fi

    else
        echo "Command not valid!";
    fi
done

# ========== CREATION IMPALA OBJECTS ==========

for HQL in $PATH_HQL_IMPALA
do 
    if ! $FLAG_SUCCESS; then
        break;
    fi
    echo *****$HQL
    
    if [[ $HQL == *.hql ]]; then
        

        if impala-shell -i "$impala_host" -f $HQL
            then EXECUTION_RESULT='SUCCESS'
        else 
            EXECUTION_RESULT='FAILED'
            echo "Execution Failed: $HQL"
            break;
        fi

    else
        echo "Command not valid!";
    fi
done


if $FLAG_SUCCESS; then
    echo "The process finished with status: SUCCESS"
else 
    echo "The process finished with status: FAILED"
fi

echo "========== Setup Environment Finished =========="
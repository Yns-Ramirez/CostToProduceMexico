#!/bin/bash

# Work Directories 
BASEDIR=$(dirname $0)
HQL_SHELL_DIRECTORY=$BASEDIR/hql_shell


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

# Get arguments
PERIOD_ARG=$1

# ========== CP_MEDIDAS_PRORRATEO PROCESS ==========

for HQL_SHELL in $HQL_SHELL_DIRECTORY/*.*
do 
    echo *****$HQL_SHELL
    
    if [[ $HQL_SHELL == *.hql ]]; then
        HQL_FILE=${HQL_SHELL##*/}
        
        if $BEELINE -hiveconf speriod=$PERIOD_ARG -f $HQL_SHELL
            then EXECUTION_RESULT='SUCCESS'
        else 
            EXECUTION_RESULT='FAILED'
            exit 1;
        fi

    elif [[ $HQL_SHELL == *.sh ]]; then

        if ! $HQL_SHELL $PERIOD_ARG $TYPE_EXEC_ARG $TYPE_PROCESS_ARG; then
          exit 1;
        fi

    else
        echo "Command not valid!";
    fi
done

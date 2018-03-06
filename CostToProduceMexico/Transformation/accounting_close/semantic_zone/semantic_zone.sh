#!/bin/bash

# Work Directories 
BASEDIR=$(dirname $0)
HQL_SHELL_DIRECTORY=$BASEDIR/hql_shell


# Variables
FLAG_SUCCESS=true

# Read the host impala
HADOOP_ENV_PROPS=$HOME/CostToProduceMexico/Configuration/Properties/hadoop_env.properties
. $HADOOP_ENV_PROPS 2>/dev/null
echo "impala host:  $impala_host"
echo "jdbc_url:  $jdbc_url"


# Creating connection with Beeline
BEELINE="beeline -u '$jdbc_url' "
echo $BEELINE

CONFIG=" -i $HOME/CostToProduceMexico/Configuration/Properties/hive_init.hql"
echo $CONFIG


# Get arguments
PERIOD_ARG=$1
LEGAL_ENTITIES_DEFAULT=$2

# Calcula las fecha inicio y fin de periodo segun el parametro lanzado
PERIODO_ANTERIOR=$PERIOD_ARG
echo "******** PERIODO ANTERIOR: $PERIODO_ANTERIOR ***********"

PERIODO_PRIMER_DIA=`$BEELINE --silent=true --outputformat=csv2 --showHeader=false -e "select concat('$PERIODO_ANTERIOR','-01');" | grep -v '0: jdbc'`
echo "******** PRIMER DIA DEL PERIODO: $PERIODO_PRIMER_DIA ***********"

PERIODO_ULTIMO_DIA=`$BEELINE --silent=true --outputformat=csv2 --showHeader=false -e "select last_day(concat('$PERIODO_ANTERIOR','-01'));" | grep -v '0: jdbc'`
echo "******** ULTIMO DIA DEL PERIODOD: $PERIODO_ULTIMO_DIA ***********"


# ========== APP_COSTOPRODUCIR PROCESS ==========


for HQL_SHELL in $HQL_SHELL_DIRECTORY/*.*
do 
    echo *****$HQL_SHELL
    
    if [[ $HQL_SHELL == *.hql ]]; then
        HQL_FILE=${HQL_SHELL##*/}

        if $BEELINE --hiveconf entidadLegal=$LEGAL_ENTITIES_DEFAULT --hiveconf fechainicioperiodo=$PERIODO_PRIMER_DIA --hiveconf fechafinperiodo=$PERIODO_ULTIMO_DIA $CONFIG -f $HQL_SHELL
            then EXECUTION_RESULT='SUCCESS'
        else 
            EXECUTION_RESULT='FAILED'
            exit 1;
        fi

    else
        echo "Command not valid!";
    fi
done


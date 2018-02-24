#!/bin/bash

# WORK DIRECTORIES 
BASEDIR=$(dirname $0)


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

INSTRUCTION_ARRAY=(
    $BASEDIR/hql/mf_lineas_prod.hql
    )


for PROCESS in ${INSTRUCTION_ARRAY[@]}
do 
    echo *****$PROCESS
    
    if [[ $PROCESS == *.hql ]]; then
        HQL_FILE=${PROCESS##*/}
        
        if $BEELINE -f $PROCESS
            then EXECUTION_RESULT='SUCCESS'
        else 
            EXECUTION_RESULT='FAILED'
            exit 1;
        fi
        
    elif [[ $PROCESS == *.sh ]]; then
        $PROCESS
    else
        echo "Command not valid!";
    fi

done

HQL_FILE="mf_turnos_procedure"


echo *****"mf_turnos_procedure_step1.hql"
if ! $BEELINE -f $BASEDIR/hql/mf_turnos_procedure_step1.hql
    exit 1;
fi

# echo *****"initialize_loop.hql"
# if ! $BEELINE  --silent=true --outputformat=csv2 -f $BASEDIR/hql/initialize_loop.hql > $BASEDIR/data/turnos_rotativos.txt
#     then FLAG_DONE=false
# fi

# echo *****"mf_turnos_procedure_step2.hql"
# for line in $(cat $BASEDIR/data/turnos_rotativos.txt | grep -v 'null'); 
# do 
#     echo $line
#     if ! $BEELINE -hiveconf orden_turno=$line -f $BASEDIR/hql/mf_turnos_procedure_step2.hql
#         then FLAG_DONE=false
#     fi
# done

echo *****"mf_turnos_procedure_step3.hql"
if ! $BEELINE -f $BASEDIR/hql/mf_turnos_procedure_step3.hql
    exit 1;
fi


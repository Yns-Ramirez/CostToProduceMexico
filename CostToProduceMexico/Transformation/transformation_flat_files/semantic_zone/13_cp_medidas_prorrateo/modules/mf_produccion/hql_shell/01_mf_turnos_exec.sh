#!/bin/bash

# Work directories
BASEDIR=$(dirname $0)
SHELL_MODULE=$BASEDIR/../modules/mf_turnos

# Variables
FLAG_SUCCESS=true

# Get arguments
PERIOD_ARG=$1
TYPE_EXEC_ARG=$2
TYPE_PROCESS_ARG=$3

# Execute shell
for SHELL in $SHELL_MODULE/*.sh 
do 
    echo *****$SHELL
    if ! $SHELL $PERIOD_ARG $TYPE_EXEC_ARG $TYPE_PROCESS_ARG; then
      FLAG_SUCCESS=false;
      break;
    fi
done

if $FLAG_SUCCESS; then 
    exit 0;
else
    exit 1;
fi
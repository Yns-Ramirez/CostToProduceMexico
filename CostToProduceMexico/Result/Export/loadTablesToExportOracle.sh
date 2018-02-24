#!/bin/bash

BASEDIR=$(dirname $0)
HADOOP_ENV_PROPS=~/CostToProduceMexico/Configuration/Properties/hadoop_env.properties
INSERT_DB_JEDOX_FROM_DATA_LAKE=$BASEDIR/../Export/parameters_script.hql
echo "$INSERT_DB_JEDOX_FROM_DATA_LAKE"

SQOOP_TRUNCATE_ORACLE_TABLES=$BASEDIR/../Export/sqoopTruncateOracleJedoxTables.sh
echo "$SQOOP_TRUNCATE_ORACLE_TABLES"

SQOOP_EXPORT_ORACLE=$BASEDIR/../Export/sqoopExportOracle.sh
echo "$SQOOP_EXPORT_ORACLE"

# Querys for stats.
GET_LAST_STATUS_BY_PROCESS_QUERY=$BASEDIR/../../Transformation/process/hql_utils/get_last_status_by_process.hql

# Constants
SUCCESS="SUCCESS"
NA="NA"
TYPE_EXEC_TRANS_FLAT_FILES="transformation_flat_files"

processArgs() {
    usage="--entidadLegal name --fechaInicio name --fechaFin name --periodo name --periodo2 name --anio name --mes name"
    while (( "$#" )); do 
    if [ $1 = "--entidadLegal" ] ; then
        shift ; entidadLegal=$1 ; shift ;
    elif [ $1 = "--fechaInicio" ] ; then
        shift ; fechaInicio=$1 ; shift ;
    elif [ $1 = "--fechaFin" ] ; then
                shift ; fechaFin=$1 ; shift ;
    elif [ $1 = "--periodo" ] ; then
                shift ; periodo=$1 ; shift ;
    elif [ $1 = "--periodo2" ] ; then
                shift ; periodo2=$1 ; shift ;
    elif [ $1 = "--anio" ] ; then
                shift ; anio=$1 ; shift ;
    elif [ $1 = "--mes" ] ; then
                shift ; mes=$1 ; shift ;
    else
        echo "invalid argument: " $1
        echo "$0: $usage"
        exit 1
    fi

    done

    if [ -z $entidadLegal ] || [ -z $fechaInicio ] || [ -z $fechaFin ] || [ -z $periodo ] || [ -z $periodo2 ] || [ -z $anio ] || [ -z $mes ] ; then
        echo "$0 : bad/insufficent arguments"
        echo "$0 : $usage"
        echo "entidadLegal = $entidadLegal"
        echo "fechaInicio = $fechaInicio"
        echo "fechaFin = $fechaFin"
        echo "periodo = $periodo"
        echo "periodo2 = $periodo2"
        echo "anio = $anio"
        echo "mes = $mes"
        exit 1
    fi
}

processArgs $*

echo "-----------------------------------------------------"
echo "--------------------PARAMETROS-----------------------"
echo "-----------------------------------------------------"

echo "  Entidad(es) Legales: $entidadLegal "
echo "  Fecha Inicio Periodo: $fechaInicio "
echo "  Fecha Fin Periodo: $fechaFin "
echo "  Periodo(YYYY-MM): $periodo "
echo "  Periodo2(MES-YY): $periodo2 "
echo "  Anio: $anio "
echo "  Mes: $mes "

. $HADOOP_ENV_PROPS 2>/dev/null
echo "impala host:  $impala_host"
echo "kerberos_user= $kerberos_user"
echo "kerberos_keytab= $kerberos_keytab"

# renew kerberos ticket
kinit -k -t $kerberos_keytab $kerberos_user@CLOUDERA

STATUS_LAST_PROCESS=`impala-shell -i "$impala_host" -f "$GET_LAST_STATUS_BY_PROCESS_QUERY" --var=type_exec=$TYPE_EXEC_TRANS_FLAT_FILES --var=type_process="$NA" -B`
if [ "$STATUS_LAST_PROCESS" != "$SUCCESS" ]; then
    echo "The before process 'transformation flat files' has finished with status failed."
    echo ">>> end process";
    exit 0;
fi

echo "starting querys final and export results"
if impala-shell -i "$impala_host" -f "$INSERT_DB_JEDOX_FROM_DATA_LAKE" --var=VAR_EL="$entidadLegal" --var=VAR_FECHA_INICIO="$fechaInicio" --var=VAR_FECHA_FIN="$fechaFin" --var=VAR_PERIODO="$periodo" --var=VAR_PERIODO2="$periodo2" --var=VAR_ANIO="$anio" --var=VAR_MES="$mes"
     then
          echo "------- Borrar informacion en las tablas destino en Oracle BD ------------"
          #$SQOOP_TRUNCATE_ORACLE_TABLES
          echo "------- Ejecutar export a las tablas destino en Oracle BD ------------"
          $SQOOP_EXPORT_ORACLE
else
     echo "Falló al generar tablas en impala"
fi


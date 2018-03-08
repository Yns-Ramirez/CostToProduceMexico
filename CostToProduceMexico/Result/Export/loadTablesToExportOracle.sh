#Example sh ./loadTablesToExportOracle.sh --periodo 2017-07 --periodo2 JUL-2017

#!/bin/bash
BASEDIR=$(dirname $0)
# Read the host impala
HADOOP_ENV_PROPS=~/CostToProduceMexico/Configuration/Properties/hadoop_env.properties
. $HADOOP_ENV_PROPS 2>/dev/null
echo "impala host:  $impala_host"
echo "jdbc_url:  $jdbc_url"

# Creating connection with Beeline
BEELINE="beeline -u '$jdbc_url' "
echo $BEELINE

INSERT_DB_JEDOX_FROM_DATA_LAKE=$BASEDIR/parameters_script.hql
echo "$INSERT_DB_JEDOX_FROM_DATA_LAKE"


processArgs() {
  usage=" --periodo name --periodo2 name "
  while (( "$#" )); do 
  if [ $1 = "--periodo" ] ; then
    shift ; periodo=$1 ; shift ;
  elif [ $1 = "--periodo2" ] ; then
    shift ; periodo2=$1 ; shift ;
  else
    echo "invalid argument: " $1
    echo "$0: $usage"
    exit 1
  fi

  done

  if [ -z $periodo ] || [ -z $periodo2 ] ; then
    echo "$0 : bad/insufficent arguments"
    echo "$0 : $usage"
    echo "periodo = $periodo"
    echo "periodo2 = $periodo2"
    exit 1
  fi
}

processArgs $*


PERIODO_ANTERIOR=$periodo
echo "******** PERIODO ANTERIOR: $PERIODO_ANTERIOR ***********"

PERIODO_PRIMER_DIA=`$BEELINE --silent=true --outputformat=csv2 --showHeader=false -e "select concat('$PERIODO_ANTERIOR','-01');" | grep -v '0: jdbc'`
echo "******** PRIMER DIA DEL PERIODO: $PERIODO_PRIMER_DIA ***********"

PERIODO_ULTIMO_DIA=`$BEELINE --silent=true --outputformat=csv2 --showHeader=false -e "select last_day(concat('$PERIODO_ANTERIOR','-01'));" | grep -v '0: jdbc'`
echo "******** ULTIMO DIA DEL PERIODOD: $PERIODO_ULTIMO_DIA ***********"

ANIO=${periodo:0:4}
echo "******** ANIO PERIODO: $ANIO ***********"

MES=${periodo:5:2}
echo "******** MES PERIODO: $MES ***********"

NOMBREPER=$periodo2

echo "********** NOMBRE PERIODO: $NOMBREPER ***********"



echo "-----------------------------------------------------"
echo "--------------------PARAMETROS-----------------------"
echo "-----------------------------------------------------"

echo "  fechaInicio = $PERIODO_PRIMER_DIA"
echo "  fechaFin = $PERIODO_ULTIMO_DIA"
echo "  Periodo(YYYY-MM): $periodo "
echo "  Periodo2(MES-YYYY): $periodo2 "
echo "  Anio(YYYY): $ANIO"
echo "  Mes(MM): $MES"
    

if impala-shell -i "$impala_host" -f "$INSERT_DB_JEDOX_FROM_DATA_LAKE" --var=VAR_FECHA_INICIO="$PERIODO_PRIMER_DIA" --var=VAR_FECHA_FIN="$PERIODO_ULTIMO_DIA" --var=VAR_PERIODO="$PERIODO_ANTERIOR" --var=VAR_PERIODO2="$NOMBREPER" --var=VAR_ANIO="$ANIO" --var=VAR_MES="$MES"
     then
          echo "------- Carga de tablas a Impala completada ------------"
else
     echo ">>>> Falló al generar tablas en impala"
     exit 1;
fi



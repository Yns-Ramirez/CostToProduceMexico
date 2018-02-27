
drop table gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION; 

CREATE TABLE gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION 
(E_Empleado_ID STRING,
E_Organizacion_ID STRING,
E_NumeroPeriodo DECIMAL(2,0),
E_FechaInicioPeriodo STRING,
E_FechaFinPeriodo STRING,
E_Posicion_ID INT,
E_FechaInicioPosicion STRING,
E_FechaFinPosicion STRING,
E_Rol_ID DECIMAL(4,0),
E_NumeroColaboradorInterno STRING,
E_TipoColaborador_ID STRING,
E_MotivoBaja_ID STRING,
E_FechaAntiguedad STRING,
P_Posicion_ID STRING,
P_FechaInicio STRING,
P_FechaFin STRING,
P_NombrePosicion STRING,
P_Puesto_ID STRING,
P_Plan_ID STRING,
P_EntidadLegal_ID STRING,
P_AreaNegocio_ID STRING,
P_UnidadTrabajo_ID STRING,
P_Categoria_ID STRING,
P_Nivel_ID STRING,
P_CentroTrabajo_ID STRING,
P_CentroCostos_ID STRING,
P_Convenio_ID STRING,
P_HorasXsemana DECIMAL(5,2),
P_TiempoCompleto STRING,
Sistema_Fuente STRING,
storeday STRING);


--SET Paso = 2;

INSERT INTO gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION
SELECT
PER.STD_ID_HR AS Empleado_ID
,PER.ID_ORGANIZATION AS Organizacion_ID
,PER.STD_OR_HR_PERIOD AS NumeroPeriodo
,cast(to_date(from_unixtime(UNIX_TIMESTAMP(PER.STD_DT_START))) as string) AS FechaInicioPeriodo
,cast(to_date(from_unixtime(UNIX_TIMESTAMP(PER.STD_DT_END))) as string) AS FechaFinPeriodo
,CAST(ROW_NUMBER() OVER (ORDER BY PER.STD_ID_HR, PER.ID_ORGANIZATION, HPOS.SCO_ID_POSITION) AS INT) AS E_Posicion_ID
,cast(to_date(from_unixtime(UNIX_TIMESTAMP(HPOS.SCO_DT_START))) as string) AS FechaInicioPosicion 
,cast(to_date(from_unixtime(UNIX_TIMESTAMP(HPOS.SCO_DT_END))) as string) AS FechaFinPosicion 
,ROL.SCO_OR_HR_ROLE AS Rol_ID
,PER.SSP_NUM_MATRICULA AS NumeroColaboradorInterno
,PER.STD_ID_HR_TYPE AS TipoColaborador_ID
,PER.STD_ID_HRP_END_REA AS MotivoBaja_ID
,cast(to_date(from_unixtime(UNIX_TIMESTAMP(PER.SSP_FEC_ANTIGUEDAD))) as string) AS FechaAntiguedad
,HPOS.SCO_ID_POSITION AS P_Posicion_ID
,cast(to_date(from_unixtime(UNIX_TIMESTAMP(HPOS.SCO_DT_START))) as string) AS FechaInicio 
,cast(to_date(from_unixtime(UNIX_TIMESTAMP(HPOS.SCO_DT_END))) as string) AS FechaFin 
,COALESCE (POS.SCO_NM_POSITIONESP, POS.SCO_NM_POSITIONENG,'S/I') AS NombrePosicion                
,POS.SCO_ID_JOB_CODE AS Puesto_ID
,NULL AS Plan_ID
,EL.STD_ID_LEG_ENT AS EntidadLegal_ID
,NULL AS AreaNegocio_ID 
,NULL AS UnidadTrabajo_ID 
,NULL AS Categoria_ID
,NULL AS Nivel_ID
,NULL AS CentroTrabajo_ID 
,NULL AS CentroCostos_ID
,NULL AS Convenio_ID 
,NULL AS HorasXsemana
,NULL AS TiempoCompleto 
,CAST('Peoplenet V7' AS STRING)
,FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM erp_mexico_sz.STD_HR_PERIOD PER
INNER JOIN erp_mexico_sz.M4SCO_HR_ROLE ROL ON  
PER.ID_ORGANIZATION=ROL.ID_ORGANIZATION
AND PER.STD_ID_HR=ROL.SCO_ID_HR
AND PER.STD_OR_HR_PERIOD=ROL.SCO_OR_HR_PER
INNER JOIN erp_mexico_sz.M4SCO_H_HR_LEGENT EL ON 
PER.ID_ORGANIZATION=EL.ID_ORGANIZATION
AND PER.STD_ID_HR=EL.STD_ID_HR
AND PER.STD_OR_HR_PERIOD = EL.STD_OR_HR_PERIOD
INNER JOIN erp_mexico_sz.M4SCO_H_HR_POS HPOS ON
ROL.ID_ORGANIZATION = HPOS.ID_ORGANIZATION
AND ROL.SCO_ID_HR = HPOS.SCO_ID_HR 
AND ROL.SCO_OR_HR_ROLE = HPOS.SCO_OR_HR_ROLE
INNER JOIN erp_mexico_sz.M4SCO_POSITION POS ON 
HPOS.SCO_ID_POSITION  = POS.SCO_ID_POSITION
AND HPOS.ID_ORGANIZATION = POS.ID_ORGANIZATION
INNER JOIN 
(
SELECT HIST.ID_ORGANIZATION, HIST.STD_ID_JOB_CODE
FROM erp_mexico_sz.STD_HT_JOB_DEF HIST
INNER JOIN (
SELECT ID_ORGANIZATION, STD_ID_JOB_CODE,MAX(STD_DT_END) AS STD_DT_END
FROM erp_mexico_sz.STD_HT_JOB_DEF
GROUP BY ID_ORGANIZATION, STD_ID_JOB_CODE
) H
ON H.ID_ORGANIZATION = HIST.ID_ORGANIZATION 
AND H.STD_ID_JOB_CODE = HIST.STD_ID_JOB_CODE 
AND H.STD_DT_END = HIST.STD_DT_END
)HIST 
ON POS.SCO_ID_JOB_CODE  = HIST.STD_ID_JOB_CODE  
INNER JOIN erp_mexico_sz.STD_LEG_ENT L 
ON L.STD_ID_LEG_ENT = EL.STD_ID_LEG_ENT
AND L.ID_ORGANIZATION = HIST.ID_ORGANIZATION
WHERE PER.STD_ID_HR_TYPE = '01' 
AND PER.ID_ORGANIZATION IN (SELECT EntidadLegal_ID_DWH 
FROM gb_mdl_mexico_costoproducir.GX_CONTROL_EL_PEOPLENET 
WHERE Cadena = 'Peoplenet V7' 
GROUP BY EntidadLegal_ID_DWH);


--SET Paso = 3; 

INSERT OVERWRITE TABLE gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION SELECT tmp.* from
(SELECT WEP.E_Empleado_ID,WEP.E_Organizacion_ID,WEP.E_NumeroPeriodo,
WEP.E_FechaInicioPeriodo,WEP.E_FechaFinPeriodo,WEP.E_Posicion_ID,
WEP.E_FechaInicioPosicion,WEP.E_FechaFinPosicion,WEP.E_Rol_ID,
WEP.E_NumeroColaboradorInterno,WEP.E_TipoColaborador_ID,
WEP.E_MotivoBaja_ID,WEP.E_FechaAntiguedad,WEP.P_Posicion_ID,
WEP.P_FechaInicio,WEP.P_FechaFin,WEP.P_NombrePosicion,
WEP.P_Puesto_ID,WEP.P_Plan_ID,WEP.P_EntidadLegal_ID,COALESCE(AN.AreaNegocio_ID,null) as AreaNegocio_ID,
WEP.P_UnidadTrabajo_ID,WEP.P_Categoria_ID,WEP.P_Nivel_ID,
WEP.P_CentroTrabajo_ID,WEP.P_CentroCostos_ID,WEP.P_Convenio_ID,
WEP.P_HorasXsemana,WEP.P_TiempoCompleto,WEP.Sistema_Fuente,WEP.storeday
FROM (SELECT
PER.STD_ID_HR AS Empleado_ID 
,PER.ID_ORGANIZATION AS Organizacion_ID 
,PER.STD_OR_HR_PERIOD AS NumeroPeriodo
,HPOS.SCO_ID_POSITION AS Posicion_ID
,ROL.SCO_OR_HR_ROLE AS Rol_ID 
,SUBSTR(LT1.STD_ID_WL_PARENT,5,4) AS AreaNegocio_ID
FROM erp_mexico_sz.STD_HR_PERIOD PER
INNER JOIN erp_mexico_sz.M4SCO_H_HR_LEGENT EL ON 
PER.ID_ORGANIZATION  = EL.ID_ORGANIZATION
AND PER.STD_ID_HR  = EL.STD_ID_HR
AND PER.STD_OR_HR_PERIOD = EL.STD_OR_HR_PERIOD
INNER JOIN erp_mexico_sz.M4SCO_HR_ROLE ROL ON  
PER.ID_ORGANIZATION = ROL.ID_ORGANIZATION
AND PER.STD_ID_HR = ROL.SCO_ID_HR
AND PER.STD_OR_HR_PERIOD = ROL.SCO_OR_HR_PER
INNER JOIN
(
SELECT WL.ID_ORGANIZATION, WL.SCO_ID_HR, WL.SCO_OR_HR_ROLE, WL.SCO_ID_WORK_LOC, WL.SCO_DT_START, WL.SCO_DT_END
FROM erp_mexico_sz.M4SCO_H_HR_WLOC WL
INNER JOIN 
(
SELECT ID_ORGANIZATION, SCO_ID_HR, SCO_OR_HR_ROLE, MAX(SCO_DT_END) AS SCO_DT_END
FROM erp_mexico_sz.M4SCO_H_HR_WLOC  
GROUP BY ID_ORGANIZATION, SCO_ID_HR, SCO_OR_HR_ROLE
) WLM ON WL.ID_ORGANIZATION = WLM.ID_ORGANIZATION 
AND WL.SCO_ID_HR = WLM.SCO_ID_HR 
AND WL.SCO_OR_HR_ROLE = WLM.SCO_OR_HR_ROLE  
AND WL.SCO_DT_END = WLM.SCO_DT_END
) WL ON
ROL.ID_ORGANIZATION  = WL.ID_ORGANIZATION
AND ROL.SCO_ID_HR = WL.SCO_ID_HR
AND ROL.SCO_OR_HR_ROLE = WL.SCO_OR_HR_ROLE 
INNER JOIN 
( SELECT LT1.ID_ORGANIZATION, LT1.STD_ID_WL_CHILD, LT1.STD_ID_WL_PARENT, LT1.STD_DT_START, LT1.STD_DT_END, max(srwl.STD_DT_END)
FROM erp_mexico_sz.std_rel_work_loc LT1,erp_mexico_sz.std_rel_work_loc srwl
WHERE LT1.ID_ORGANIZATION=srwl.ID_ORGANIZATION
AND LT1.STD_ID_WL_CHILD=srwl.STD_ID_WL_CHILD
AND LT1.STD_DT_END=srwl.STD_DT_END
GROUP BY LT1.ID_ORGANIZATION, LT1.STD_ID_WL_CHILD, LT1.STD_ID_WL_PARENT, LT1.STD_DT_START, LT1.STD_DT_END
) LT1 ON  
WL.ID_ORGANIZATION  = LT1.ID_ORGANIZATION
AND WL.SCO_ID_WORK_LOC  = LT1.STD_ID_WL_CHILD
INNER JOIN erp_mexico_sz.M4SCO_H_HR_POS HPOS ON
ROL.ID_ORGANIZATION  = HPOS.ID_ORGANIZATION
AND ROL.SCO_ID_HR = HPOS.SCO_ID_HR 
AND ROL.SCO_OR_HR_ROLE = HPOS.SCO_OR_HR_ROLE
WHERE PER.STD_ID_HR_TYPE = '01'
AND CAST(PER.ID_ORGANIZATION AS INT) IN
(SELECT EntidadLegal_ID_PNET FROM gb_mdl_mexico_costoproducir.GX_CONTROL_EL_PEOPLENET WHERE Cadena = 'Peoplenet V7' GROUP BY EntidadLegal_ID_PNET)
GROUP BY PER.STD_ID_HR,PER.ID_ORGANIZATION,PER.STD_OR_HR_PERIOD,HPOS.SCO_ID_POSITION,ROL.SCO_OR_HR_ROLE,LT1.STD_ID_WL_PARENT
) AN, gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION WEP
WHERE WEP.E_Empleado_ID=AN.Empleado_ID 
AND WEP.E_Organizacion_ID=AN.Organizacion_ID 
AND WEP.E_NumeroPeriodo=AN.NumeroPeriodo 
AND WEP.E_Rol_ID=AN.Rol_ID 
AND WEP.P_Posicion_ID=AN.Posicion_ID) tmp
join (SELECT wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID,max(wrke.storeday) as first_record 
from gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION wrke 
group by wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID) sec 
on tmp.E_Empleado_ID=sec.E_Empleado_ID
and tmp.E_NumeroPeriodo=sec.E_NumeroPeriodo
and tmp.E_FechaInicioPeriodo=sec.E_FechaInicioPeriodo
and tmp.E_Posicion_ID=sec.E_Posicion_ID
and tmp.storeday=sec.first_record;


--SET Paso = 4;

INSERT OVERWRITE TABLE gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION SELECT tmp.* 
from (SELECT WEP.E_Empleado_ID,WEP.E_Organizacion_ID,WEP.E_NumeroPeriodo
,WEP.E_FechaInicioPeriodo,WEP.E_FechaFinPeriodo,WEP.E_Posicion_ID
,WEP.E_FechaInicioPosicion,WEP.E_FechaFinPosicion,WEP.E_Rol_ID
,WEP.E_NumeroColaboradorInterno,WEP.E_TipoColaborador_ID,WEP.E_MotivoBaja_ID
,WEP.E_FechaAntiguedad,WEP.P_Posicion_ID,WEP.P_FechaInicio,WEP.P_FechaFin
,WEP.P_NombrePosicion,WEP.P_Puesto_ID,WEP.P_Plan_ID,WEP.P_EntidadLegal_ID
,WEP.P_AreaNegocio_ID,WEP.P_UnidadTrabajo_ID,COALESCE (CAT.Categoria_ID,NULL) as Categoria_ID
,WEP.P_Nivel_ID,WEP.P_CentroTrabajo_ID,WEP.P_CentroCostos_ID
,WEP.P_Convenio_ID,WEP.P_HorasXsemana,WEP.P_TiempoCompleto,WEP.Sistema_Fuente,WEP.storeday
FROM (
SELECT
PER.STD_ID_HR  AS Empleado_ID 
,PER.ID_ORGANIZATION AS Organizacion_ID 
,PER.STD_OR_HR_PERIOD  AS NumeroPeriodo
,HPOS.SCO_ID_POSITION AS Posicion_ID
,ROL.SCO_OR_HR_ROLE AS Rol_ID 
,CAST(CAT.STD_ID_JOB_CATEGOR AS STRING) AS  Categoria_ID 
FROM erp_mexico_sz.STD_HR_PERIOD PER
INNER JOIN erp_mexico_sz.M4SCO_H_HR_LEGENT EL ON 
PER.ID_ORGANIZATION = EL.ID_ORGANIZATION
AND PER.STD_ID_HR = EL.STD_ID_HR
AND PER.STD_OR_HR_PERIOD = EL.STD_OR_HR_PERIOD
INNER JOIN erp_mexico_sz.M4SCO_HR_ROLE ROL ON  
PER.ID_ORGANIZATION = ROL.ID_ORGANIZATION
AND PER.STD_ID_HR = ROL.SCO_ID_HR
AND PER.STD_OR_HR_PERIOD = ROL.SCO_OR_HR_PER
INNER JOIN
(
SELECT HC.ID_ORGANIZATION, HC.SCO_ID_HR, HC.SCO_OR_HR_ROLE, HC.DT_START, HC.DT_END, HC.STD_ID_JOB_CATEGOR
FROM erp_mexico_sz.M4SGB_H_HR_CATEGOR HC
INNER JOIN
(
SELECT ID_ORGANIZATION, SCO_ID_HR, SCO_OR_HR_ROLE, MAX(DT_END) AS DT_END 
FROM erp_mexico_sz.M4SGB_H_HR_CATEGOR 
GROUP BY ID_ORGANIZATION, SCO_ID_HR, SCO_OR_HR_ROLE
) HCC
ON HC.ID_ORGANIZATION = HCC.ID_ORGANIZATION 
AND HC.SCO_ID_HR = HCC.SCO_ID_HR 
AND HC.SCO_OR_HR_ROLE = HCC.SCO_OR_HR_ROLE 
AND HC.DT_END = HCC.DT_END 
) CAT ON 
ROL.ID_ORGANIZATION  = CAT.ID_ORGANIZATION
AND ROL.SCO_ID_HR = CAT.SCO_ID_HR
AND ROL.SCO_OR_HR_ROLE = CAT.SCO_OR_HR_ROLE 
INNER JOIN erp_mexico_sz.M4SCO_H_HR_POS HPOS ON
ROL.ID_ORGANIZATION  = HPOS.ID_ORGANIZATION
AND ROL.SCO_ID_HR = HPOS.SCO_ID_HR 
AND ROL.SCO_OR_HR_ROLE = HPOS.SCO_OR_HR_ROLE
WHERE PER.STD_ID_HR_TYPE = '01'
AND PER.ID_ORGANIZATION IN (SELECT EntidadLegal_ID_PNET 
FROM gb_mdl_mexico_costoproducir.GX_CONTROL_EL_PEOPLENET 
WHERE Cadena = 'Peoplenet V7' GROUP BY EntidadLegal_ID_PNET) 
GROUP BY PER.STD_ID_HR,PER.ID_ORGANIZATION,PER.STD_OR_HR_PERIOD,HPOS.SCO_ID_POSITION,ROL.SCO_OR_HR_ROLE,CAT.STD_ID_JOB_CATEGOR
) CAT, gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION WEP
WHERE WEP.E_Empleado_ID=CAT.Empleado_ID 
AND WEP.E_Organizacion_ID=CAT.Organizacion_ID 
AND WEP.E_NumeroPeriodo=CAT.NumeroPeriodo 
AND WEP.E_Rol_ID=CAT.Rol_ID  
AND WEP.P_Posicion_ID=CAT.Posicion_ID) tmp 
join (SELECT wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID,max(wrke.storeday) as first_record 
from gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION wrke 
group by wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID) sec 
on tmp.E_Empleado_ID=sec.E_Empleado_ID
and tmp.E_NumeroPeriodo=sec.E_NumeroPeriodo
and tmp.E_FechaInicioPeriodo=sec.E_FechaInicioPeriodo
and tmp.E_Posicion_ID=sec.E_Posicion_ID
and tmp.storeday=sec.first_record;


--SET Paso = 5; 

INSERT OVERWRITE TABLE gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION SELECT tmp.* 
from (SELECT WEP.E_Empleado_ID,WEP.E_Organizacion_ID,WEP.E_NumeroPeriodo
,WEP.E_FechaInicioPeriodo,WEP.E_FechaFinPeriodo,WEP.E_Posicion_ID,WEP.E_FechaInicioPosicion
,WEP.E_FechaFinPosicion,WEP.E_Rol_ID,WEP.E_NumeroColaboradorInterno,WEP.E_TipoColaborador_ID
,WEP.E_MotivoBaja_ID,WEP.E_FechaAntiguedad,WEP.P_Posicion_ID,WEP.P_FechaInicio,WEP.P_FechaFin
,WEP.P_NombrePosicion,WEP.P_Puesto_ID,WEP.P_Plan_ID,WEP.P_EntidadLegal_ID,WEP.P_AreaNegocio_ID
,WEP.P_UnidadTrabajo_ID,WEP.P_Categoria_ID,COALESCE(N.Nivel_ID,NULL) as Nivel_ID,WEP.P_CentroTrabajo_ID,WEP.P_CentroCostos_ID
,WEP.P_Convenio_ID,WEP.P_HorasXsemana,WEP.P_TiempoCompleto,WEP.Sistema_Fuente,WEP.storeday
FROM (SELECT
PER.STD_ID_HR AS Empleado_ID 
,PER.ID_ORGANIZATION  AS Organizacion_ID 
,PER.STD_OR_HR_PERIOD  AS NumeroPeriodo
,HPOS.SCO_ID_POSITION AS Posicion_ID
,ROL.SCO_OR_HR_ROLE AS Rol_ID  
,HIST.STD_ID_JOB_INT_CLA AS Nivel_ID
FROM erp_mexico_sz.STD_HR_PERIOD PER
INNER JOIN erp_mexico_sz.M4SCO_H_HR_LEGENT EL ON 
PER.ID_ORGANIZATION = EL.ID_ORGANIZATION
AND PER.STD_ID_HR = EL.STD_ID_HR
AND PER.STD_OR_HR_PERIOD = EL.STD_OR_HR_PERIOD
INNER JOIN erp_mexico_sz.M4SCO_HR_ROLE ROL ON  
PER.ID_ORGANIZATION = ROL.ID_ORGANIZATION
AND PER.STD_ID_HR = ROL.SCO_ID_HR
AND PER.STD_OR_HR_PERIOD = ROL.SCO_OR_HR_PER
INNER JOIN erp_mexico_sz.M4SCO_H_HR_POS HPOS ON
ROL.ID_ORGANIZATION = HPOS.ID_ORGANIZATION
AND ROL.SCO_ID_HR = HPOS.SCO_ID_HR 
AND ROL.SCO_OR_HR_ROLE = HPOS.SCO_OR_HR_ROLE
INNER JOIN erp_mexico_sz.M4SCO_POSITION POS ON 
HPOS.SCO_ID_POSITION  = POS.SCO_ID_POSITION
AND HPOS.ID_ORGANIZATION = POS.ID_ORGANIZATION
INNER JOIN 
( 
SELECT CAST(OL.STD_ID_LEG_ENT AS STRING) AS STD_ID_LEG_ENT ,HIST.ID_ORGANIZATION,HIST.STD_ID_JOB_CODE ,HIST.STD_ID_JOB_INT_CLA
FROM erp_mexico_sz.STD_HT_JOB_DEF HIST 
INNER JOIN 
(  
SELECT 
HIST.ID_ORGANIZATION
,HIST.STD_ID_JOB_CODE 
,MAX(HIST.STD_DT_END) AS STD_DT_END
FROM erp_mexico_sz.STD_HT_JOB_DEF HIST
GROUP BY HIST.ID_ORGANIZATION,HIST.STD_ID_JOB_CODE 
) H
ON HIST.ID_ORGANIZATION = H.ID_ORGANIZATION AND HIST.STD_ID_JOB_CODE = H.STD_ID_JOB_CODE AND HIST.STD_DT_END = H.STD_DT_END
INNER JOIN erp_mexico_sz.STD_LEG_ENT OL ON  OL.ID_ORGANIZATION  = HIST.ID_ORGANIZATION 
) HIST
ON POS.ID_ORGANIZATION=HIST.STD_ID_LEG_ENT 
AND POS.SCO_ID_JOB_CODE=HIST.STD_ID_JOB_CODE   
WHERE PER.STD_ID_HR_TYPE='01'
AND PER.ID_ORGANIZATION IN (SELECT EntidadLegal_ID_DWH FROM gb_mdl_mexico_costoproducir.GX_CONTROL_EL_PEOPLENET WHERE Cadena = 'Peoplenet V7' GROUP BY EntidadLegal_ID_DWH)
GROUP BY PER.STD_ID_HR,PER.ID_ORGANIZATION,PER.STD_OR_HR_PERIOD,HPOS.SCO_ID_POSITION,ROL.SCO_OR_HR_ROLE,HIST.STD_ID_JOB_INT_CLA
) N, gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION WEP
WHERE WEP.E_Empleado_ID=N.Empleado_ID 
AND WEP.E_Organizacion_ID=N.Organizacion_ID 
AND WEP.E_NumeroPeriodo=N.NumeroPeriodo 
AND WEP.E_Rol_ID=N.Rol_ID  
AND WEP.P_Posicion_ID=N.Posicion_ID) tmp 
join (SELECT wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID,max(wrke.storeday) as first_record 
from gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION wrke 
group by wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID) sec 
on tmp.E_Empleado_ID=sec.E_Empleado_ID
and tmp.E_NumeroPeriodo=sec.E_NumeroPeriodo
and tmp.E_FechaInicioPeriodo=sec.E_FechaInicioPeriodo
and tmp.E_Posicion_ID=sec.E_Posicion_ID
and tmp.storeday=sec.first_record;


--SET Paso = 6; 

INSERT OVERWRITE TABLE gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION SELECT tmp.* 
FROM (SELECT WEP.E_Empleado_ID,WEP.E_Organizacion_ID,WEP.E_NumeroPeriodo
,WEP.E_FechaInicioPeriodo,WEP.E_FechaFinPeriodo,WEP.E_Posicion_ID,WEP.E_FechaInicioPosicion
,WEP.E_FechaFinPosicion,WEP.E_Rol_ID,WEP.E_NumeroColaboradorInterno,WEP.E_TipoColaborador_ID
,WEP.E_MotivoBaja_ID,WEP.E_FechaAntiguedad,WEP.P_Posicion_ID,WEP.P_FechaInicio,WEP.P_FechaFin
,WEP.P_NombrePosicion,WEP.P_Puesto_ID,WEP.P_Plan_ID,WEP.P_EntidadLegal_ID,WEP.P_AreaNegocio_ID
,WEP.P_UnidadTrabajo_ID,WEP.P_Categoria_ID,WEP.P_Nivel_ID,COALESCE(CTR.CentroTrabajo_ID,NULL) as CentroTrabajo_ID
,WEP.P_CentroCostos_ID,WEP.P_Convenio_ID,WEP.P_HorasXsemana,WEP.P_TiempoCompleto,WEP.Sistema_Fuente,WEP.storeday
FROM ( SELECT
PER.STD_ID_HR AS Empleado_ID 
,PER.ID_ORGANIZATION AS Organizacion_ID 
,PER.STD_OR_HR_PERIOD AS NumeroPeriodo
,HPOS.SCO_ID_POSITION AS Posicion_ID
,ROL.SCO_OR_HR_ROLE AS Rol_ID
,CAST(WL.SCO_ID_WORK_LOC AS STRING) AS CentroTrabajo_ID
FROM erp_mexico_sz.STD_HR_PERIOD PER
INNER JOIN erp_mexico_sz.M4SCO_H_HR_LEGENT EL ON 
PER.ID_ORGANIZATION=EL.ID_ORGANIZATION
AND PER.STD_ID_HR=EL.STD_ID_HR
AND PER.STD_OR_HR_PERIOD=EL.STD_OR_HR_PERIOD
INNER JOIN erp_mexico_sz.M4SCO_HR_ROLE ROL ON  
PER.ID_ORGANIZATION=ROL.ID_ORGANIZATION
AND PER.STD_ID_HR=ROL.SCO_ID_HR
AND PER.STD_OR_HR_PERIOD=ROL.SCO_OR_HR_PER
INNER JOIN 
(
SELECT WL.ID_ORGANIZATION,WL.SCO_ID_HR,WL.SCO_OR_HR_ROLE,WL.SCO_ID_WORK_LOC,WL.SCO_DT_START,WL.SCO_DT_END,
ROW_NUMBER() OVER (PARTITION BY WL.ID_ORGANIZATION, WL.SCO_ID_HR, WL.SCO_DT_START ORDER BY WL.SCO_OR_HR_ROLE)=1
FROM erp_mexico_sz.M4SCO_H_HR_WLOC WL
INNER JOIN 
(
SELECT ID_ORGANIZATION, SCO_ID_HR, SCO_OR_HR_ROLE, MAX(SCO_DT_END) AS SCO_DT_END
FROM erp_mexico_sz.M4SCO_H_HR_WLOC  
GROUP BY ID_ORGANIZATION, SCO_ID_HR, SCO_OR_HR_ROLE
) WLM ON
WL.ID_ORGANIZATION = WLM.ID_ORGANIZATION 
AND WL.SCO_ID_HR = WLM.SCO_ID_HR 
AND WL.SCO_OR_HR_ROLE = WLM.SCO_OR_HR_ROLE  
AND WL.SCO_DT_END = WLM.SCO_DT_END
) WL ON  
ROL.ID_ORGANIZATION=WL.ID_ORGANIZATION
AND ROL.SCO_ID_HR=WL.SCO_ID_HR
AND ROL.SCO_OR_HR_ROLE=WL.SCO_OR_HR_ROLE 
INNER JOIN erp_mexico_sz.M4SCO_H_HR_POS HPOS ON
ROL.ID_ORGANIZATION=HPOS.ID_ORGANIZATION
AND ROL.SCO_ID_HR=HPOS.SCO_ID_HR 
AND ROL.SCO_OR_HR_ROLE=HPOS.SCO_OR_HR_ROLE
WHERE PER.STD_ID_HR_TYPE='01'
AND PER.ID_ORGANIZATION IN (SELECT EntidadLegal_ID_DWH  FROM gb_mdl_mexico_costoproducir.GX_CONTROL_EL_PEOPLENET WHERE Cadena = 'Peoplenet V7' GROUP BY EntidadLegal_ID_DWH)
GROUP BY PER.STD_ID_HR,PER.ID_ORGANIZATION,PER.STD_OR_HR_PERIOD,HPOS.SCO_ID_POSITION,ROL.SCO_OR_HR_ROLE,WL.SCO_ID_WORK_LOC
) CTR, gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION WEP
WHERE WEP.E_Empleado_ID=CTR.Empleado_ID 
AND WEP.E_Organizacion_ID=CTR.Organizacion_ID 
AND WEP.E_NumeroPeriodo=CTR.NumeroPeriodo 
AND WEP.E_Rol_ID=CTR.Rol_ID 
AND WEP.P_Posicion_ID=CTR.Posicion_ID) tmp 
join (SELECT wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID,max(wrke.storeday) as first_record 
from gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION wrke 
group by wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID) sec 
on tmp.E_Empleado_ID=sec.E_Empleado_ID
and tmp.E_NumeroPeriodo=sec.E_NumeroPeriodo
and tmp.E_FechaInicioPeriodo=sec.E_FechaInicioPeriodo
and tmp.E_Posicion_ID=sec.E_Posicion_ID
and tmp.storeday=sec.first_record;



--SET Paso = 7; 

INSERT OVERWRITE TABLE gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION SELECT tmp.* 
From (SELECT WEP.E_Empleado_ID,WEP.E_Organizacion_ID,WEP.E_NumeroPeriodo
,WEP.E_FechaInicioPeriodo,WEP.E_FechaFinPeriodo,WEP.E_Posicion_ID
,WEP.E_FechaInicioPosicion,WEP.E_FechaFinPosicion,WEP.E_Rol_ID
,WEP.E_NumeroColaboradorInterno,WEP.E_TipoColaborador_ID,WEP.E_MotivoBaja_ID
,WEP.E_FechaAntiguedad,WEP.P_Posicion_ID,WEP.P_FechaInicio,WEP.P_FechaFin
,WEP.P_NombrePosicion,WEP.P_Puesto_ID,WEP.P_Plan_ID,WEP.P_EntidadLegal_ID
,WEP.P_AreaNegocio_ID,WEP.P_UnidadTrabajo_ID,WEP.P_Categoria_ID,WEP.P_Nivel_ID
,WEP.P_CentroTrabajo_ID,COALESCE(CC.CentroCostos_ID,NULL) as CentroCostos_ID,WEP.P_Convenio_ID,WEP.P_HorasXsemana
,WEP.P_TiempoCompleto,WEP.Sistema_Fuente,WEP.storeday
FROM (
SELECT PER.STD_ID_HR AS Empleado_ID 
,PER.ID_ORGANIZATION AS Organizacion_ID 
,PER.STD_OR_HR_PERIOD AS NumeroPeriodo
,HPOS.SCO_ID_POSITION AS Posicion_ID
,ROL.SCO_OR_HR_ROLE AS Rol_ID 
,CAST(CC.SSP_ID_CENT_COSTO AS STRING) AS CentroCostos_ID    
FROM erp_mexico_sz.STD_HR_PERIOD PER
INNER JOIN erp_mexico_sz.M4SCO_H_HR_LEGENT EL ON 
PER.ID_ORGANIZATION  = EL.ID_ORGANIZATION
AND PER.STD_ID_HR  = EL.STD_ID_HR
AND PER.STD_OR_HR_PERIOD = EL.STD_OR_HR_PERIOD
INNER JOIN erp_mexico_sz.M4SCO_HR_ROLE ROL ON  
PER.ID_ORGANIZATION = ROL.ID_ORGANIZATION
AND PER.STD_ID_HR = ROL.SCO_ID_HR
AND PER.STD_OR_HR_PERIOD = ROL.SCO_OR_HR_PER
INNER JOIN 
(
SELECT
A.ID_ORGANIZATION
,A.SCO_ID_HR
,A.SCO_OR_HR_ROLE
,A.DT_START as DT_START
,A.DT_END as DT_END
,A.SSP_ID_CENT_COSTO as SSP_ID_CENT_COSTO
FROM erp_mexico_sz.M4SAR_H_HR_C_COSTO A,gb_mdl_mexico_costoproducir_views.V_M4SAR_H_HR_C_COSTO_TEMP B
WHERE A.ID_ORGANIZATION <> '072' 
AND A.ID_ORGANIZATION=B.ID_ORGANIZATION
AND A.SCO_ID_HR=B.SCO_ID_HR
AND A.SCO_OR_HR_ROLE=B.SCO_OR_HR_ROLE
AND A.DT_END=B.DT_END
UNION ALL
SELECT
A.ID_ORGANIZATION
,A.SCO_ID_HR
,A.SCO_OR_HR_ROLE
,A.SCB_DT_START as DT_START
,A.SCB_DT_END as DT_END
,A.SCB_ID_CENTRO_COST as SSP_ID_CENT_COSTO
FROM erp_mexico_sz.M4SCB_H_HR_ROL_CC A,gb_mdl_mexico_costoproducir_views.V_M4SCB_H_HR_ROL_CC_TEMP_COL B
WHERE A.ID_ORGANIZATION = '072' 
AND A.ID_ORGANIZATION=B.ID_ORGANIZATION
AND A.SCO_ID_HR=B.SCO_ID_HR
AND A.SCO_OR_HR_ROLE=B.SCO_OR_HR_ROLE
AND A.SCB_DT_END=B.DT_END
UNION ALL
SELECT
A.ID_ORGANIZATION
,A.SCO_ID_HR
,A.SCO_OR_HR_ROLE
,A.SSP_FEC_INICIO as DT_START
,A.SSP_FEC_FIN as DT_END
,A.SSP_ID_CENT_COSTO as SSP_ID_CENT_COSTO
FROM erp_mexico_sz.M4SSP_H_CENT_COS A,gb_mdl_mexico_costoproducir_views.V_M4SSP_H_CENT_COS_IBER B
WHERE A.ID_ORGANIZATION IN ('118','170','171','172','173','175','176','177','183','190') 
AND A.ID_ORGANIZATION=B.ID_ORGANIZATION 
AND A.SCO_ID_HR=B.SCO_ID_HR
AND A.SCO_OR_HR_ROLE=B.SCO_OR_HR_ROLE
AND A.SSP_FEC_FIN=B.DT_END
) CC ON 
ROL.ID_ORGANIZATION  = CC.ID_ORGANIZATION
AND ROL.SCO_ID_HR = CC.SCO_ID_HR
AND ROL.SCO_OR_HR_ROLE = CC.SCO_OR_HR_ROLE 
INNER JOIN erp_mexico_sz.M4SCO_H_HR_POS HPOS ON ROL.ID_ORGANIZATION  = HPOS.ID_ORGANIZATION
AND ROL.SCO_ID_HR = HPOS.SCO_ID_HR 
AND ROL.SCO_OR_HR_ROLE = HPOS.SCO_OR_HR_ROLE
WHERE PER.STD_ID_HR_TYPE = '01' 
AND PER.ID_ORGANIZATION IN (SELECT EntidadLegal_ID_DWH
FROM gb_mdl_mexico_costoproducir.GX_CONTROL_EL_PEOPLENET 
WHERE Cadena = 'Peoplenet V7' GROUP BY EntidadLegal_ID_DWH)
GROUP BY PER.STD_ID_HR,PER.ID_ORGANIZATION,PER.STD_OR_HR_PERIOD,HPOS.SCO_ID_POSITION,ROL.SCO_OR_HR_ROLE,CC.SSP_ID_CENT_COSTO
) CC,gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION WEP
WHERE WEP.E_Empleado_ID = CC.Empleado_ID 
AND WEP.E_Organizacion_ID = CC.Organizacion_ID 
AND WEP.E_NumeroPeriodo  = CC.NumeroPeriodo 
AND WEP.E_Rol_ID = CC.Rol_ID  
AND WEP.P_Posicion_ID = CC.Posicion_ID) tmp 
join (SELECT wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID,max(wrke.storeday) as first_record 
from gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION wrke 
group by wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID) sec 
on tmp.E_Empleado_ID=sec.E_Empleado_ID
and tmp.E_NumeroPeriodo=sec.E_NumeroPeriodo
and tmp.E_FechaInicioPeriodo=sec.E_FechaInicioPeriodo
and tmp.E_Posicion_ID=sec.E_Posicion_ID
and tmp.storeday=sec.first_record;



--SET Paso = 8; 

INSERT OVERWRITE TABLE gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION 
SELECT tmp.* from (SELECT WEP.E_Empleado_ID,WEP.E_Organizacion_ID,WEP.E_NumeroPeriodo,
WEP.E_FechaInicioPeriodo,WEP.E_FechaFinPeriodo,WEP.E_Posicion_ID,WEP.E_FechaInicioPosicion,
WEP.E_FechaFinPosicion,WEP.E_Rol_ID,WEP.E_NumeroColaboradorInterno,WEP.E_TipoColaborador_ID,
WEP.E_MotivoBaja_ID,WEP.E_FechaAntiguedad,WEP.P_Posicion_ID,WEP.P_FechaInicio,
WEP.P_FechaFin,WEP.P_NombrePosicion,WEP.P_Puesto_ID,WEP.P_Plan_ID,WEP.P_EntidadLegal_ID,
WEP.P_AreaNegocio_ID,WEP.P_UnidadTrabajo_ID,WEP.P_Categoria_ID,WEP.P_Nivel_ID,
WEP.P_CentroTrabajo_ID,WEP.P_CentroCostos_ID,COALESCE(C.Convenio_ID,NULL) as Convenio_ID,WEP.P_HorasXsemana,
WEP.P_TiempoCompleto,WEP.Sistema_Fuente,WEP.storeday
FROM 
(
SELECT
PER.STD_ID_HR AS Empleado_ID 
,PER.ID_ORGANIZATION AS Organizacion_ID 
,PER.STD_OR_HR_PERIOD AS NumeroPeriodo
,HPOS.SCO_ID_POSITION AS Posicion_ID
,ROL.SCO_OR_HR_ROLE AS Rol_ID 
,CAST(CONV.SAR_ID_CONVENIO AS STRING) AS Convenio_ID 
FROM erp_mexico_sz.STD_HR_PERIOD PER
INNER JOIN erp_mexico_sz.M4SCO_H_HR_LEGENT EL ON 
PER.ID_ORGANIZATION = EL.ID_ORGANIZATION
AND PER.STD_ID_HR = EL.STD_ID_HR
AND PER.STD_OR_HR_PERIOD = EL.STD_OR_HR_PERIOD
INNER JOIN erp_mexico_sz.M4SCO_HR_ROLE ROL ON  
PER.ID_ORGANIZATION = ROL.ID_ORGANIZATION
AND PER.STD_ID_HR = ROL.SCO_ID_HR
AND PER.STD_OR_HR_PERIOD = ROL.SCO_OR_HR_PER
INNER JOIN 
(SELECT
A.ID_ORGANIZATION
,A.STD_ID_HR
,A.STD_OR_HR_PERIOD
,A.DT_START  
,A.DT_END
,A.SAR_ID_CONVENIO 
FROM erp_mexico_sz.M4SAR_H_CONVENIO A,gb_mdl_mexico_costoproducir_views.V_M4SAR_H_CONVENIO_TEMP B
WHERE A.ID_ORGANIZATION <> '072' 
AND A.ID_ORGANIZATION=B.ID_ORGANIZATION
AND A.STD_ID_HR=B.STD_ID_HR
AND A.STD_OR_HR_PERIOD=B.STD_OR_HR_PERIOD
AND A.DT_END = B.DT_END
UNION ALL
SELECT
A.ID_ORGANIZATION
,A.STD_ID_HR
,A.STD_OR_HR_PERIOD
,A.SCB_DT_START AS DT_START
,A.SCB_DT_END AS DT_END
,A.SCB_ID_CONVENIO AS SAR_ID_CONVENIO
FROM erp_mexico_sz.M4SCB_H_HR_CONVENI A,gb_mdl_mexico_costoproducir_views.V_M4SCB_H_HR_CONVENI_TEMP_COL B
WHERE A.ID_ORGANIZATION='072' 
AND A.ID_ORGANIZATION=B.ID_ORGANIZATION
AND A.STD_ID_HR=B.STD_ID_HR
AND A.STD_OR_HR_PERIOD=B.STD_OR_HR_PERIOD
AND A.SCB_DT_END=B.DT_END
UNION ALL
SELECT
A.ID_ORGANIZATION
,A.SSP_ID_HR as STD_ID_HR
,A.STD_OR_HR_PERIOD
,A.FEC_INICIO AS DT_START
,A.FEC_FIN AS DT_END
,A.SSP_ID_CONVENIO AS SAR_ID_CONVENIO
FROM erp_mexico_sz.M4SSP_H_CONVENIOS A,gb_mdl_mexico_costoproducir_views.V_M4SSP_H_CONVENIOS_IBER B
WHERE A.ID_ORGANIZATION IN ('118','170','171','172','173','175','176','177','183','190')
AND A.ID_ORGANIZATION=B.ID_ORGANIZATION
AND A.SSP_ID_HR=B.SSP_ID_HR
AND A.STD_OR_HR_PERIOD=B.STD_OR_HR_PERIOD
AND A.FEC_FIN=B.DT_END
) CONV 
ON PER.ID_ORGANIZATION = CONV.ID_ORGANIZATION
AND PER.STD_ID_HR = CONV.STD_ID_HR
AND PER.STD_OR_HR_PERIOD = CONV.STD_OR_HR_PERIOD
INNER JOIN erp_mexico_sz.M4SCO_H_HR_POS HPOS ON
ROL.ID_ORGANIZATION = HPOS.ID_ORGANIZATION
AND ROL.SCO_ID_HR = HPOS.SCO_ID_HR 
AND ROL.SCO_OR_HR_ROLE = HPOS.SCO_OR_HR_ROLE 
WHERE PER.STD_ID_HR_TYPE = '01'
AND PER.ID_ORGANIZATION IN (SELECT EntidadLegal_ID_DWH
FROM gb_mdl_mexico_costoproducir.GX_CONTROL_EL_PEOPLENET 
WHERE Cadena = 'Peoplenet V7' GROUP BY EntidadLegal_ID_DWH)
GROUP BY PER.STD_ID_HR,PER.ID_ORGANIZATION,PER.STD_OR_HR_PERIOD,HPOS.SCO_ID_POSITION,ROL.SCO_OR_HR_ROLE,CONV.SAR_ID_CONVENIO
) C, gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION WEP
WHERE WEP.E_Empleado_ID=C.Empleado_ID 
AND WEP.E_Organizacion_ID=C.Organizacion_ID 
AND WEP.E_NumeroPeriodo=C.NumeroPeriodo 
AND WEP.E_Rol_ID=C.Rol_ID 
AND WEP.P_Posicion_ID=C.Posicion_ID) tmp 
join (SELECT wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID,max(wrke.storeday) as first_record 
from gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION wrke 
group by wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID) sec 
on tmp.E_Empleado_ID=sec.E_Empleado_ID
and tmp.E_NumeroPeriodo=sec.E_NumeroPeriodo
and tmp.E_FechaInicioPeriodo=sec.E_FechaInicioPeriodo
and tmp.E_Posicion_ID=sec.E_Posicion_ID
and tmp.storeday=sec.first_record;


--SET Paso = 9;

INSERT OVERWRITE TABLE gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION 
SELECT tmp.* from (SELECT WEP.E_Empleado_ID,WEP.E_Organizacion_ID
,WEP.E_NumeroPeriodo,WEP.E_FechaInicioPeriodo,WEP.E_FechaFinPeriodo
,WEP.E_Posicion_ID,WEP.E_FechaInicioPosicion,WEP.E_FechaFinPosicion
,WEP.E_Rol_ID,WEP.E_NumeroColaboradorInterno,WEP.E_TipoColaborador_ID
,WEP.E_MotivoBaja_ID,WEP.E_FechaAntiguedad,WEP.P_Posicion_ID,WEP.P_FechaInicio
,WEP.P_FechaFin,WEP.P_NombrePosicion,WEP.P_Puesto_ID,WEP.P_Plan_ID
,WEP.P_EntidadLegal_ID,WEP.P_AreaNegocio_ID,WEP.P_UnidadTrabajo_ID
,WEP.P_Categoria_ID,WEP.P_Nivel_ID,WEP.P_CentroTrabajo_ID
,WEP.P_CentroCostos_ID,WEP.P_Convenio_ID,COALESCE(HT.HorasXsemana,NULL) as HorasXsemana
,COALESCE(HT.TiempoCompleto,NULL) as TiempoCompleto
,WEP.Sistema_Fuente,WEP.storeday
FROM 
(
SELECT
PER.STD_ID_HR AS Empleado_ID 
,PER.ID_ORGANIZATION AS Organizacion_ID 
,PER.STD_OR_HR_PERIOD AS NumeroPeriodo
,HPOS.SCO_ID_POSITION AS Posicion_ID
,ROL.SCO_OR_HR_ROLE AS Rol_ID  
,TIM.SCO_ROLE_W_HOURS AS HorasXsemana 
,TIM.SCO_FULL_TIME AS TiempoCompleto 
FROM erp_mexico_sz.STD_HR_PERIOD PER
INNER JOIN erp_mexico_sz.M4SCO_H_HR_LEGENT EL ON 
PER.ID_ORGANIZATION  = EL.ID_ORGANIZATION
AND PER.STD_ID_HR  = EL.STD_ID_HR
AND PER.STD_OR_HR_PERIOD = EL.STD_OR_HR_PERIOD
INNER JOIN erp_mexico_sz.M4SCO_HR_ROLE ROL ON  
PER.ID_ORGANIZATION = ROL.ID_ORGANIZATION
AND PER.STD_ID_HR = ROL.SCO_ID_HR
AND PER.STD_OR_HR_PERIOD = ROL.SCO_OR_HR_PER 
INNER JOIN erp_mexico_sz.M4SCO_H_HR_RO_TIME TIM ON
TIM.ID_ORGANIZATION = ROL.ID_ORGANIZATION
AND TIM.SCO_ID_HR = ROL.SCO_ID_HR
AND TIM.SCO_OR_HR_ROLE = ROL.SCO_OR_HR_ROLE
INNER JOIN erp_mexico_sz.M4SCO_H_HR_POS HPOS ON
ROL.ID_ORGANIZATION  = HPOS.ID_ORGANIZATION
AND ROL.SCO_ID_HR = HPOS.SCO_ID_HR 
AND ROL.SCO_OR_HR_ROLE = HPOS.SCO_OR_HR_ROLE
WHERE PER.STD_ID_HR_TYPE = '01'
AND PER.ID_ORGANIZATION IN (SELECT EntidadLegal_ID_DWH  
FROM gb_mdl_mexico_costoproducir.GX_CONTROL_EL_PEOPLENET 
WHERE Cadena = 'Peoplenet V7' GROUP BY EntidadLegal_ID_DWH) 
GROUP BY PER.STD_ID_HR,PER.ID_ORGANIZATION,PER.STD_OR_HR_PERIOD
,HPOS.SCO_ID_POSITION,ROL.SCO_OR_HR_ROLE,TIM.SCO_ROLE_W_HOURS,TIM.SCO_FULL_TIME
) HT,gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION WEP
WHERE WEP.E_Empleado_ID=HT.Empleado_ID 
AND WEP.E_Organizacion_ID=HT.Organizacion_ID 
AND WEP.E_NumeroPeriodo=HT.NumeroPeriodo 
AND WEP.E_Rol_ID=HT.Rol_ID 
AND WEP.P_Posicion_ID=HT.Posicion_ID) tmp 
join (SELECT wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID,max(wrke.storeday) as first_record 
from gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION wrke 
group by wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID) sec 
on tmp.E_Empleado_ID=sec.E_Empleado_ID
and tmp.E_NumeroPeriodo=sec.E_NumeroPeriodo
and tmp.E_FechaInicioPeriodo=sec.E_FechaInicioPeriodo
and tmp.E_Posicion_ID=sec.E_Posicion_ID
and tmp.storeday=sec.first_record;


--SET Paso = 10;

INSERT OVERWRITE TABLE gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION 
SELECT tmp.* from (SELECT WEP.E_Empleado_ID,WEP.E_Organizacion_ID,WEP.E_NumeroPeriodo
,WEP.E_FechaInicioPeriodo,WEP.E_FechaFinPeriodo,WEP.E_Posicion_ID,WEP.E_FechaInicioPosicion
,WEP.E_FechaFinPosicion,WEP.E_Rol_ID,WEP.E_NumeroColaboradorInterno,WEP.E_TipoColaborador_ID
,WEP.E_MotivoBaja_ID,WEP.E_FechaAntiguedad,WEP.P_Posicion_ID,WEP.P_FechaInicio,WEP.P_FechaFin
,WEP.P_NombrePosicion,WEP.P_Puesto_ID,COALESCE(P.Plan_ID,NULL) as Plan_ID,WEP.P_EntidadLegal_ID,WEP.P_AreaNegocio_ID
,WEP.P_UnidadTrabajo_ID,WEP.P_Categoria_ID,WEP.P_Nivel_ID,WEP.P_CentroTrabajo_ID,WEP.P_CentroCostos_ID
,WEP.P_Convenio_ID,WEP.P_HorasXsemana,WEP.P_TiempoCompleto,WEP.Sistema_Fuente,WEP.storeday
FROM 
(
SELECT 
PER.STD_ID_HR  AS Empleado_ID 
,PER.ID_ORGANIZATION  AS Organizacion_ID 
,PER.STD_OR_HR_PERIOD  AS NumeroPeriodo
,HPOS.SCO_ID_POSITION AS Posicion_ID
,ROL.SCO_OR_HR_ROLE AS Rol_ID  
,CAST(PLAN.SCO_ID_PLAN  AS STRING) AS Plan_ID
FROM erp_mexico_sz.STD_HR_PERIOD PER
INNER JOIN erp_mexico_sz.M4SCO_H_HR_LEGENT EL ON 
PER.ID_ORGANIZATION = EL.ID_ORGANIZATION
AND PER.STD_ID_HR = EL.STD_ID_HR
AND PER.STD_OR_HR_PERIOD = EL.STD_OR_HR_PERIOD 
INNER JOIN erp_mexico_sz.M4SCO_HR_ROLE ROL ON  
PER.ID_ORGANIZATION = ROL.ID_ORGANIZATION
AND PER.STD_ID_HR = ROL.SCO_ID_HR
AND PER.STD_OR_HR_PERIOD = ROL.SCO_OR_HR_PER 
INNER JOIN 
(
SELECT PLAN.ID_ORGANIZATION, PLAN.SCO_ID_HR, PLAN.SCO_OR_HR_ROLE, PLAN.SCO_DT_START, PLAN.SCO_DT_END, PLAN.SCO_ID_PLAN                   
FROM erp_mexico_sz.M4SCO_H_HR_SAL_PL PLAN
INNER JOIN
(
SELECT PLAN.ID_ORGANIZATION, PLAN.SCO_ID_HR, PLAN.SCO_OR_HR_ROLE, MAX(PLAN.SCO_DT_END) AS SCO_DT_END
FROM erp_mexico_sz.M4SCO_H_HR_SAL_PL PLAN
GROUP BY PLAN.ID_ORGANIZATION, PLAN.SCO_ID_HR, PLAN.SCO_OR_HR_ROLE
) P ON
PLAN.ID_ORGANIZATION = P.ID_ORGANIZATION 
AND PLAN.SCO_ID_HR = P.SCO_ID_HR 
AND PLAN.SCO_OR_HR_ROLE = P.SCO_OR_HR_ROLE 
AND PLAN.SCO_DT_END = P.SCO_DT_END 
)PLAN ON 
ROL.ID_ORGANIZATION  = PLAN.ID_ORGANIZATION
AND ROL.SCO_ID_HR = PLAN.SCO_ID_HR  
AND ROL.SCO_OR_HR_ROLE = PLAN.SCO_OR_HR_ROLE
INNER JOIN erp_mexico_sz.M4SCO_H_HR_POS HPOS ON
ROL.ID_ORGANIZATION  = HPOS.ID_ORGANIZATION
AND ROL.SCO_ID_HR = HPOS.SCO_ID_HR 
AND ROL.SCO_OR_HR_ROLE = HPOS.SCO_OR_HR_ROLE
WHERE PER.STD_ID_HR_TYPE = '01' 
AND PER.ID_ORGANIZATION IN (SELECT EntidadLegal_ID_DWH 
FROM gb_mdl_mexico_costoproducir.GX_CONTROL_EL_PEOPLENET WHERE Cadena = 'Peoplenet V7' GROUP BY EntidadLegal_ID_DWH)
GROUP BY PER.STD_ID_HR,PER.ID_ORGANIZATION,PER.STD_OR_HR_PERIOD,HPOS.SCO_ID_POSITION,ROL.SCO_OR_HR_ROLE,PLAN.SCO_ID_PLAN
) P,gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION WEP
WHERE WEP.E_Empleado_ID = P.Empleado_ID 
AND WEP.E_Organizacion_ID = P.Organizacion_ID 
AND WEP.E_NumeroPeriodo = P.NumeroPeriodo 
AND WEP.E_Rol_ID = P.Rol_ID 
AND WEP.P_Posicion_ID = P.Posicion_ID) tmp 
join (SELECT wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID,max(wrke.storeday) as first_record 
from gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION wrke 
group by wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID) sec 
on tmp.E_Empleado_ID=sec.E_Empleado_ID
and tmp.E_NumeroPeriodo=sec.E_NumeroPeriodo
and tmp.E_FechaInicioPeriodo=sec.E_FechaInicioPeriodo
and tmp.E_Posicion_ID=sec.E_Posicion_ID
and tmp.storeday=sec.first_record;



--SET Paso = 11;

INSERT OVERWRITE TABLE gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION 
SELECT tmp.* from (SELECT WEP.E_Empleado_ID,WEP.E_Organizacion_ID,WEP.E_NumeroPeriodo,WEP.E_FechaInicioPeriodo
,WEP.E_FechaFinPeriodo,WEP.E_Posicion_ID,WEP.E_FechaInicioPosicion,WEP.E_FechaFinPosicion
,WEP.E_Rol_ID,WEP.E_NumeroColaboradorInterno,WEP.E_TipoColaborador_ID,WEP.E_MotivoBaja_ID
,WEP.E_FechaAntiguedad,WEP.P_Posicion_ID,WEP.P_FechaInicio,WEP.P_FechaFin,WEP.P_NombrePosicion
,WEP.P_Puesto_ID,WEP.P_Plan_ID,WEP.P_EntidadLegal_ID,WEP.P_AreaNegocio_ID,COALESCE(UT.UnidadTrabajo_ID,NULL) as UnidadTrabajo_ID
,WEP.P_Categoria_ID,WEP.P_Nivel_ID,WEP.P_CentroTrabajo_ID,WEP.P_CentroCostos_ID,WEP.P_Convenio_ID
,WEP.P_HorasXsemana,WEP.P_TiempoCompleto,WEP.Sistema_Fuente,WEP.storeday
FROM 
(
SELECT
UT.Empleado_ID 
,UT.Organizacion_ID
,UT.NumeroPeriodo
,HPOS.SCO_ID_POSITION AS Posicion_ID
,UT.Rol_ID
,UT.UnidadTrabajo_ID
FROM
(
SELECT 
PER.STD_ID_HR  AS Empleado_ID 
,PER.ID_ORGANIZATION  AS Organizacion_ID 
,PER.STD_OR_HR_PERIOD  AS NumeroPeriodo
,ROL.SCO_OR_HR_ROLE AS Rol_ID  
,CAST(LT2.STD_ID_WL_PARENT  AS STRING) AS UnidadTrabajo_ID
FROM erp_mexico_sz.STD_HR_PERIOD PER
INNER JOIN erp_mexico_sz.M4SCO_H_HR_LEGENT EL ON 
PER.ID_ORGANIZATION = EL.ID_ORGANIZATION
AND PER.STD_ID_HR = EL.STD_ID_HR
AND PER.STD_OR_HR_PERIOD = EL.STD_OR_HR_PERIOD
INNER JOIN erp_mexico_sz.M4SCO_HR_ROLE ROL ON  
PER.ID_ORGANIZATION = ROL.ID_ORGANIZATION
AND PER.STD_ID_HR = ROL.SCO_ID_HR
AND PER.STD_OR_HR_PERIOD = ROL.SCO_OR_HR_PER
INNER JOIN 
(
SELECT WL.ID_ORGANIZATION, WL.SCO_ID_HR, WL.SCO_OR_HR_ROLE, WL.SCO_ID_WORK_LOC, WL.SCO_DT_START, WL.SCO_DT_END
FROM erp_mexico_sz.M4SCO_H_HR_WLOC WL
INNER JOIN 
(
SELECT ID_ORGANIZATION, SCO_ID_HR, SCO_OR_HR_ROLE, MAX(SCO_DT_END) AS SCO_DT_END
FROM erp_mexico_sz.M4SCO_H_HR_WLOC  
GROUP BY ID_ORGANIZATION, SCO_ID_HR, SCO_OR_HR_ROLE
) WLM ON
WL.ID_ORGANIZATION = WLM.ID_ORGANIZATION AND WL.SCO_ID_HR = WLM.SCO_ID_HR AND WL.SCO_OR_HR_ROLE = WLM.SCO_OR_HR_ROLE  AND WL.SCO_DT_END = WLM.SCO_DT_END
) WL ON ROL.ID_ORGANIZATION  = WL.ID_ORGANIZATION
AND ROL.SCO_ID_HR = WL.SCO_ID_HR
AND ROL.SCO_OR_HR_ROLE = WL.SCO_OR_HR_ROLE 
INNER JOIN 
( SELECT LT1.ID_ORGANIZATION, LT1.STD_ID_WL_CHILD, LT1.STD_ID_WL_PARENT, LT1.STD_DT_START, LT1.STD_DT_END
FROM erp_mexico_sz.STD_REL_WORK_LOC LT1
INNER JOIN
(
SELECT ID_ORGANIZATION, STD_ID_WL_CHILD, MAX(STD_DT_END) AS STD_DT_END 
FROM erp_mexico_sz.STD_REL_WORK_LOC 
GROUP BY ID_ORGANIZATION, STD_ID_WL_CHILD
) LT11 ON LT1.ID_ORGANIZATION = LT11.ID_ORGANIZATION 
AND LT1.STD_ID_WL_CHILD = LT11.STD_ID_WL_CHILD 
AND LT1.STD_DT_END = LT11.STD_DT_END
) LT1 ON  
WL.ID_ORGANIZATION  = LT1.ID_ORGANIZATION
AND WL.SCO_ID_WORK_LOC  = LT1.STD_ID_WL_CHILD
INNER JOIN 
(
SELECT LT2.ID_ORGANIZATION, LT2.STD_ID_WL_CHILD, LT2.STD_ID_WL_PARENT, LT2.STD_DT_START, LT2.STD_DT_END
FROM erp_mexico_sz.STD_REL_WORK_LOC LT2
INNER JOIN 
(
SELECT ID_ORGANIZATION, STD_ID_WL_CHILD, MAX(STD_DT_END) AS STD_DT_END 
FROM erp_mexico_sz.STD_REL_WORK_LOC 
GROUP BY ID_ORGANIZATION, STD_ID_WL_CHILD
) LT22 ON LT2.ID_ORGANIZATION = LT22.ID_ORGANIZATION 
AND LT2.STD_ID_WL_CHILD = LT22.STD_ID_WL_CHILD 
AND LT2.STD_DT_END = LT22.STD_DT_END  
) LT2 ON LT1.ID_ORGANIZATION  = LT2.ID_ORGANIZATION
AND LT1.STD_ID_WL_PARENT  = LT2.STD_ID_WL_CHILD
WHERE PER.STD_ID_HR_TYPE = '01'
AND PER.ID_ORGANIZATION IN (SELECT EntidadLegal_ID_DWH 
FROM gb_mdl_mexico_costoproducir.GX_CONTROL_EL_PEOPLENET WHERE Cadena = 'Peoplenet V7' GROUP BY EntidadLegal_ID_DWH)
GROUP BY PER.STD_ID_HR,PER.ID_ORGANIZATION,PER.STD_OR_HR_PERIOD,ROL.SCO_OR_HR_ROLE,LT2.STD_ID_WL_PARENT
) UT
INNER JOIN erp_mexico_sz.M4SCO_H_HR_POS HPOS ON
UT.Organizacion_ID = HPOS.ID_ORGANIZATION
AND UT.Empleado_ID = HPOS.SCO_ID_HR 
AND UT.Rol_ID = HPOS.SCO_OR_HR_ROLE
GROUP BY UT.Empleado_ID,UT.Organizacion_ID,UT.NumeroPeriodo,HPOS.SCO_ID_POSITION,UT.Rol_ID,UT.UnidadTrabajo_ID
) UT, gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION WEP
WHERE WEP.E_Empleado_ID = UT.Empleado_ID 
AND WEP.E_Organizacion_ID  = UT.Organizacion_ID 
AND WEP.E_NumeroPeriodo  = UT.NumeroPeriodo 
AND WEP.E_Rol_ID = UT.Rol_ID 
AND WEP.P_Posicion_ID = UT.Posicion_ID) tmp 
join (SELECT wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID,max(wrke.storeday) as first_record 
from gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION wrke 
group by wrke.E_Empleado_ID,wrke.E_NumeroPeriodo,
wrke.E_FechaInicioPeriodo,wrke.E_Posicion_ID) sec 
on tmp.E_Empleado_ID=sec.E_Empleado_ID
and tmp.E_NumeroPeriodo=sec.E_NumeroPeriodo
and tmp.E_FechaInicioPeriodo=sec.E_FechaInicioPeriodo
and tmp.E_Posicion_ID=sec.E_Posicion_ID
and tmp.storeday=sec.first_record;


--=================================================================================     
--Materializamos E_EMPLEADO_POSICION 
--=================================================================================     

TRUNCATE TABLE gb_smntc_mexico_costoproducir.E_EMPLEADO_POSICION;

INSERT INTO gb_smntc_mexico_costoproducir.E_EMPLEADO_POSICION PARTITION(EntidadLegal_ID)
SELECT * FROM gb_mdl_mexico_costoproducir_views.VDW_E_EMPLEADO_POSICION;

--compactacion
INSERT OVERWRITE TABLE gb_smntc_mexico_costoproducir.E_EMPLEADO_POSICION PARTITION(EntidadLegal_ID) select tmp.*
from gb_smntc_mexico_costoproducir.E_EMPLEADO_POSICION tmp join (select Empleado_ID,NumeroPeriodo,FechaInicioPeriodo
,Posicion_ID,MAX(storeday) as first_record from gb_smntc_mexico_costoproducir.E_EMPLEADO_POSICION 
group by Empleado_ID,NumeroPeriodo,FechaInicioPeriodo,Posicion_ID) sec
on tmp.Empleado_ID=sec.Empleado_ID
and tmp.NumeroPeriodo=sec.NumeroPeriodo
and tmp.FechaInicioPeriodo=sec.FechaInicioPeriodo
and tmp.Posicion_ID=sec.Posicion_ID
and tmp.storeday=sec.first_record;



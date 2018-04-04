invalidate metadata;
---------------------------------------------
---- ext_big_data_12_importes
---------------------------------------------
insert overwrite jedoxMexico.ext_big_data_12_importes
SELECT 
P.EntidadLegal_ID,
PL.PLANTA_ID,
P.Producto_ID,
cast(SUM(T.PRIMARY_QUANTITY*T.Actual_cost) as DECIMAL(38,10)) AS monto 
FROM gb_mdl_mexico_costoproducir.MTL_TRANSACCION_MATERIALES T
LEFT OUTER JOIN gb_mdl_mexico_costoproducir.MTL_CATALOGO_MATERIALES M ON  t.inventory_item_id = m.inventory_item_id and t.organization_id = m.organization_id
LEFT OUTER JOIN gb_mdl_mexico_manufactura.MF_PLANTAS PL ON T.ORGANIZATION_ID = PL.MF_ORGANIZACION_ID AND PL.sistema_fuente = 'CP'
INNER JOIN 
           (SELECT Tipo_producto_ID, EntidadLegal_ID, MF_Producto_ID, TRIM(Producto_ID) AS Producto_ID
                FROM gb_mdl_mexico_manufactura.MF_Producto_Organizacion
                WHERE Origen IN('MEXICO','CA')
        AND Tipo_producto_ID IN(1)
                GROUP BY 1,2,3,4) P
           ON T.INVENTORY_ITEM_ID = CAST(P.MF_Producto_ID AS INT) AND P.EntidadLegal_ID in (${VAR:VAR_EL})
WHERE  t.transaction_type_id in (17,44) and m.item_type = 'PT' and t.subinventory_Code='SUB'
AND T.transaction_Date BETWEEN '${VAR:VAR_FECHA_INICIO}' AND '${VAR:VAR_FECHA_FIN}'
AND t.organization_id IN (
SELECT DISTINCT MF_Organizacion_ID
FROM gb_mdl_mexico_manufactura.MF_Plantas 
WHERE ENTIDADLEGAL_ID in (${VAR:VAR_EL})
) 
group by 1,2,3;

---------------------------------------------
---- ext_big_data_210_importes
---------------------------------------------

--Insert
insert overwrite jedoxMexico.ext_big_data_210_importes
SELECT
entidadlegal_id,
'Caso 1' as Caso,
sum(TOT_mactividaddelperiodo) AS Valor
FROM gb_smntc_mexico_costoproducir.t_A_rubros_fsg
where entidadlegal_id in (${VAR:VAR_EL}) and aniosaldo = ${VAR:VAR_ANIO} and messaldo = ${VAR:VAR_MES} and LINEA_ID = 25
group by 1,2

UNION ALL

SELECT 
entidadlegal_id,
'Caso 2' as Caso,
SUM(CANTIDAD*COSTO) AS Valor
FROM gb_smntc_mexico_costoproducir.V_MF_Compras C
WHERE ENTIDADLEGAL_ID in (${VAR:VAR_EL}) AND PERIODO = '${VAR:VAR_PERIODO}' AND Codigo_Sub LIKE  '%PT%'
group by 1,2;

---------------------------------------------
---- ext_big_data_211_costostd
---------------------------------------------

--insert
insert overwrite jedoxMexico.ext_big_data_211_costostd
select c.entidadlegal_id, c.planta_id, po.producto_id, c.costo as costoestandarmaquilado
from gb_smntc_mexico_costoproducir.v_producto_Maquilado C 
inner join gb_mdl_mexico_manufactura.mf_producto_organizacion po
on  po.mf_producto_id = C.mf_producto_id
and po.entidadlegal_id =  C.entidadlegal_id 
and po.mf_organizacion_id = C.mf_organizacion_id
Where c.EntidadLegal_Id in (${VAR:VAR_EL})
and c.periodo = '${VAR:VAR_PERIODO}';


---------------------------------------------
---- ext_big_data_25_importes
---------------------------------------------

--insert
insert overwrite jedoxMexico.ext_big_data_25_importes
SELECT
D.EntidadLegal_ID
,A.Planta_ID
,'s/cc' as CentroCostos_ID
,Coalesce(P.Producto_ID, 's/i') as producto
,SUM(Monto) AS Monto
FROM gb_smntc_mexico_costoproducir.V_Rubro25_Diferencia_Inv D
LEFT OUTER JOIN 
( 
SELECT
F.EntidadLegal_ID,
P.MF_Organizacion_ID,
P.Planta_ID,
F.CentroCostos_ID
FROM gb_smntc_mexico_costoproducir.t_A_Rubros_FSG F
INNER JOIN gb_smntc_mexico_costoproducir.V_MF_Plantas P 
ON F.EntidadLegal_Id = P.EntidadLegal_Id AND F.AreaNegocio_ID = P.Planta_ID AND p.sistema_fuente = 'CP'
WHERE F.Linea_ID = 25 
AND F.AnioSaldo = ${VAR:VAR_ANIO}
AND F.MesSaldo = ${VAR:VAR_MES}
AND F.EntidadLegal_ID in (${VAR:VAR_EL})
GROUP BY 1,2,3,4
) A
ON (A.EntidadLegal_ID = D.EntidadLegal_ID 
AND A.MF_Organizacion_ID = D.MF_Organizacion_ID AND A.CentroCostos_ID = D.CentroCostos_ID) 
LEFT OUTER JOIN 
(SELECT EntidadLegal_ID, MF_Producto_ID, TRIM(Producto_ID) AS Producto_ID
FROM gb_mdl_mexico_manufactura.MF_Producto_Organizacion
WHERE Origen IN('MEXICO','CA')
GROUP BY 1,2,3) P
ON D.MF_Producto_ID = CAST(P.MF_Producto_ID AS INT) 
AND D.EntidadLegal_ID = P.EntidadLegal_ID
WHERE D.Periodo ='${VAR:VAR_PERIODO}' AND D.EntidadLegal_ID in (${VAR:VAR_EL})
GROUP BY 1,2,3,4;


---------------------------------------------
---- ext_big_data_26a29_importes
---------------------------------------------

--insert
insert overwrite jedoxMexico.ext_big_data_26a29_importes
SELECT 
SN.Entidadlegal_ID as entidadlegal , 
SN.Areanegocio_id as plantas,
CAST('-1' as int) as ingredientes,
SUM(Coalesce(Montodebito,0) - Coalesce(Montocredito,0)) AS  Importe,
CASE WHEN CUENTANATURAL_ID = '5116' THEN 26 WHEN CUENTANATURAL_ID = '5117' THEN 28 WHEN CUENTANATURAL_ID = '5118' THEN 27 ELSE 29 END AS concepto
FROM gb_smntc_mexico_costoproducir.A_Saldo_Nomina SN
WHERE PERIODO = '${VAR:VAR_PERIODO2}' AND ENTIDADLEGAL_ID in (${VAR:VAR_EL}) AND CUENTANATURAL_ID IN ('5117','5116','5118')
AND HDR_STATUS = 'P'
GROUP BY 1,2,3,5

UNION ALL

SELECT 
Der.EntidadLegal_ID as entidadlegal,
 Der.Planta_ID as plantas,
  cast(coalesce(poi.producto_id,'-1') as int) as ingredientes,
   Importe,
CAST ('29' as int) as concepto
FROM gb_smntc_mexico_costoproducir.CP_Derivados_Fin Der
left join gb_smntc_mexico_costoproducir.V_mf_producto_organizacion poi on   
        der.entidadlegal_id = poi.entidadlegal_id
        and  der.mf_organizacion_id = poi.mf_organizacion_id
        and  der.ingrediente_id = poi.mf_producto_id
where Der.Periodo = '${VAR:VAR_PERIODO}' 
and Der.EntidadLegal_id in (${VAR:VAR_EL})
and Der.Fecha_Fin is null;


---------------------------------------------
---- ext_big_data_31a24_cc_a_lineas
---------------------------------------------

--insert
insert overwrite jedoxMexico.ext_big_data_31a24_cc_a_lineas
SELECT LC.EntidadLegal_Id, LC.Planta_ID, LC.CentroCostos_ID, LC.Linea_Prod_ID
FROM gb_smntc_mexico_costoproducir.V_MF_Lineas_Prod_Centro_Costos LC
INNER JOIN
(
   SELECT EntidadLegal_Id, MF_Organizacion_ID, Planta_ID, Linea_Prod_ID, Medida_Linea
   FROM gb_smntc_mexico_costoproducir.CP_Medidas_Prorrateo
   WHERE EntidadLegal_Id IN (${VAR:VAR_EL}) AND TipoMedida_ID = 2 AND Periodo = '${VAR:VAR_PERIODO}'
   GROUP BY 1,2,3,4,5
)  ML
ON   LC.EntidadLegal_Id = ML.EntidadLegal_Id AND LC.MF_Organizacion_ID = ML.MF_Organizacion_ID AND LC.Planta_ID = ML.Planta_ID AND LC.Linea_Prod_ID = ML.Linea_Prod_ID
WHERE LC.EntidadLegal_Id IN (${VAR:VAR_EL}) AND LC.Fecha_Fin is null and ML.Medida_Linea <> 0 and LC.DL = 1 
GROUP BY 1,2,3,4;

---------------------------------------------
---- ext_big_data_31a34_fsg
---------------------------------------------

--insert
insert overwrite jedoxMexico.ext_big_data_31a34_fsg
SELECT 
fsg.EntidadLegal_ID,fsg.AreaNegocio_ID, fsg.CentroCostos_ID,
CASE 
 WHEN (FSG.Cuentanatural_ID BETWEEN '6300' AND '6399' OR FSG.Cuentanatural_ID = '6997') AND FSG.Cuentanatural_id <> '6304' AND LC.DL = 1 THEN 31
 WHEN FSG.Cuentanatural_ID = '6304' AND LC.DL = 1 THEN 32
 WHEN (FSG.Cuentanatural_ID BETWEEN '6300' AND '6399' OR FSG.Cuentanatural_ID = '6997') AND LC.DL = 0 THEN 33
 WHEN (FSG.Cuentanatural_ID BETWEEN '6300' AND '6399' OR FSG.Cuentanatural_ID = '6997') AND LC.DL = 2 THEN 34
END AS Subrubro_ID,
sum(TOT_mActividaddelPeriodo) as monto
FROM gb_smntc_mexico_costoproducir.T_A_RUBROS_FSG FSG      
INNER JOIN gb_smntc_mexico_costoproducir.V_MF_Plantas P 
 ON FSG.EntidadLegal_Id = P.EntidadLegal_Id AND FSG.AreaNegocio_ID = P.Planta_ID
INNER JOIN (SELECT EntidadLegal_Id , MF_Organizacion_ID, Planta_ID, CentroCostos_ID, DL
          FROM gb_mdl_mexico_manufactura.MF_Lineas_Prod_Centro_Costos 
          WHERE DL IN (0,1,2) 
          AND Fecha_Fin IS NULL
          GROUP BY 1,2,3,4,5) LC
ON P.EntidadLegal_Id = LC.EntidadLegal_Id AND P.MF_Organizacion_ID = LC.MF_Organizacion_ID AND P.Planta_ID = LC.Planta_ID AND FSG.CentroCostos_ID = LC.CentroCostos_ID 
WHERE FSG.EntidadLegal_ID IN (${VAR:VAR_EL})
AND FSG.AnioSaldo = ${VAR:VAR_ANIO}
AND FSG.MesSaldo  = ${VAR:VAR_MES}
AND FSG.Linea_ID = 30
AND (FSG.Cuentanatural_ID BETWEEN '6300' AND '6399' OR FSG.Cuentanatural_ID = '6997')
AND FSG.TOT_MActividadDelPeriodo <> 0
group by 1,2,3,4;


---------------------------------------------
---- ext_big_data_31a34_importes
---------------------------------------------

--insert
insert overwrite jedoxMexico.ext_big_data_31a34_importes
SELECT      
(CASE WHEN LINEAS.DL = 0 THEN '33' WHEN N.Cuentanatural_ID = '6304' THEN '32' ELSE '31' END ) AS subrubro_id, 
N.EntidadLegal_id, 
N.Centrocostos_id, 
N.Areanegocio_id, 
sum(coalesce(montodebito,0))-Sum(coalesce(montocredito,0)) as Monto
FROM gb_smntc_mexico_costoproducir.A_Saldo_Nomina N
INNER JOIN (
        SELECT
         EntidadLegal_ID,
         TRIM(lower(Condicion)) AS  Condicion 
         FROM gb_smntc_mexico_costoproducir.cp_parametros 
        WHERE EntidadLegal_ID IN (${VAR:VAR_EL}) 
           AND Objeto = 'A_SALDO_NOMINA' 
           AND Campo = 'HDR_JE_Source' 
           AND Subrubro_ID IN (31,32,33,34) GROUP BY 1,2
        )P ON P.entidadLegal_ID = N.entidadlegal_id and lower(p.Condicion) = lower(N.Hdr_je_source)
INNER JOIN 
(
SELECT LC.EntidadLegal_Id, LC.Planta_ID, LC.CentroCostos_ID, LC.DL
FROM gb_mdl_mexico_manufactura.MF_Lineas_Prod_Centro_Costos LC
   INNER JOIN
   (
        SELECT EntidadLegal_Id, MF_Organizacion_ID, Planta_ID, Linea_Prod_ID, Medida_Linea
        FROM gb_smntc_mexico_costoproducir.CP_Medidas_Prorrateo
        WHERE EntidadLegal_Id IN (${VAR:VAR_EL}) AND TipoMedida_ID = 2 
        AND Periodo = '${VAR:VAR_PERIODO}'
        GROUP BY 1,2,3,4,5
   )  ML
   ON LC.EntidadLegal_Id = ML.EntidadLegal_Id 
   AND LC.MF_Organizacion_ID = ML.MF_Organizacion_ID 
   AND LC.Planta_ID = ML.Planta_ID AND LC.Linea_Prod_ID = ML.Linea_Prod_ID
WHERE LC.EntidadLegal_Id IN (${VAR:VAR_EL}) AND LC.Fecha_Fin is null 
AND ML.Medida_Linea <> 0 and LC.DL in (0,1) 
GROUP BY 1,2,3,4
) LINEAS
ON N.EntidadLegal_ID = LINEAS.EntidadLegal_ID and N.AreaNegocio_ID = LINEAS.Planta_ID And N.CentroCostos_ID = LINEAS.CentroCostos_ID 
WHERE 
(N.Cuentanatural_ID BETWEEN '6300' AND '6399' OR N.Cuentanatural_ID = '6997') 
and N.EntidadLegal_ID in (${VAR:VAR_EL}) and N.periodo = '${VAR:VAR_PERIODO2}'
GROUP BY 1,2,3,4;


-- ---------------------------------------------
-- ---- ext_big_data_41y42_fsg_2
-- ---------------------------------------------
-- --insert
-- insert overwrite jedoxMexico.ext_big_data_41y42_fsg_2
-- SELECT       
--        FSG.EntidadLegal_Id       
--        ,P.Planta_ID       
--        ,FSG.CentroCostos_ID       
--        ,SUM(COALESCE(FSG.TOT_mactividaddelperiodo,0)) AS Monto       
-- FROM (SELECT
--      AnioSaldo
--      ,MesSaldo
--      ,EntidadLegal_ID
--      ,Linea_ID
--      ,NombreConcepto
--      ,AreaNegocio_ID
--      ,CuentaNatural_ID
--      ,CentroCostos_ID
--      ,AnalisisLocal_ID
--      ,SUM(CASE WHEN FSG.SIGNO = '-' THEN (-1)*ActividaddelPeriodo ELSE ActividaddelPeriodo END) TOT_mActividaddelPeriodo
-- FROM 
--      (
--   SELECT * 
--   FROM    gb_mdl_mexico_costoproducir.A_SALDO 
--   WHERE  Presupuesto = 0
--   AND EntidadLegal_ID in (${VAR:VAR_EL}) AND JuegoLibros_ID IN (141,161)
--                 ) SLD
--      INNER JOIN
--           (
--                SELECT
--                     RFD.Reporte_ID
--                     ,RFD.Linea_ID
--                     ,RF.NombreConcepto
--                     ,Signo
--                     ,COALESCE(I_AreaNegocio_ID,'0000') I_AreaNegocio_IDD, COALESCE(F_AreaNegocio_ID,'9999') F_AreaNegocio_IDD
--                     ,COALESCE(I_CuentaNatural_ID,'0000') I_CuentaNatural_IDD, COALESCE(F_CuentaNatural_ID,'9999') F_CuentaNatural_IDD
--                     ,COALESCE(I_CentroCostos_ID,'0000') I_CentroCostos_IDD, COALESCE(F_CentroCostos_ID,'9999') F_CentroCostos_IDD
--                     ,COALESCE(I_Marca_ID,'000') I_Marca_IDD, COALESCE(F_Marca_ID,'999') F_Marca_IDD
--                     ,COALESCE(I_AnalisisLocal_ID,'0000') I_AnalisisLocal_IDD, COALESCE(F_AnalisisLocal_ID,'9999') F_AnalisisLocal_IDD
--                FROM gb_mdl_mexico_costoproducir.a_reporte_financiero_dtl RFD
--                     INNER JOIN 
--                          (
--                               SELECT * 
--                               FROM gb_mdl_mexico_costoproducir.a_reporte_financiero 
--                               WHERE displayflag = 'Y' 
--                                    AND Reporte_ID = 10001  --9439 cambio 20150801 -- 9443   -- cambio en 20120315, antes era 5285  
--                                    AND Linea_ID IN (45)
--                           ) RF ON RF.Linea_ID = RFD.Linea_ID AND RF.Reporte_ID = RFD.Reporte_ID
--           ) FSG ON 
--                (SLD.AreaNegocio_ID           BETWEEN I_AreaNegocio_IDD     AND F_AreaNegocio_IDD
--                     AND SLD.CuentaNatural_ID BETWEEN I_CuentaNatural_IDD   AND F_CuentaNatural_IDD
--                     AND SLD.CentroCostos_ID  BETWEEN I_CentroCostos_IDD    AND F_CentroCostos_IDD
--                     AND SLD.Marca_ID         BETWEEN I_Marca_IDD           AND F_Marca_IDD
--                     AND SLD.AnalisisLocal_ID BETWEEN I_AnalisisLocal_IDD   AND F_AnalisisLocal_IDD)
  
--   WHERE  EntidadLegal_ID IN (${VAR:VAR_EL}) AND Presupuesto = 0 AND ANIOSALDO = ${VAR:VAR_ANIO} AND MESSALDO = ${VAR:VAR_MES}

-- GROUP BY 1,2,3,4,5,6,7,8,9  ) FSG       
-- INNER JOIN  (SELECT DISTINCT ENTIDADLEGAL_ID, PLANTA_ID FROM gb_smntc_mexico_costoproducir.V_MF_PLANTAS WHERE EntidadLegal_ID in (${VAR:VAR_EL})) P          
--  ON FSG.EntidadLegal_Id = P.EntidadLegal_Id AND FSG.AreaNegocio_ID = P.Planta_ID       
-- WHERE Linea_ID = 45
-- AND FSG.CuentaNatural_ID  like '64%'  
-- AND FSG.AnioSaldo = ${VAR:VAR_ANIO}
-- AND FSG.MesSaldo  = ${VAR:VAR_MES}
-- AND FSG.EntidadLegal_ID IN (${VAR:VAR_EL}) 
-- GROUP BY 1,2,3
-- having monto <> 0;



---------------------------------------------
---- ext_big_data_41y42_fsg_2
---------------------------------------------
--insert

drop table if exists jedoxMexico.ext_big_data_41y42_fsg_2_fsg;
drop table if exists jedoxMexico.ext_big_data_41y42_fsg_2_fsg_2;

create table if not exists jedoxMexico.ext_big_data_41y42_fsg_2_fsg
as
SELECT
    RFD.Reporte_ID
    ,RFD.Linea_ID
    ,RF.NombreConcepto
    ,Signo
    ,COALESCE(I_AreaNegocio_ID,'0000') I_AreaNegocio_IDD, COALESCE(F_AreaNegocio_ID,'9999') F_AreaNegocio_IDD
    ,COALESCE(I_CuentaNatural_ID,'0000') I_CuentaNatural_IDD, COALESCE(F_CuentaNatural_ID,'9999') F_CuentaNatural_IDD
    ,COALESCE(I_CentroCostos_ID,'0000') I_CentroCostos_IDD, COALESCE(F_CentroCostos_ID,'9999') F_CentroCostos_IDD
    ,COALESCE(I_Marca_ID,'000') I_Marca_IDD, COALESCE(F_Marca_ID,'999') F_Marca_IDD
    ,COALESCE(I_AnalisisLocal_ID,'0000') I_AnalisisLocal_IDD, COALESCE(F_AnalisisLocal_ID,'9999') F_AnalisisLocal_IDD
FROM gb_mdl_mexico_costoproducir.a_reporte_financiero_dtl RFD
    INNER JOIN 
         (
              SELECT * 
              FROM gb_mdl_mexico_costoproducir.a_reporte_financiero 
              WHERE displayflag = 'Y' 
                   AND Reporte_ID = 10001  --9439 cambio 20150801 -- 9443   -- cambio en 20120315, antes era 5285  
                   AND Linea_ID IN (45)
          ) RF ON RF.Linea_ID = RFD.Linea_ID AND RF.Reporte_ID = RFD.Reporte_ID;


create table if not exists jedoxMexico.ext_big_data_41y42_fsg_2_fsg_2
as
SELECT
     AnioSaldo
     ,MesSaldo
     ,EntidadLegal_ID
     ,Linea_ID
     ,NombreConcepto
     ,AreaNegocio_ID
     ,CuentaNatural_ID
     ,CentroCostos_ID
     ,AnalisisLocal_ID
     ,SUM(CASE WHEN FSG.SIGNO = '-' THEN (-1)*ActividaddelPeriodo ELSE ActividaddelPeriodo END) TOT_mActividaddelPeriodo
FROM 
     (
  SELECT * 
  FROM    gb_mdl_mexico_costoproducir.A_SALDO 
  WHERE  Presupuesto = 0
  AND EntidadLegal_ID in (${VAR:VAR_EL}) AND JuegoLibros_ID IN (141,161,641)
                ) SLD
     INNER JOIN jedoxMexico.ext_big_data_41y42_fsg_2_fsg FSG ON 
               (SLD.AreaNegocio_ID           BETWEEN I_AreaNegocio_IDD     AND F_AreaNegocio_IDD
                    AND SLD.CuentaNatural_ID BETWEEN I_CuentaNatural_IDD   AND F_CuentaNatural_IDD
                    AND SLD.CentroCostos_ID  BETWEEN I_CentroCostos_IDD    AND F_CentroCostos_IDD
                    AND SLD.Marca_ID         BETWEEN I_Marca_IDD           AND F_Marca_IDD
                    AND SLD.AnalisisLocal_ID BETWEEN I_AnalisisLocal_IDD   AND F_AnalisisLocal_IDD)
  
  WHERE  EntidadLegal_ID IN (${VAR:VAR_EL}) AND Presupuesto = 0 AND ANIOSALDO = ${VAR:VAR_ANIO} AND MESSALDO = ${VAR:VAR_MES}

GROUP BY 1,2,3,4,5,6,7,8,9;

insert overwrite jedoxMexico.ext_big_data_41y42_fsg_2
SELECT       
       FSG.EntidadLegal_Id       
       ,P.Planta_ID       
       ,FSG.CentroCostos_ID       
       ,SUM(COALESCE(FSG.TOT_mactividaddelperiodo,0)) AS Monto       
FROM jedoxMexico.ext_big_data_41y42_fsg_2_fsg_2 FSG       
INNER JOIN  (SELECT DISTINCT ENTIDADLEGAL_ID, PLANTA_ID FROM gb_smntc_mexico_costoproducir.V_MF_PLANTAS WHERE EntidadLegal_ID in (${VAR:VAR_EL})) P          
 ON FSG.EntidadLegal_Id = P.EntidadLegal_Id AND FSG.AreaNegocio_ID = P.Planta_ID       
WHERE Linea_ID = 45
AND FSG.CuentaNatural_ID  like '64%'  
AND FSG.AnioSaldo = ${VAR:VAR_ANIO}
AND FSG.MesSaldo  = ${VAR:VAR_MES}
AND FSG.EntidadLegal_ID IN (${VAR:VAR_EL}) 
GROUP BY 1,2,3
having monto <> 0;


---------------------------------------------
---- ext_big_data_41y42_importes_2
---------------------------------------------

--insert
insert overwrite jedoxMexico.ext_big_data_41y42_importes_2
SELECT A.*
FROM(
SELECT     
N.periodo AS Periodo     
,N.EntidadLegal_ID AS EntidadLegal_ID     
,b.Planta_ID     
,N.centrocostos_id AS CentroCostos_ID 
--,N.Cuentanatural_ID
,coalesce(sum(n.montodebito),0)-coalesce(Sum(n.montocredito),0) as Monto
FROM gb_smntc_mexico_costoproducir.A_SALDO_NOMINA  N      
INNER JOIN  gb_smntc_mexico_costoproducir.V_MF_Plantas b ON N.EntidadLegal_ID = B.EntidadLegal_ID AND N.AreaNegocio_ID = B.Planta_ID     
WHERE  N.periodo = '${VAR:VAR_PERIODO2}'
AND (N.Cuentanatural_ID like '642%' OR (N.Cuentanatural_ID = '6401' AND N.EntidadLegal_ID = '125'))
AND N.EntidadLegal_ID IN (${VAR:VAR_EL}) 
 AND N.centrocostos_id NOT IN ('0051', '0402', '0404', '0405')
 AND b.Planta_ID NOT IN ('0001')  
and n.flagencabezado = 'A'
GROUP BY 1,2,3,4
)A WHERE A.MONTO != 0;


---------------------------------------------
---- ext_big_data_510_importes
---------------------------------------------

--insert
insert overwrite jedoxMexico.ext_big_data_510_importes
SELECT SN.Entidadlegal_ID, 
SN.Areanegocio_id, 
SN.Centrocostos_id
,SUM(Coalesce(Montodebito,0) - Coalesce(Montocredito,0)) AS Monto
FROM gb_smntc_mexico_costoproducir.A_Saldo_Nomina SN
    INNER JOIN (
        SELECT distinct entidadlegal_id, planta_id, centroCostos_id FROM 
        gb_mdl_mexico_manufactura.MF_Lineas_Prod_Centro_Costos C 
        WHERE EntidadLegal_ID IN (${VAR:VAR_EL}) 
        and fecha_Fin is null
        and dl in (1,2)
        ) CC
    ON SN.EntidadLegal_ID = CC.EntidadLegal_ID and SN.AreaNegocio_ID = CC.Planta_ID 
    And SN.CentroCostos_ID = CC.CentroCostos_ID 
    WHERE SN.EntidadLegal_ID IN (${VAR:VAR_EL}) 
    AND (SN.CuentaNatural_ID < '6300' OR  SN.CuentaNatural_ID > '6599')
    AND NOT (SN.CuentaNatural_ID = '6997')
    AND NOT (SN.CuentaNatural_ID = '6717' AND SN.CentroCostos_ID = '0153')
    AND cast(substring(SN.Fechamovimiento,1,7) as string) = '${VAR:VAR_PERIODO}'
    AND SN.Status = 'P'
    AND SN.Hdr_status = 'P'
    AND SN.Hdr_posted_date IS NOT NULL
    AND (SN.Hdr_je_source) IN ('Spreadsheet', 'Manual', 'Other')
    AND Categoriaencabezado_je <> 'Budget'
AND Flagencabezado = 'A'
    GROUP BY 1,2,3
    HAVING monto != 0;


---------------------------------------------
---- ext_big_data_5111_importes
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_5111_importes
SELECT
FSG.EntidadLegal_ID AS EntidadLegal_ID
,P.Planta_ID AS Planta_ID
,F.Factor_ID
,Sum(FSG.TOT_mActividaddelPeriodo) AS Monto
FROM gb_smntc_mexico_costoproducir.T_A_RUBROS_FSG FSG 
    INNER JOIN gb_smntc_mexico_costoproducir.V_MF_Plantas P ON FSG.EntidadLegal_ID = P.EntidadLegal_ID AND FSG.AreaNegocio_ID = P.Planta_ID 
   INNER JOIN gb_smntc_mexico_costoproducir.CP_Factores_Prorrateo F ON   FSG.EntidadLegal_ID = F.EntidadLegal_ID AND FSG.CuentaNatural_ID = F.CuentaNatural_ID AND FSG.AnalisisLocal_ID = F.AnalisisLocal_ID AND FSG.CentroCostos_ID = F.CentroCostos_ID AND F.Fecha_Fin IS NULL   
WHERE FSG.Linea_Id IN (45)
AND FSG.CuentaNatural_ID like  '65%%' 
AND FSG.EntidadLegal_ID IN (${VAR:VAR_EL})
AND FSG.AnioSaldo =  ${VAR:VAR_ANIO}
AND FSG.MesSaldo = ${VAR:VAR_MES}
Group by 1,2,3;


---------------------------------------------
---- ext_big_data_5112_importes
---------------------------------------------

--insert
insert overwrite jedoxMexico.ext_big_data_5112_importes
SELECT   
FSG.EntidadLegal_ID
,FSG.AreaNegocio_ID
,P.Planta_ID
,F.Factor_ID
,SUM(FSG.TOT_mactividaddelperiodo) AS Monto
FROM  gb_smntc_mexico_costoproducir.T_A_RUBROS_FSG FSG 
INNER JOIN  gb_smntc_mexico_costoproducir.V_MF_Plantas P
ON FSG.EntidadLegal_ID = P.EntidadLegal_ID AND FSG.AreaNegocio_ID = P.Planta_ID 
LEFT OUTER JOIN  gb_smntc_mexico_costoproducir.CP_Factores_Prorrateo F ON   
FSG.EntidadLegal_ID = F.EntidadLegal_ID AND 
FSG.CuentaNatural_ID = F.CuentaNatural_ID AND 
FSG.AnalisisLocal_ID = F.AnalisisLocal_ID AND 
FSG.CentroCostos_ID = F.CentroCostos_ID AND 
F.Fecha_Fin IS NULL
WHERE FSG.Linea_id = 45
and FSG.CentroCostos_ID between '0151' and '0259'
AND (FSG.CuentaNatural_ID < '6300' OR  FSG.CuentaNatural_ID > '6599')
AND NOT (FSG.CuentaNatural_ID = '6997')
AND NOT (FSG.CuentaNatural_ID = '6717' AND FSG.CentroCostos_ID= '0153')
--and ((FSG.CuentaNatural_ID <> '6401' AND FSG.EntidadLegal_ID = '125') OR (FSG.EntidadLegal_ID <> '125'))
--and ((FSG.AreaNegocio_ID = '1731' and FSG.EntidadLegal_id='125') OR (FSG.EntidadLegal_id<>'125'))
AND FSG.EntidadLegal_id IN (${VAR:VAR_EL})
AND FSG.AnioSaldo =  ${VAR:VAR_ANIO}
AND FSG.MesSaldo = ${VAR:VAR_MES}
GROUP BY 1,2,3,4;

---------------------------------------------
-- ext_big_data_512_importes
---------------------------------------------


--insert
insert overwrite jedoxMexico.ext_big_data_512_importes
SELECT   
FSG.EntidadLegal_ID
,FSG.AreaNegocio_ID
,P.Planta_ID
,SUM(FSG.TOT_mactividaddelperiodo) AS Monto
FROM  gb_smntc_mexico_costoproducir.T_A_RUBROS_FSG FSG 
LEFT OUTER JOIN  gb_smntc_mexico_costoproducir.V_MF_Plantas P
ON FSG.EntidadLegal_ID = P.EntidadLegal_ID AND FSG.AreaNegocio_ID = P.Planta_ID 
WHERE FSG.Linea_id = 45
and (p.planta_id is null or p.planta_id = '1')
AND FSG.EntidadLegal_id IN (${VAR:VAR_EL})
AND FSG.AnioSaldo =  ${VAR:VAR_ANIO}
AND FSG.MesSaldo = ${VAR:VAR_MES}
GROUP BY 1,2,3;


---------------------------------------------
-- ext_big_data_513_importes
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_513_importes
SELECT   
FSG.EntidadLegal_ID AS EntidadLegal_ID
,P.Planta_ID AS Planta_ID
,SUM(TOT_mactividaddelperiodo) AS Monto
FROM  gb_smntc_mexico_costoproducir.T_A_RUBROS_FSG FSG 
INNER JOIN  gb_smntc_mexico_costoproducir.V_MF_Plantas P 
ON FSG.EntidadLegal_ID = P.EntidadLegal_ID 
AND FSG.AreaNegocio_ID = P.Planta_ID 
WHERE FSG.Linea_Id IN (45)
AND FSG.CuentaNatural_ID <> '6401'
AND FSG.EntidadLegal_ID IN (${VAR:VAR_EL})
AND FSG.AnioSaldo = ${VAR:VAR_ANIO}
AND FSG.MesSaldo = ${VAR:VAR_MES}
AND P.MF_Organizacion_ID <> -1
GROUP BY 1,2;


---------------------------------------------
---- ext_big_data_51a58_fsg
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_51a58_fsg 
SELECT       
 '53' as subrubro, 
 FSG.EntidadLegal_Id       
 ,P.Planta_ID       
 ,FSG.CentroCostos_ID       
 ,SUM(COALESCE(FSG.TOT_mactividaddelperiodo,0)) AS Monto
FROM  gb_smntc_mexico_costoproducir.T_A_RUBROS_FSG FSG        
INNER JOIN  gb_smntc_mexico_costoproducir.V_MF_PLANTAS P        
 ON FSG.EntidadLegal_Id = P.EntidadLegal_Id AND FSG.AreaNegocio_ID = P.Planta_ID       
WHERE Linea_ID = 45
AND  (FSG.CuentaNatural_ID like '63%' or FSG.CuentaNatural_ID = '6997')
AND FSG.AnioSaldo = ${VAR:VAR_ANIO}
AND FSG.CentroCostos_ID = '0151'
AND FSG.MesSaldo = ${VAR:VAR_MES}
AND FSG.EntidadLegal_ID IN (${VAR:VAR_EL})
GROUP BY 1,2,3,4       
HAVING SUM(COALESCE(FSG.TOT_mactividaddelperiodo,0)) <> 0

UNION ALL

SELECT       
       '56' as subrubro, 
       FSG.EntidadLegal_Id       
       ,P.Planta_ID       
       ,FSG.CentroCostos_ID       
       ,SUM(COALESCE(FSG.TOT_mactividaddelperiodo,0))    AS Monto       
FROM gb_smntc_mexico_costoproducir.T_A_RUBROS_FSG FSG       
INNER JOIN gb_smntc_mexico_costoproducir.V_MF_PLANTAS P        
 ON FSG.EntidadLegal_Id = P.EntidadLegal_Id AND FSG.AreaNegocio_ID = P.Planta_ID       
WHERE Linea_ID = 45
AND  (FSG.CuentaNatural_ID like '63%' or FSG.CuentaNatural_ID = '6997')
AND FSG.AnioSaldo = ${VAR:VAR_ANIO}
AND FSG.CentroCostos_ID = '0251'
AND FSG.MesSaldo = ${VAR:VAR_MES}
AND FSG.EntidadLegal_ID IN (${VAR:VAR_EL})
GROUP BY 1,2,3,4       
HAVING SUM(COALESCE(FSG.TOT_mactividaddelperiodo,0)) <> 0

UNION ALL

SELECT       
       '58' as subrubro, 
       FSG.EntidadLegal_Id       
       ,P.Planta_ID       
       ,'s/cc' as CentroCostos_ID       
       ,SUM(COALESCE(FSG.TOT_mactividaddelperiodo,0))    AS Monto       
FROM gb_smntc_mexico_costoproducir.T_A_RUBROS_FSG FSG       
INNER JOIN gb_smntc_mexico_costoproducir.V_MF_PLANTAS P        
 ON FSG.EntidadLegal_Id = P.EntidadLegal_Id AND FSG.AreaNegocio_ID = P.Planta_ID       
WHERE Linea_ID = 45
AND  (FSG.CuentaNatural_ID like '63%' or FSG.CuentaNatural_ID = '6997')
AND FSG.AnioSaldo = ${VAR:VAR_ANIO}
AND FSG.CentroCostos_ID between '0152' and '0299' 
AND  FSG.CentroCostos_ID not in ('0151','0251') 
AND FSG.MesSaldo = ${VAR:VAR_MES}
AND FSG.EntidadLegal_ID IN (${VAR:VAR_EL})
GROUP BY 1,2,3,4      
HAVING SUM(COALESCE(FSG.TOT_mactividaddelperiodo,0)) <> 0;


---------------------------------------------
---- ext_big_data_51a58_importes
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_51a58_importes
Select 
po.entidadlegal_id,
po.AreaNegocio_ID,
po.puesto_id, 
(case 
when  po.puesto_id in ('0204' , '1121', '1124', '1127', '1128') and po.centrocosto_id = '0151' then '51' 
when  po.puesto_id not in ('0204' , '1121', '1124', '1127', '1128') and po.centrocosto_id = '0151' then '52' 
when  po.puesto_id in ('0600', '0601', '0610', '0612', '0615', '0618', '0642') and po.centrocosto_id = '0251' then '54' 
when  po.puesto_id not in ('0600', '0601', '0610', '0612', '0615', '0618', '0642') and po.centrocosto_id = '0251' then '55' 
when po.centrocosto_id not in  ('0151',  '0251')  then '57'
else 'otro' end) as tipo, sum(PA.montopago) as montopago
from gb_mdl_mexico_erp.a_pago_empleado PA
inner join gb_smntc_mexico_costoproducir.e_empleado_posicion ep on pa.empleado_id = ep.empleado_id
inner join gb_smntc_mexico_costoproducir.e_posicion po on ep.posicion_id = po.posicion_id 
inner join gb_smntc_mexico_costoproducir.e_puesto pu on po.puesto_id = pu.puesto_id and po.entidadlegal_id = pu.entidadlegal_id 
where year(pa.fechapago) = ${VAR:VAR_ANIO} 
and month(pa.fechapago) = ${VAR:VAR_MES}
and pa.fechapago between ep.fechainicioposicion and ep.fechafinposicion
and po.entidadlegal_id IN (${VAR:VAR_EL}) and po.centrocosto_id between '0151' and '0299' 
and (cuentanatural_id like '63%%' or cuentanatural_id = '6997')
group by 1,2,3,4;


---------------------------------------------
---- ext_big_data_51a58_operarios
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_51a58_operarios
SELECT EntidadLegal_ID
,MF_Organizacion_ID
,Planta_ID
,Linea_Prod_ID
,SUM(Medida_Linea) AS Operarios_Linea
FROM gb_smntc_mexico_costoproducir.CP_Operarios
WHERE Periodo = '${VAR:VAR_PERIODO}'
AND EntidadLegal_ID IN (${VAR:VAR_EL})
GROUP BY 1,2,3,4;

---------------------------------------------
---- ext_big_data_59_importes
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_59_importes
SELECT SN.Entidadlegal_ID, SN.Areanegocio_id, SN.Centrocostos_id
    ,SUM(Coalesce(Montodebito,0) - Coalesce(Montocredito,0)) AS Monto
    FROM gb_smntc_mexico_costoproducir.A_Saldo_Nomina SN
    INNER JOIN (
        SELECT distinct entidadlegal_id, planta_id, centroCostos_id FROM 
        gb_mdl_mexico_manufactura.MF_Lineas_Prod_Centro_Costos C 
        WHERE EntidadLegal_ID IN (${VAR:VAR_EL})
        and fecha_Fin is null
        and dl in (1)
        ) CC
    ON SN.EntidadLegal_ID = CC.EntidadLegal_ID 
    AND SN.AreaNegocio_ID = CC.Planta_ID 
    AND SN.CentroCostos_ID = CC.CentroCostos_ID 
    WHERE SN.EntidadLegal_ID IN (${VAR:VAR_EL})
    AND SN.CuentaNatural_ID >=  '6000' 
    AND SN.CuentaNatural_ID <=  '6999'
    AND NOT (SN.CuentaNatural_ID >= '6300' AND  SN.CuentaNatural_ID < '6600')
    AND NOT (SN.CuentaNatural_ID = '6997')
    AND NOT (SN.CuentaNatural_ID = '6717' AND SN.CentroCostos_ID = '0153')
    AND cast(substr(SN.Fechamovimiento,1,7) as string) = '${VAR:VAR_PERIODO}'
    AND SN.Status = 'P'
    AND SN.Hdr_status = 'P'
    AND SN.Hdr_posted_date IS NOT NULL
    AND (SN.Hdr_je_source) NOT IN ('Spreadsheet', 'Manual', 'Other')
    AND Categoriaencabezado_je <> 'Budget'
AND Flagencabezado = 'A'
    GROUP BY 1,2,3
    HAVING monto <> 0;

---------------------------------------------
---- ext_big_data_81_importes_caso1
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_81_importes_caso1
SELECT    
B.Periodo
,B.EntidadLegal_ID
,B.Planta_ID
,Producto_ID
,1 AS TipoCasoSubrubro_ID
,SUM(B.importe) AS Importe
from
gb_smntc_mexico_costoproducir.V_Rubro81_Costo_Inv B
INNER JOIN 
(SELECT EntidadLegal_ID, MF_Producto_ID, TRIM(Producto_ID) AS Producto_ID
  FROM gb_mdl_mexico_manufactura.MF_Producto_Organizacion
  WHERE Origen IN('MEXICO','CA')
  AND Tipo_producto_id = 1  -- Solo traemos PTs
  GROUP BY 1,2,3) P
ON B.MF_Producto_ID = CAST(P.MF_Producto_ID AS INT) AND B.EntidadLegal_ID = P.EntidadLegal_ID
where 
 B.EntidadLegal_ID IN (${VAR:VAR_EL})
AND B.Periodo = '${VAR:VAR_PERIODO}'
GROUP BY 1,2,3,4;

---------------------------------------------
---- ext_big_data_81_importes_caso2
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_81_importes_caso2
SELECT 
A.Periodo
,A.EntidadLegal_ID
,A.Planta_ID
,E.Producto_ID AS Ingrediente_id
,2 AS TipoCasoSubrubro_ID
,SUM(ZEROIFNULL((A.Importe * C.Medida) / NULLIFZERO(D.Medida))) AS Importe
from 
gb_smntc_mexico_costoproducir.V_Rubro81_Costo_Inv A, 
gb_mdl_mexico_manufactura.MF_Producto_Organizacion B,
gb_smntc_mexico_costoproducir.V_Rubro81_PRODS C,
gb_smntc_mexico_costoproducir.V_Rubro81_PLANTAS D,
(SELECT EntidadLegal_ID, MF_Producto_ID, TRIM(Producto_ID) AS Producto_ID
FROM gb_mdl_mexico_manufactura.MF_Producto_Organizacion
WHERE Origen IN('MEXICO','CA')
GROUP BY 1,2,3) E
WHERE C.Periodo               = A.Periodo 
AND C.EntidadLegal_ID         = A.EntidadLegal_ID 
AND C.MF_Organizacion_ID      = A.MF_Organizacion_ID 
AND C.Planta_ID               = A.Planta_ID 
AND C.Ingrediente_ID          = A.MF_Producto_ID 
AND C.Periodo                 = D.Periodo 
AND C.EntidadLegal_ID         = D.EntidadLegal_ID 
AND C.MF_Organizacion_ID      = D.MF_Organizacion_ID 
AND C.Planta_ID               = D.Planta_ID 
AND C.Ingrediente_ID          = D.Ingrediente_ID 
AND C.EntidadLegal_ID         = B.EntidadLegal_ID 
AND C.MF_Organizacion_ID      = B.MF_Organizacion_ID  
AND C.Planta_ID               = B.Planta_ID 
AND C.Ingrediente_ID          = B.MF_Producto_ID 
AND C.EntidadLegal_ID         = E.EntidadLegal_ID 
AND C.Ingrediente_ID          = E.MF_Producto_ID 
AND B.Tipo_Producto_ID        = 3
AND D.Medida                  <> 0
AND A.Tsubinven               = 'MP'
AND A.EntidadLegal_ID IN (${VAR:VAR_EL})
AND A.Periodo                 = '${VAR:VAR_PERIODO}'
GROUP BY 1,2,3,4;


---------------------------------------------
---- ext_big_data_81_importes_caso3 qry_validado_inserta
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_81_importes_caso3
select 
W.Periodo
,W.EntidadLegal_ID
,W.Planta_ID
,E.Producto_ID as Ingrediente_ID
,SUM(Importe) as importe 
FROM 
(SELECT   
B.Periodo,
B.EntidadLegal_ID
,B.Planta_ID
,B.MF_Producto_ID  
,SUM(B.Importe) as importe     
FROM    
gb_smntc_mexico_costoproducir.V_Rubro81_Costo_Inv B
INNER JOIN gb_smntc_mexico_costoproducir.V_Rubro81_Ing_SE C 
ON C.Periodo = B.Periodo 
AND C.EntidadLegal_ID = B.EntidadLegal_ID 
AND C.MF_Organizacion_ID = B.MF_Organizacion_ID 
WHERE B.EntidadLegal_ID IN (${VAR:VAR_EL})
AND B.Periodo = '${VAR:VAR_PERIODO}' AND B.Tsubinven = 'SE'
AND B.MF_Producto_ID = C.SubEnsamble_ID 
AND B.Tsubinven = 'SE'
AND C.Medida <> 0
AND B.Periodo  = '${VAR:VAR_PERIODO}'
AND B.EntidadLegal_ID IN (${VAR:VAR_EL})
GROUP BY 1,2,3,4
) W
left outer join
(SELECT EntidadLegal_ID, MF_Producto_ID, TRIM(Producto_ID) AS Producto_ID
FROM gb_mdl_mexico_manufactura.MF_Producto_Organizacion
WHERE Origen IN('MEXICO','CA')
GROUP BY 1,2,3) E
ON W.EntidadLegal_ID         = E.EntidadLegal_ID 
AND W.MF_Producto_ID          = E.MF_Producto_ID 
GROUP BY 1,2,3,4;


---------------------------------------------
---- ext_big_data_81_importes_caso4
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_81_importes_caso4
SELECT
          A.Periodo
          ,A.EntidadLegal_ID
          ,A.MF_Organizacion_ID
          ,A.Planta_ID
      ,SUM(A.importe) AS Monto_Producto   -- Importe
FROM gb_smntc_mexico_costoproducir.V_Rubro81_Costo_Inv A 
LEFT OUTER JOIN gb_smntc_mexico_costoproducir.CP_Medidas_Prorrateo B ON 
   A.Periodo  = B.Periodo 
   AND A.EntidadLegal_ID = B.EntidadLegal_ID
   AND A.MF_Organizacion_ID = B.MF_Organizacion_ID
   AND A.MF_Producto_ID = B.MF_Producto_ID
   AND B.TipoMedida_ID = 5
INNER JOIN (SELECT EntidadLegal_ID, TRIM(Condicion) AS Condicion 
   FROM gb_smntc_mexico_costoproducir.CP_Parametros  
   WHERE EntidadLegal_ID IN (${VAR:VAR_EL}) AND Objeto = 'V_Rubro81_Costo_Inv' 
   AND Campo = 'TSubInven' 
   AND Subrubro_ID = 81 
   GROUP BY 1,2) C ON A.EntidadLegal_ID = C.EntidadLegal_ID
  AND A.TSubInven = TRIM(C.Condicion)
  WHERE B.MF_Organizacion_ID is null
  AND A.Periodo = '${VAR:VAR_PERIODO}'
  AND A.EntidadLegal_ID IN (${VAR:VAR_EL})
  GROUP BY 1,2,3,4;

---------------------------------------------
---- ext_big_data_81_importes_caso5
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_81_importes_caso5
SELECT A.entidadLegal_ID
,A.Periodo
,A.Planta_ID
,5 AS TipoCasoSubrubro_ID
,SUM(A.importe) AS importe
FROM  gb_smntc_mexico_costoproducir.V_Rubro81_Costo_Inv A 
LEFT OUTER JOIN gb_smntc_mexico_costoproducir.V_Rubro81_PRODS B 
ON A.EntidadLegal_ID = B.EntidadLegal_ID 
AND A.MF_Organizacion_ID = B.MF_Organizacion_ID 
AND A.MF_Producto_ID = B.Ingrediente_ID 
AND A.Periodo = B.Periodo 
WHERE  A.TSubInven = 'MP' 
AND  B.MF_Organizacion_ID IS NULL
AND  A.Periodo = '${VAR:VAR_PERIODO}'
AND  A.EntidadLegal_ID IN (${VAR:VAR_EL})
GROUP BY 1,2,3,4;


---------------------------------------------
---- ext_big_data_81_importes_caso6
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_81_importes_caso6
SELECT    
A.Periodo
,A.EntidadLegal_ID
,A.MF_Organizacion_ID
,A.Planta_ID
,SUM(A.importe) AS Monto_Producto --Importe
FROM gb_smntc_mexico_costoproducir.V_Rubro81_Costo_Inv A 
LEFT OUTER JOIN 
     (
          SELECT     
               A.Periodo,
               A.EntidadLegal_ID, 
               A.MF_Organizacion_ID, 
               A.Planta_ID, 
               A.SubEnsamble_ID as Ingrediente_ID,
               D.Medida_Producto * A.Cantidad Medida
            FROM gb_smntc_mexico_costoproducir.CP_Medidas_Prorrateo D, gb_smntc_mexico_costoproducir.V_MF_Formulas_SE A 
            WHERE D.Periodo = A.Periodo 
             AND D.EntidadLegal_ID = A.EntidadLegal_ID 
               AND D.MF_Organizacion_ID = A.MF_Organizacion_ID 
               AND D.MF_Producto_ID = A.MF_Producto_ID 
               AND D.Periodo = '${VAR:VAR_PERIODO}'
               AND A.EntidadLegal_ID IN (${VAR:VAR_EL})
               AND (D.TipoMedida_ID = 5)
     ) B
     ON A.EntidadLegal_ID = B.EntidadLegal_ID 
AND A.MF_Organizacion_ID = B.MF_Organizacion_ID 
AND A.MF_Producto_ID = B.Ingrediente_ID AND A.Periodo = B.Periodo 
WHERE A.TSubInven = 'SE'  
AND B.MF_Organizacion_ID IS NULL 
AND A.Periodo = '${VAR:VAR_PERIODO}' 
AND A.EntidadLegal_ID IN (${VAR:VAR_EL})
GROUP BY 1,2,3,4;

---------------------------------------------
---- ext_big_data_82_importe_costo_capital
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_82_importe_costo_capital
SELECT A.EntidadLegal_ID, ZEROIFNULL(CAST(COALESCE(MAX(CAST(A.Valor AS FLOAT)),0) AS FLOAT)/100) AS Costo_Capital
 FROM gb_mdl_mexico_manufactura.MF_Parametro A, (SELECT EntidadLegal_ID, MAX(Fecha) AS Fecha 
            FROM gb_mdl_mexico_manufactura.MF_Parametro 
            WHERE EntidadLegal_ID IN (${VAR:VAR_EL}) AND Tipo_Parametro_ID = 12 GROUP BY 1) B
WHERE A.EntidadLegal_ID=B.EntidadLegal_ID
AND A.Fecha=B.Fecha
 AND A.EntidadLegal_ID IN (${VAR:VAR_EL})
 AND A.Tipo_Parametro_ID = 12 
 GROUP BY 1;

---------------------------------------------
---- ext_big_data_costo_estandar
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_costo_estandar
SELECT 
cp.entidadlegal_id,
cp.planta_id,
po.producto_id,
cp.Tipo_Costo_ID,
cp.costo
FROM gb_mdl_mexico_manufactura.MF_Costo_Prod CP
inner join gb_mdl_mexico_manufactura.mf_producto_organizacion po
on  po.mf_producto_id = Cp.mf_producto_id
and po.entidadlegal_id =  CP.entidadlegal_id 
and po.mf_organizacion_id = CP.mf_organizacion_id
WHERE CP.entidadlegal_id IN (${VAR:VAR_EL})
and CP.periodo = '${VAR:VAR_PERIODO}'
and po.tipo_producto_id = 1;

---------------------------------------------
---- ext_big_data_drivers_linea
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_drivers_linea
SELECT EntidadLegal_ID, Planta_ID, Linea_Prod_ID,  TipoMedida_ID, sum(Medida_Linea) as operarios
FROM gb_smntc_mexico_costoproducir.CP_Operarios    
WHERE periodo = '${VAR:VAR_PERIODO}'
and  EntidadLegal_ID IN (${VAR:VAR_EL})
group by 1,2,3,4;

---------------------------------------------
---- ext_big_data_drivers_prorrateo
---------------------------------------------

--insert
insert overwrite jedoxMexico.ext_big_data_drivers_prorrateo
SELECT M.EntidadLegal_ID, M.Planta_ID,
        M.Linea_Prod_ID, M.Turno_ID, Coalesce(cast(P.Producto_ID as INT), M.MF_producto_id) as Producto_ID, M.TipoMedida_ID, 
        M.Factor, M.Medida_Factor, 
  (case when M.TipoMedida_ID = 6 then M.Medida_Linea else M.Medida_Producto end) as Medida_Producto,
  M.Turnos_Linea_Produccion
FROM gb_smntc_mexico_costoproducir.CP_Medidas_Prorrateo M
LEFT OUTER JOIN 
          (SELECT EntidadLegal_ID, MF_Producto_ID, TRIM(Producto_ID) AS Producto_ID
               FROM gb_mdl_mexico_manufactura.MF_Producto_Organizacion
               WHERE Origen IN('MEXICO','CA')
               GROUP BY 1,2,3) P
ON M.MF_Producto_ID = CAST(P.MF_Producto_ID AS INT) AND M.EntidadLegal_ID = P.EntidadLegal_ID
WHERE M.entidadlegal_id in (${VAR:VAR_EL})
and M.periodo = '${VAR:VAR_PERIODO}'
and NOT (P.producto_id is null and cast(M.MF_Producto_ID as string) <> '-1');


---------------------------------------------
---- ext_big_data_drivers_prorrateo2
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_drivers_prorrateo2
SELECT M.EntidadLegal_ID, M.Planta_ID,M.Linea_Prod_ID, M.Turno_ID, M.TipoMedida_ID,1 as TurnosLinea
FROM gb_smntc_mexico_costoproducir.CP_Medidas_Prorrateo M
INNER JOIN 
(SELECT EntidadLegal_ID, MF_Producto_ID, TRIM(Producto_ID) AS Producto_ID
     FROM gb_mdl_mexico_manufactura.MF_Producto_Organizacion
     WHERE Origen IN('MEXICO','CA')
     AND Tipo_producto_id = 1   -- Solo traemos PTs
     GROUP BY 1,2,3) P
ON M.MF_Producto_ID = CAST(P.MF_Producto_ID AS INT) 
AND M.EntidadLegal_ID = P.EntidadLegal_ID
WHERE M.entidadlegal_id in (${VAR:VAR_EL})
and M.periodo = '${VAR:VAR_PERIODO}'
group by 1,2,3,4,5;

---------------------------------------------
---- ext_big_data_formulas
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_data_formulas
select
0 as EsSubensamble
,prod.entidadlegal_id
,prod.planta_id
,prod.producto_id
,cast(prod.mf_producto_id as string)
,cast(prod.mf_organizacion_id as string)
,cast(prod.ingrediente_id as string) as mf_ingrediente_id
,poi.producto_id  as  ingrediente_id
,case when tipo_producto_id = 3 and  indicador_eye = 1 then 4 else tipo_producto_id end  as tipo_producto
,prod.cantidad
,prod.costoreal
,prod.costoestandar
from
(
select
f.planta_id  as planta_id 
,po.producto_id  as  producto_id
,f.mf_producto_id as mf_producto_id
,cast(f.cantidad  as decimal(20,10)) as  cantidad
,cast(f.costoreal  as decimal (20,10)) as  costoreal
,cast(f.costoestandar  as decimal (20,10)) as  costoestandar
,f.ingrediente_id  as  ingrediente_id
,f.entidadlegal_id  as  entidadlegal_id
,f.mf_organizacion_id  as  mf_organizacion_id
from gb_mdl_mexico_manufactura.mf_formulas  f
inner join
gb_mdl_mexico_manufactura.mf_plantas  p
on  f.entidadlegal_id = p.entidadlegal_id
and  f.mf_organizacion_id = p.mf_organizacion_id
and  f.planta_id = p.planta_id
and  p.sistema_fuente = 'CP'
inner  join
gb_mdl_mexico_manufactura.mf_producto_organizacion po
on  f.entidadlegal_id = po.entidadlegal_id
and  f.mf_organizacion_id = po.mf_organizacion_id
and  f.mf_producto_id = po.mf_producto_id                                                

where f.EntidadLegal_ID in (${VAR:VAR_EL})
and cast(substr(f.fecha,1,7) as string) = '${VAR:VAR_PERIODO}'
) prod
inner join
gb_mdl_mexico_manufactura.mf_producto_organizacion poi
on   prod.entidadlegal_id = poi.entidadlegal_id
and  prod.mf_organizacion_id = poi.mf_organizacion_id
and  prod.ingrediente_id = poi.mf_producto_id

UNION ALL 
select
1 as EsSubensamble
,prod.entidadlegal_id
,prod.planta_id 
,prod.producto_id
,cast(prod.mf_producto_id as string)
,cast(prod.mf_organizacion_id as string)
,cast(prod.ingrediente_id as string) as mf_ingrediente_id
,poi.producto_id  as  ingrediente_id
,case when tipo_producto_id = 3 and  indicador_eye = 1 then 4 else tipo_producto_id end  as tipo_producto
,prod.cantidad
,0 as costoreal
,prod.costoestandar
from
(
select
f.planta_id  as planta_id 
,po.producto_id  as  producto_id
,f.mf_producto_id as mf_producto_id
,cast (f.cantidad  as decimal (20,14)) as  cantidad
,cast (f.costoestandar  as decimal (20,12)) as  costoestandar
,f.subensamble_id as  ingrediente_id
,f.entidadlegal_id  as  entidadlegal_id
,f.mf_organizacion_id  as  mf_organizacion_id
from gb_mdl_mexico_manufactura.mf_formulas_se  f
inner join
gb_mdl_mexico_manufactura.mf_plantas  p
on f.entidadlegal_id = p.entidadlegal_id
and f.mf_organizacion_id = p.mf_organizacion_id
and f.planta_id = p.planta_id
and p.sistema_fuente = 'CP'
inner  join
gb_mdl_mexico_manufactura.mf_producto_organizacion po
on f.entidadlegal_id = po.entidadlegal_id
and f.mf_organizacion_id = po.mf_organizacion_id
and f.mf_producto_id = po.mf_producto_id                                                
where f.EntidadLegal_ID in (${VAR:VAR_EL})
and cast(substr(f.fecha,1,7) as string) = '${VAR:VAR_PERIODO}'
) prod
inner join
gb_mdl_mexico_manufactura.mf_producto_organizacion poi
on   prod.entidadlegal_id = poi.entidadlegal_id
and  prod.mf_organizacion_id = poi.mf_organizacion_id
and  prod.ingrediente_id = poi.mf_producto_id;


---------------------------------------------
---- ext_big_data_tipocambio
---------------------------------------------
insert overwrite jedoxMexico.ext_big_data_tipocambio
SELECT
TM.MonedaOrigen_ID,
TM.MonedaDestino_ID,
TM.TipoCambio
FROM gb_smntc_mexico_costoproducir.A_Tipo_Cambio TM
WHERE TM.MonedaOrigen_ID='mxp'
      AND TM.MonedaDestino_ID ='usd'
      AND(EXTRACT(YEAR FROM TM.FechaTipoCambio) = ${VAR:VAR_ANIO} AND EXTRACT(MONTH FROM TM.FechaTipoCambio) = ${VAR:VAR_MES} )
ORDER BY TM.FechaTipoCambio DESC LIMIT 1;



---------------------------------------------
---- ext_big_data_ultimos_precios
---------------------------------------------
--insert jedoxMexico.ext_big_data_ultimos_precios
insert overwrite jedoxMexico.ext_big_data_ultimos_precios
SELECT f.EntidadLegal_ID, 
f.planta_id, 
p.producto_id, 
cast(sum(f.Cantidad*f.CostoReal) as decimal(30,20)) costo_unitario
FROM gb_smntc_mexico_costoproducir.V_MF_Formulas F
inner join 
(
select 
entidadlegal_id, planta_id, MF_Producto_ID, max(periodo) as periodo FROM gb_smntc_mexico_costoproducir.V_MF_Formulas
where  entidadlegal_id IN (${VAR:VAR_EL})
and periodo <= '${VAR:VAR_PERIODO}' -- con esto no tomamos un periodo superior al que estamos tomando para desarrollo and mf_producto_id = 74987 
group by 1,2,3
) L 
ON F.Entidadlegal_id = L.entidadlegal_id 
and f.mf_producto_id = L.mf_producto_id 
and f.periodo = L.periodo 
and f.planta_id = l.planta_id
INNER JOIN 
          (SELECT EntidadLegal_ID, MF_Producto_ID, TRIM(Producto_ID) AS Producto_ID
               FROM gb_smntc_mexico_costoproducir.V_MF_Producto_Organizacion
               WHERE Origen IN('MEXICO','CA')
               AND Tipo_producto_id = 1       -- Solo traemos PTs
               GROUP BY 1,2,3) P
ON F.MF_Producto_ID = CAST(P.MF_Producto_ID AS INT) AND F.EntidadLegal_ID = P.EntidadLegal_ID
where f.entidadlegal_id IN (${VAR:VAR_EL})
group by 1,2,3;


---------------------------------------------
---- ext_big_dim_centros_costo
---------------------------------------------
--insert
insert overwrite jedoxMexico.ext_big_dim_centros_costo
SELECT 
LC.EntidadLegal_ID,
LC.CentroCostos_ID
FROM gb_smntc_mexico_costoproducir.v_mf_lineas_prod_centro_costos LC
WHERE LC.EntidadLegal_Id IN (${VAR:VAR_EL})
AND LC.Fecha_Fin is null 
GROUP BY 1,2;

---------------------------------------------
---- ext_big_dim_entidad_legal
---------------------------------------------

insert overwrite jedoxMexico.ext_big_dim_entidad_legal 
SELECT EntidadLegal_ID
FROM gb_smntc_mexico_costoproducir.CP_Medidas_Prorrateo 
WHERE periodo = '${VAR:VAR_PERIODO}'
and  EntidadLegal_ID in (${VAR:VAR_EL})
group by 1;


---------------------------------------------
---- ext_big_dim_ingredientes
---------------------------------------------
insert overwrite jedoxMexico.ext_big_dim_ingredientes
select 
poi.EntidadLegal_ID
,cast('-1' as string) as  ingrediente_id
,cast('-1' as string) as descripcion
from gb_mdl_mexico_manufactura.mf_producto_organizacion poi
group by 1,2,3

UNION ALL

select
poi.entidadlegal_id
,poi.producto_id  as  ingrediente_id
,poi.descripcion as descripcion
from
(
select 
f.mf_organizacion_id
,f.entidadlegal_id
,f.ingrediente_id  as  ingrediente_id
from gb_mdl_mexico_manufactura.mf_formulas  f
inner join gb_mdl_mexico_manufactura.mf_plantas  p on  f.entidadlegal_id = p.entidadlegal_id
and  f.mf_organizacion_id = p.mf_organizacion_id
and  f.planta_id = p.planta_id
and  p.sistema_fuente = 'CP'
inner  join gb_mdl_mexico_manufactura.mf_producto_organizacion po on  f.entidadlegal_id = po.entidadlegal_id
and  f.mf_organizacion_id = po.mf_organizacion_id
and  f.mf_producto_id = po.mf_producto_id                                                
where f.entidadlegal_id in (${VAR:VAR_EL}) and cast(substr(f.fecha,1,7) as string) = '${VAR:VAR_PERIODO}'
group by 1,2,3
) prod
inner join gb_mdl_mexico_manufactura.mf_producto_organizacion poi on   
prod.entidadlegal_id = poi.entidadlegal_id
and  prod.mf_organizacion_id = poi.mf_organizacion_id
and  prod.ingrediente_id = poi.mf_producto_id
group by 1,2,3

UNION ALL 

select 
poi.entidadlegal_id,
poi.producto_id as ingrediente_id,
poi.descripcion as descripcion
from
(
select
f.subensamble_id as  ingrediente_id
,f.entidadlegal_id  as  entidadlegal_id
,f.mf_organizacion_id  as  mf_organizacion_id
from gb_mdl_mexico_manufactura.mf_formulas_se  f
inner join gb_mdl_mexico_manufactura.mf_plantas  p on  f.entidadlegal_id = p.entidadlegal_id
  and  f.mf_organizacion_id = p.mf_organizacion_id
  and  f.planta_id = p.planta_id
  and  p.sistema_fuente = 'CP'
inner  join gb_mdl_mexico_manufactura.mf_producto_organizacion po on  f.entidadlegal_id = po.entidadlegal_id
   and  f.mf_organizacion_id = po.mf_organizacion_id
   and  f.mf_producto_id = po.mf_producto_id                                                
where f.entidadlegal_id in (${VAR:VAR_EL})
and cast(substr(f.fecha,1,7) as string) = '${VAR:VAR_PERIODO}'
group by 1,2,3
) prod
inner join gb_mdl_mexico_manufactura.mf_producto_organizacion poi
on   prod.entidadlegal_id = poi.entidadlegal_id
and  prod.mf_organizacion_id = poi.mf_organizacion_id
and  prod.ingrediente_id = poi.mf_producto_id
group by 1,2,3;


---------------------------------------------
---- ext_big_dim_lineas
---------------------------------------------
insert overwrite jedoxMexico.ext_big_dim_lineas
SELECT 
EntidadLegal_ID,
cast(linea_prod_ID as string) as linea_id
FROM gb_smntc_mexico_costoproducir.CP_Medidas_Prorrateo 
WHERE periodo = '${VAR:VAR_PERIODO}'
and EntidadLegal_ID in (${VAR:VAR_EL})
group by 1,2

union 

SELECT 
EntidadLegal_ID, 
Cast('-1' as string) AS linea_id
FROM gb_smntc_mexico_costoproducir.CP_Medidas_Prorrateo
WHERE EntidadLegal_ID in (${VAR:VAR_EL})
GROUP BY 1,2;
--ORDER BY 1;


---------------------------------------------
---- ext_big_dim_plantas
---------------------------------------------
--Insert
insert overwrite jedoxMexico.ext_big_dim_plantas
SELECT 
EntidadLegal_ID,
Planta_ID, Planta_DS
FROM gb_mdl_mexico_manufactura.MF_Plantas
where entidadlegal_id in (${VAR:VAR_EL})
and sistema_fuente = 'CP'
GROUP BY 1,2,3;


---------------------------------------------
---- ext_big_dim_productos
---------------------------------------------

--Insert
insert overwrite jedoxMexico.ext_big_dim_productos
SELECT  
M.EntidadLegal_ID,
P.Producto_ID, 
P.Descripcion 
FROM gb_smntc_mexico_costoproducir.CP_Medidas_Prorrateo M
INNER JOIN 
      (
   SELECT Entidadlegal_id, MF_Producto_ID, Cast(Trim(Producto_ID) AS string) AS Producto_ID, 
   Cast(Descripcion AS string) AS Descripcion
       FROM gb_mdl_mexico_manufactura.MF_Producto_Organizacion
       WHERE Origen IN('MEXICO','CA')
       AND Tipo_producto_id = 1       
       AND EntidadLegal_ID in (${VAR:VAR_EL})
       GROUP BY 1,2,3,4
      ) P
ON M.MF_Producto_ID = CAST(P.MF_Producto_ID AS INT) AND M.EntidadLegal_ID = P.EntidadLegal_ID
WHERE M.periodo = '${VAR:VAR_PERIODO}'
group by 1,2,3

union 

SELECT     
EntidadLegal_ID,
Cast('-1' as string) AS Producto_ID, 
Cast('producto -1' AS string) AS Descripcion
FROM gb_smntc_mexico_costoproducir.CP_Medidas_Prorrateo
WHERE EntidadLegal_ID in (${VAR:VAR_EL})
GROUP BY 1,2,3;


---------------------------------------------
---- ext_big_dim_tipo_costo_estandar
---------------------------------------------
--Insert
insert overwrite jedoxMexico.ext_big_dim_tipo_costo_estandar
SELECT Tipo_Costo_id, Indicador_Costo, Formula, Tipo_Costo_Desc
FROM gb_smntc_mexico_costoproducir.MF_Tipo_Costo
order by 1;

---------------------------------------------
---- ext_big_data_29_importestotal
---------------------------------------------
--Insert
insert overwrite jedoxMexico.ext_big_data_29_importestotal
SELECT 
Der.EntidadLegal_ID as entidadlegal,
 Der.Planta_ID as plantas,
 -1 as ingredientes,
 sum(Importe) as Importe,
CAST ('29' as INT) as concepto
FROM gb_smntc_mexico_costoproducir.CP_Derivados_Fin Der
left join gb_mdl_mexico_manufactura.mf_producto_organizacion poi on   
        der.entidadlegal_id = poi.entidadlegal_id
        and  der.mf_organizacion_id = poi.mf_organizacion_id
        and  der.ingrediente_id = poi.mf_producto_id
where Der.Periodo = '${VAR:VAR_PERIODO}' and Der.EntidadLegal_id in (${VAR:VAR_EL})
and Der.Fecha_Fin is null
group by 1,2,3;


---------------------------------------------
---- ext_big_data_34_cc_generales
---------------------------------------------
insert overwrite jedoxMexico.ext_big_data_34_cc_generales
SELECT EntidadLegal_ID as entidad_legal,
  Planta_ID as plantas,
  CentroCostos_ID as centrocostos,
  1 as importe
FROM gb_smntc_mexico_costoproducir.V_MF_Lineas_Prod_Centro_Costos
WHERE ENTIDADLEGAL_ID = '100' AND FECHA_FIN IS NULL
AND DL = 0;


---------------------------------------------
---- ext_big_data_43_cc_a_lineas
---------------------------------------------
insert overwrite jedoxMexico.ext_big_data_43_cc_a_lineas
SELECT 
LC.EntidadLegal_Id, 
LC.Planta_ID, 
LC.CentroCostos_ID, 
CASE WHEN ML.MEDIDA_LINEA IS NOT NULL THEN LC.Linea_Prod_ID 
ELSE -1 END AS Linea_Prod_ID
FROM gb_smntc_mexico_costoproducir.V_MF_Lineas_Prod_Centro_Costos LC
LEFT JOIN
(
       SELECT EntidadLegal_Id, MF_Organizacion_ID, Planta_ID, Linea_Prod_ID, Medida_Linea
       FROM gb_smntc_mexico_costoproducir.CP_Medidas_Prorrateo
       WHERE EntidadLegal_Id IN ('100')
       AND TipoMedida_ID = 2 
       AND Periodo = '${VAR:VAR_PERIODO}'
       GROUP BY 1,2,3,4,5
)  ML
ON LC.EntidadLegal_Id = ML.EntidadLegal_Id 
AND LC.MF_Organizacion_ID = ML.MF_Organizacion_ID 
AND LC.Planta_ID = ML.Planta_ID 
AND LC.Linea_Prod_ID = ML.Linea_Prod_ID
WHERE LC.EntidadLegal_Id IN ('100') 
AND LC.Fecha_Fin is null and LC.DL = 1 
GROUP BY 1,2,3,4;


-- ================== MF_FROMULAS HQL ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update "MF_Formulas" information on table cp_dwh_mf.MF_Formulas_SE
-- Subject Area / Area Sujeto : Manufacture 

 
-- ========================================
-- Sumbensambles 
-- =========================================

--SET Paso = 18;
INSERT OVERWRITE TABLE cp_dwh_mf.MF_Formulas_se partition(Entidadlegal_id)
SELECT tmp.* from cp_dwh_mf.MF_Formulas_se tmp 
LEFT OUTER JOIN (select Fecha ,EntidadLegal_ID ,MF_Organizacion_ID ,
MF_Producto_ID,SubEnsamble_ID from cp_dwh_mf.MF_Formulas_se where Fecha='${hiveconf:V_PRIMER_DIA}') sec
on tmp.Fecha=sec.Fecha
and tmp.EntidadLegal_ID=sec.EntidadLegal_ID
and tmp.MF_Organizacion_ID=sec.MF_Organizacion_ID
and tmp.MF_Producto_ID=sec.MF_Producto_ID
and tmp.SubEnsamble_ID=sec.SubEnsamble_ID
where sec.Fecha is null
and sec.EntidadLegal_ID is null
and sec.MF_Organizacion_ID is null
and sec.MF_Producto_ID is null
and sec.SubEnsamble_ID is null
and tmp.entidadlegal_id IN (SELECT EntidadLegal_ID FROM cp_view.v_entidadeslegales_activas_for
WHERE TRIM(Aplicacion) = 'FORMULAS' GROUP BY EntidadLegal_ID);


--SET Paso = 19;
INSERT INTO cp_dwh_mf.MF_Formulas_SE PARTITION(entidadlegal_id)
SELECT concat(a.periodo,'-01')
,a.planta_id MF_Organizacion_ID
,MF_Plantas_0.planta_id
,a.producto_id MF_Producto_ID
,a.invitem_hijos SubEnsamble_ID
,a.cantidad
,COALESCE(b.costo,0)
,COALESCE(TipoMoneda_ID,-1)
,FROM_UNIXTIME(UNIX_TIMESTAMP())
,FROM_UNIXTIME(UNIX_TIMESTAMP())
,FROM_UNIXTIME(UNIX_TIMESTAMP())
,a.entidadlegal_id
FROM 
(SELECT a.periodo, 
a.entidadlegal_id, 
a.planta_id, 
a.producto_id, 
a.invitem_hijos, 
SUM(a.cantidad) cantidad
FROM (
SELECT a.periodo, 
a.entidadlegal_id, 
a.planta_id, 
a.producto_id, 
a.invitem_hijos , 
CAST(ABS(a.cantidadpzasH/ a.cantidadpzasp) AS DECIMAL(18,6)) Cantidad
FROM cp_dwh.FORM_ODS_CM_A_01_2  a 
WHERE a.cantidadpzasp<>0
AND a.codigosubinv LIKE '%SUB%'

UNION ALL

SELECT A.Periodo
,A.EntidadLegal_ID
,A.Planta_ID
,A.SubEnsamble_ID_Ori as producto_id
,A.Ingrediente_ID as invitem_hijos
,SUM(fact) as Cantidad
FROM (
SELECT DISTINCT
 A.Periodo
 ,A.EntidadLegal_ID
 ,A.Planta_ID
 ,A.SubEnsamble_ID_Ori
 ,A.Ingrediente_ID
 ,CAST(A.fact AS DECIMAL(18,6)) as fact
FROM cp_dwh.T_FORM_STG_SUBENSFORM_CP A,
cp_dwh.MTL_CATALOGO_MATERIALES B,
(SELECT DISTINCT a.periodo, a.entidadlegal_id, a.planta_id, a.producto_id FROM cp_dwh.FORM_ODS_CM_A_01_2 a) C
WHERE B.ORGANIZATION_ID=a.Planta_ID
AND a.SubEnsamble_ID_Ori=B.INVENTORY_ITEM_ID
AND B.ITEM_TYPE='PT'
AND A.periodo=C.periodo
AND A.entidadlegal_id=C.entidadlegal_id
AND A.planta_id=C.planta_id
AND A.SubEnsamble_ID_Ori=C.producto_id
AND A.CodigoSubInv LIKE '%SUB%'
) A
GROUP BY A.Periodo, A.EntidadLegal_ID, A.Planta_ID, A.SubEnsamble_ID_Ori, A.Ingrediente_ID 
) A
GROUP BY a.periodo,a.entidadlegal_id,a.planta_id,a.producto_id,a.invitem_hijos
) A 
LEFT OUTER JOIN cp_app_costoproducir.v_mf_plantas MF_Plantas_0 
ON MF_Plantas_0.MF_Organizacion_ID = a.Planta_ID
LEFT OUTER JOIN ( SELECT c.Periodo, c.EntidadLegal_ID, c.MF_Organizacion_ID, c.planta_id, c.MF_Producto_ID, SUM(c.Costo) Costo
  FROM cp_dwh_mf.MF_Costo_Prod c
  GROUP BY c.Periodo, c.EntidadLegal_ID, c.MF_Organizacion_ID, c.planta_id, c.MF_Producto_ID
  ) B ON A.periodo=B.periodo
  AND A.entidadlegal_id=B.entidadlegal_id
  AND A.planta_id=B.MF_Organizacion_ID
  AND A.invitem_hijos=B.MF_Producto_ID  
-- Obtenemos Tipo Moneda
LEFT OUTER JOIN  (
 SELECT 
  O.EntidadLegal_ID AS EntidadLegal_ID
  ,COALESCE(M.TipoMoneda_ID,-1) AS TipoMoneda_ID
 FROM cp_dwh.O_ENTIDADLEGAL_ORGANIZACION O
 LEFT OUTER JOIN cp_dwh.O_ENTIDAD_LEGAL El 
 ON O.EntidadLegal_ID = EL.EntidadLegal_ID
 LEFT OUTER JOIN cp_dwh.V_TIPO_MONEDA M 
 ON M.Pais_ID = O.Pais_ID) TM
ON a.EntidadLegal_ID = TM.EntidadLegal_ID
where MF_Plantas_0.EntidadLegal_ID IN (SELECT c.EntidadLegal_ID 
FROM cp_view.V_ENTIDADESLEGALES_ACTIVAS_FOR c
 WHERE TRIM(c.Aplicacion) = 'FORMULAS' GROUP BY c.EntidadLegal_ID);

-- compactacion
insert overwrite table cp_dwh_mf.mf_formulas_se partition(entidadlegal_id) select tmp.* from cp_dwh_mf.mf_formulas_se tmp join (select fecha, entidadlegal_id, mf_organizacion_id, mf_producto_id, subensamble_id, max(storeday) as first_record from cp_dwh_mf.mf_formulas_se group by fecha, entidadlegal_id, mf_organizacion_id, mf_producto_id, subensamble_id) sec on tmp.fecha = sec.fecha and tmp.entidadlegal_id = sec.entidadlegal_id and tmp.mf_organizacion_id = sec.mf_organizacion_id and tmp.mf_producto_id = sec.mf_producto_id and tmp.subensamble_id = sec.subensamble_id and tmp.storeday = sec.first_record;
insert overwrite table cp_dwh_mf.mf_formulas_se partition(entidadlegal_id) select distinct * from cp_dwh_mf.mf_formulas_se;

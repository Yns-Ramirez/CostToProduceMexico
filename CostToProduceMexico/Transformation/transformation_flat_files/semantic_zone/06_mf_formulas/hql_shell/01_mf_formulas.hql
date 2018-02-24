-- ================== MF_FROMULAS HQL ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update "MF_Formulas" information on table cp_dwh_mf.MF_Formulas
-- Subject Area / Area Sujeto : Manufacture 


--============================================ 
-- Fin deflactacion
--============================================ 

--SET Paso = 16;
INSERT OVERWRITE TABLE cp_dwh_mf.MF_Formulas partition(Entidadlegal_id)
SELECT tmp.* from cp_dwh_mf.MF_Formulas tmp 
LEFT OUTER JOIN (select Fecha,EntidadLegal_ID,MF_Organizacion_ID,
MF_Producto_ID,Ingrediente_ID from cp_dwh_mf.MF_Formulas where Fecha='${hiveconf:V_PRIMER_DIA}') sec 
on tmp.Fecha=sec.Fecha
and tmp.EntidadLegal_ID=sec.EntidadLegal_ID
and tmp.MF_Organizacion_ID=sec.MF_Organizacion_ID
and tmp.MF_Producto_ID=sec.MF_Producto_ID
and tmp.Ingrediente_ID=sec.Ingrediente_ID
where sec.Fecha is null
and sec.EntidadLegal_ID is null
and sec.MF_Organizacion_ID is null
and sec.MF_Producto_ID is null
and sec.Ingrediente_ID is null
and tmp.entidadlegal_id IN (SELECT EntidadLegal_ID FROM cp_view.v_entidadeslegales_activas_for
WHERE TRIM(Aplicacion) = 'FORMULAS' GROUP BY EntidadLegal_ID);


--SET Paso = 17;
INSERT INTO cp_dwh_mf.MF_Formulas partition(Entidadlegal_id)
SELECT concat(a.Periodo,'-01')
,MF_Plantas_0.MF_Organizacion_ID
,MF_Plantas_0.Planta_ID
,a.Producto_ID
,a.Ingrediente_ID
,a.Cantidad
,a.CostoReal
,a.CostoEstandar
,TipoMoneda_ID
,CASE WHEN a.TMoneda_ID= -1 THEN 1 
ELSE NULL END as Ajuste_Flag
,FROM_UNIXTIME(UNIX_TIMESTAMP())
,FROM_UNIXTIME(UNIX_TIMESTAMP())
,FROM_UNIXTIME(UNIX_TIMESTAMP())
,a.EntidadLegal_ID
FROM cp_dwh.T_F_FORMULAS a 
--INNER JOIN cp_dwh_mf.mf_plantas MF_Plantas_0 
INNER JOIN cp_app_costoproducir.v_mf_plantas MF_Plantas_0 
ON MF_Plantas_0.MF_Organizacion_ID = a.Planta_ID 
AND MF_Plantas_0.EntidadLegal_ID = a.EntidadLegal_ID
-- Obtenemos Tipo Moneda
LEFT OUTER JOIN 
(
   SELECT 
        O.EntidadLegal_ID AS EntidadLegal_ID
        ,COALESCE(M.TipoMoneda_ID,-1) AS TipoMoneda_ID
   FROM cp_dwh.O_ENTIDADLEGAL_ORGANIZACION O
   LEFT OUTER JOIN cp_dwh.O_ENTIDAD_LEGAL El 
   ON O.EntidadLegal_ID = EL.EntidadLegal_ID
   LEFT OUTER JOIN cp_dwh.V_TIPO_MONEDA M 
   ON M.Pais_ID = O.Pais_ID
) TM
ON a.EntidadLegal_ID = TM.EntidadLegal_ID
WHERE a.Periodo= '${hiveconf:V_PERIODO}';

-- compactacion
insert overwrite table cp_dwh_mf.mf_formulas partition(entidadlegal_id) select tmp.* from cp_dwh_mf.mf_formulas tmp join (select fecha, entidadlegal_id, mf_organizacion_id, mf_producto_id, ingrediente_id, max(storeday) as first_record from cp_dwh_mf.mf_formulas group by fecha, entidadlegal_id, mf_organizacion_id, mf_producto_id, ingrediente_id) sec on tmp.fecha = sec.fecha and tmp.entidadlegal_id = sec.entidadlegal_id and tmp.mf_organizacion_id = sec.mf_organizacion_id and tmp.mf_producto_id = sec.mf_producto_id and tmp.ingrediente_id = sec.ingrediente_id and tmp.storeday = sec.first_record;
insert overwrite table cp_dwh_mf.mf_formulas partition(entidadlegal_id) select distinct * from cp_dwh_mf.mf_formulas;

CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_mf_plantas AS SELECT p.mf_organizacion_id,p.planta_id,p.gerencia_id,p.planta_ds,p.sistema_fuente,p.organizacioninventario,p.siglas,p.storeday,p.entidadlegal_id
FROM gb_mdl_mexico_manufactura.MF_Plantas P
WHERE p.sistema_fuente = 'CP';


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.a_rubros_fsg AS SELECT
sld.aniosaldo
,sld.messaldo
,fsg.linea_id
,fsg.nombreconcepto
,sld.areanegocio_id
,sld.cuentanatural_id
,sld.centrocostos_id
,sld.analisislocal_id
,SUM(CASE WHEN fsg.signo = '-' 
THEN (-1)*sld.actividaddelperiodo 
ELSE sld.actividaddelperiodo END) TOT_mActividaddelPeriodo
,FROM_UNIXTIME(UNIX_TIMESTAMP()) as storeday
,sld.entidadlegal_id
FROM 
(
SELECT a_saldo.aniosaldo, a_saldo.messaldo, a_saldo.areanegocio_id, a_saldo.cuentanatural_id, a_saldo.analisislocal_id, a_saldo.centrocostos_id, a_saldo.intercost_id, a_saldo.segment8, a_saldo.segment9, a_saldo.segment10, a_saldo.juegolibros_id, a_saldo.marca_id, a_saldo.presupuesto, a_saldo.balanceinicial, a_saldo.actividaddelperiodo, a_saldo.balancefinal, a_saldo.creditodelperiodo, a_saldo.debitodelperiodo, a_saldo.storeday, a_saldo.entidadlegal_id 
FROM gb_mdl_mexico_costoproducir.A_SALDO 
WHERE  a_saldo.presupuesto = 0
AND 
(
(a_saldo.entidadlegal_id = '100' AND a_saldo.juegolibros_id IN (141,161))
OR
(a_saldo.entidadlegal_id = '101' AND a_saldo.juegolibros_id IN (141,161))
OR
(EntidadLegal_ID = '125' AND JuegoLibros_ID = 641)
)
) SLD,
(
SELECT
rfd.reporte_id
,rfd.linea_id
,rf.nombreconcepto
,rfd.signo
,COALESCE(rfd.i_areanegocio_id,'0000') I_AreaNegocio_IDD, COALESCE(rfd.f_areanegocio_id,'9999') F_AreaNegocio_IDD
,COALESCE(rfd.i_cuentanatural_id,'0000') I_CuentaNatural_IDD, COALESCE(rfd.f_cuentanatural_id,'9999') F_CuentaNatural_IDD
,COALESCE(rfd.i_centrocostos_id,'0000') I_CentroCostos_IDD, COALESCE(rfd.f_centrocostos_id,'9999') F_CentroCostos_IDD
,COALESCE(rfd.i_marca_id,'000') I_Marca_IDD, COALESCE(rfd.f_marca_id,'999') F_Marca_IDD
,COALESCE(rfd.i_analisislocal_id,'0000') I_AnalisisLocal_IDD, COALESCE(rfd.f_analisislocal_id,'9999') F_AnalisisLocal_IDD
FROM gb_mdl_mexico_costoproducir_views.VDW_A_REPORTE_FINANCIERO_DTL RFD, 
(  SELECT vdw_a_reporte_financiero.reporte_id, vdw_a_reporte_financiero.linea_id, vdw_a_reporte_financiero.codigoconcepto, vdw_a_reporte_financiero.nombreconcepto, vdw_a_reporte_financiero.displayflag 
FROM gb_mdl_mexico_costoproducir_views.VDW_A_REPORTE_FINANCIERO 
WHERE TRIM(vdw_a_reporte_financiero.displayflag) = 'Y' 
AND vdw_a_reporte_financiero.reporte_id = 10001
AND vdw_a_reporte_financiero.linea_id IN (25,30,45,95)
) RF 
WHERE rf.linea_id = rfd.linea_id 
AND rf.reporte_id = rfd.reporte_id
) FSG 
WHERE  sld.entidadlegal_id IN ('100','101','125') 
AND sld.presupuesto = 0
AND sld.areanegocio_id BETWEEN fsg.i_areanegocio_idd AND fsg.f_areanegocio_idd
AND sld.cuentanatural_id BETWEEN fsg.i_cuentanatural_idd AND fsg.f_cuentanatural_idd
AND sld.centrocostos_id BETWEEN fsg.i_centrocostos_idd AND fsg.f_centrocostos_idd
AND sld.marca_id BETWEEN fsg.i_marca_idd AND fsg.f_marca_idd
AND sld.analisislocal_id BETWEEN fsg.i_analisislocal_idd AND fsg.f_analisislocal_idd
GROUP BY sld.aniosaldo,sld.messaldo,sld.entidadlegal_id,fsg.linea_id,fsg.nombreconcepto,sld.areanegocio_id,sld.cuentanatural_id,sld.centrocostos_id,sld.analisislocal_id,sld.storeday;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.view_rubros_fsg AS SELECT
sld.aniosaldo
,sld.messaldo
,fsg.linea_id
,fsg.nombreconcepto
,sld.areanegocio_id
,sld.cuentanatural_id
,sld.analisislocal_id
,sld.segmentofiscal_id
,sld.centrocostos_id
,sld.intercost_id
,SUM(CASE WHEN fsg.signo = '-' 
THEN (-1)*sld.actividaddelperiodo 
ELSE sld.actividaddelperiodo END) TOT_mActividaddelPeriodo
,FROM_UNIXTIME(UNIX_TIMESTAMP()) as storeday
,sld.entidadlegal_id
FROM 
(
SELECT a_saldo.aniosaldo, a_saldo.messaldo, a_saldo.areanegocio_id, a_saldo.cuentanatural_id, a_saldo.analisislocal_id, a_saldo.segmentofiscal_id, a_saldo.centrocostos_id, a_saldo.intercost_id, a_saldo.segment8, a_saldo.segment9, a_saldo.segment10, a_saldo.juegolibros_id, a_saldo.marca_id, a_saldo.presupuesto, a_saldo.balanceinicial, a_saldo.actividaddelperiodo, a_saldo.balancefinal, a_saldo.creditodelperiodo, a_saldo.debitodelperiodo, a_saldo.storeday, a_saldo.entidadlegal_id 
FROM gb_mdl_mexico_costoproducir.A_SALDO 
WHERE  a_saldo.presupuesto = 0
AND 
(
(a_saldo.entidadlegal_id = '100' AND a_saldo.juegolibros_id IN (141,161))
OR
(a_saldo.entidadlegal_id = '101' AND a_saldo.juegolibros_id IN (141,161))
OR
(EntidadLegal_ID = '125' AND JuegoLibros_ID = 641)
)
) SLD,
(
SELECT
rfd.reporte_id
,rfd.linea_id
,rf.nombreconcepto
,rfd.signo
,COALESCE(rfd.i_areanegocio_id,'0000') I_AreaNegocio_IDD, COALESCE(rfd.f_areanegocio_id,'9999') F_AreaNegocio_IDD
,COALESCE(rfd.i_cuentanatural_id,'0000') I_CuentaNatural_IDD, COALESCE(rfd.f_cuentanatural_id,'9999') F_CuentaNatural_IDD
,COALESCE(rfd.i_centrocostos_id,'0000') I_CentroCostos_IDD, COALESCE(rfd.f_centrocostos_id,'9999') F_CentroCostos_IDD
,COALESCE(rfd.i_marca_id,'000') I_Marca_IDD, COALESCE(rfd.f_marca_id,'999') F_Marca_IDD
,COALESCE(rfd.i_analisislocal_id,'0000') I_AnalisisLocal_IDD, COALESCE(rfd.f_analisislocal_id,'9999') F_AnalisisLocal_IDD
FROM gb_mdl_mexico_costoproducir_views.VDW_A_REPORTE_FINANCIERO_DTL RFD, 
(  SELECT vdw_a_reporte_financiero.reporte_id, vdw_a_reporte_financiero.linea_id, vdw_a_reporte_financiero.codigoconcepto, vdw_a_reporte_financiero.nombreconcepto, vdw_a_reporte_financiero.displayflag 
FROM gb_mdl_mexico_costoproducir_views.VDW_A_REPORTE_FINANCIERO 
WHERE TRIM(vdw_a_reporte_financiero.displayflag) = 'Y' 
AND vdw_a_reporte_financiero.reporte_id = 10001
--AND vdw_a_reporte_financiero.linea_id IN (25,30,45,95)
) RF 
WHERE rf.linea_id = rfd.linea_id 
AND rf.reporte_id = rfd.reporte_id
) FSG 
WHERE  sld.entidadlegal_id IN ('100','101','125') 
AND sld.presupuesto = 0
AND sld.areanegocio_id BETWEEN fsg.i_areanegocio_idd AND fsg.f_areanegocio_idd
AND sld.cuentanatural_id BETWEEN fsg.i_cuentanatural_idd AND fsg.f_cuentanatural_idd
AND sld.centrocostos_id BETWEEN fsg.i_centrocostos_idd AND fsg.f_centrocostos_idd
AND sld.marca_id BETWEEN fsg.i_marca_idd AND fsg.f_marca_idd
AND sld.analisislocal_id BETWEEN fsg.i_analisislocal_idd AND fsg.f_analisislocal_idd
GROUP BY 
sld.aniosaldo
,sld.messaldo
,fsg.linea_id
,fsg.nombreconcepto
,sld.areanegocio_id
,sld.cuentanatural_id
,sld.analisislocal_id
,sld.segmentofiscal_id
,sld.centrocostos_id
,sld.intercost_id
,sld.entidadlegal_id;


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.cs_gllines_sf_s3 AS select  
 c.je_source 
,c.name
,b.segment1
,b.segment2 
,b.segment3 
,b.segment4 
,b.segment5 
,b.segment6 
,b.segment7 
,b.segment8 
,b.segment9 
,a.accounted_cr, a.accounted_dr, a.amount_includes_tax_flag, a.attribute1, a.attribute10, a.attribute11, a.attribute12, a.attribute13, a.attribute14, a.attribute15, a.attribute16, a.attribute17, a.attribute18, a.attribute19, a.attribute2, a.attribute20, a.attribute3, a.attribute4, a.attribute5, a.attribute6, a.attribute7, a.attribute8, a.attribute9, a.code_combination_id, a.context, a.context2, a.context3, a.context4, a.created_by, a.creation_date, a.creation_date_h, a.description, a.effective_date, a.effective_date_h, a.entered_cr, a.entered_dr, a.gl_sl_link_id, a.gl_sl_link_table, a.global_attribute_category, a.global_attribute1, a.global_attribute10, a.global_attribute2, a.global_attribute3, a.global_attribute4, a.global_attribute5, a.global_attribute6, a.global_attribute7, a.global_attribute8, a.global_attribute9, a.ignore_rate_flag, a.invoice_amount, a.invoice_date, a.invoice_date_h, a.invoice_identifier, a.je_header_id, a.je_line_num, a.jgzz_recon_context, a.jgzz_recon_date, a.jgzz_recon_date_h, a.jgzz_recon_id, a.jgzz_recon_ref, a.jgzz_recon_status, a.last_update_date, a.last_update_date_h, a.last_update_login, a.last_updated_by, a.line_type_code, a.no1, a.period_name, a.reference_1, a.reference_10, a.reference_2, a.reference_3, a.reference_4, a.reference_5, a.reference_6, a.reference_7, a.reference_8, a.reference_9, a.set_of_books_id, a.stat_amount, a.status, a.subledger_doc_sequence_id, a.subledger_doc_sequence_value, a.tax_code, a.tax_code_id, a.tax_customer_name, a.tax_customer_reference, a.tax_document_date, a.tax_document_date_h, a.tax_document_identifier, a.tax_group_id, a.tax_line_flag, a.tax_registration_number, a.tax_rounding_rule_code, a.tax_type_code, a.taxable_line_flag, a.ussgl_transaction_code, a.storeday
from  erp_mexico_sz.gl_je_lines a
inner join erp_mexico_sz.gl_je_headers c
on (a.je_header_id=c.je_header_id)
left outer join 
erp_mexico_sz.gl_code_combinations b
on (a.code_combination_id = b.code_combination_id )
where 
-- b.segment3 IN ( '4101','4103')  -- comentado a peticion de Robinhoy '2013-06-06'
 lower(c.name) not like lower('Budget%')
and a.je_header_id <> 9131911     ----- eliminamos reclasificacion barcel clubs, 20080814
and ( lower(c.name) <> lower('RLMJRUIZESQ290808')    ----- eliminamos reclasificacion barcel clubs, 20080917
      OR lower(a.description) NOT LIKE lower('RECLASIF%') )
AND ( lower(c.name) <> lower('JRUIZESQ290808-2')    ----- eliminamos reclasificacion barcel clubs, 20080926
      OR lower(a.description) NOT LIKE lower('RECLAS%') )
AND lower(c.status) <> lower('U');


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.d_lineas AS select 
    coalesce(pla.entidadlegal_id,'-1') as entidadlegal_id,
    pla.mf_organizacion_id as planta_id,
    pla.planta_id as codigoplanta,
    wl.line_id  as linea_id,
    upper(max(wl.description)) as linea_ds,
    wl.storeday
  from gb_mdl_mexico_costoproducir.wip_lineas wl
  left join gb_mdl_mexico_manufactura.mf_plantas pla on wl.organization_id = pla.mf_organizacion_id
  where pla.entidadlegal_id in (select entidadlegal_id from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf group by entidadlegal_id)
  group by 
  coalesce(pla.entidadlegal_id,'-1') ,
  pla.mf_organizacion_id,
  pla.planta_id,
  wl.line_id,
  wl.storeday;


  CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_mf_producto_organizacion AS SELECT po.mf_organizacion_id, po.planta_id, 
po.mf_producto_id, po.producto_id, 
po.tipo_producto_id, po.descripcion, 
po.marca_id, po.presentacion_id, 
po.categoria_id, po.linea_id, po.sublinea_id, 
po.gramaje, po.mf_unidadmedida_id, 
po.mf_envase_id, po.vida_anaquel, 
po.contenedor_desc, po.cupo_contenedor, 
po.cupo_envase, po.tope_devolucion, 
po.indicador_eye, po.origen, po.fecha_alta, 
po.fecha_mod, po.storeday, po.entidadlegal_id
FROM gb_mdl_mexico_manufactura.mf_producto_organizacion PO
INNER JOIN gb_smntc_mexico_costoproducir.V_MF_Plantas P 
ON po.entidadlegal_id = p.entidadlegal_id 
AND po.mf_organizacion_id = p.mf_organizacion_id 
AND po.planta_id = p.planta_id;


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.d_materias_primas AS select
     p.entidadlegal_id          as entidadlegal_id
     ,p.mf_producto_id          as materiaprima_id
     ,trim(p.producto_id)       as codigomp
     ,trim(max(p.descripcion))  as materiaprima_ds
from gb_smntc_mexico_costoproducir.v_mf_producto_organizacion p
where p.tipo_producto_id in (1,2,3)
group by p.entidadlegal_id,p.mf_producto_id,trim(p.producto_id);


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.d_plantas AS select
    p.entidadlegal_id   as entidadlegal_id 
    ,p.mf_organizacion_id  as planta_id
    ,p.planta_id as codigoplanta
    ,p.planta_ds as planta_ds
from gb_smntc_mexico_costoproducir.v_mf_plantas p;


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.d_productos AS select
     p.entidadlegal_id           as entidadlegal_id
     ,p.mf_producto_id           as producto_id
     ,trim(p.producto_id)        as codigoproducto
     ,p.mf_organizacion_id       as mf_organizacion_id
     ,trim(max(p.descripcion))   as producto_ds
from gb_smntc_mexico_costoproducir.v_mf_producto_organizacion p
where p.tipo_producto_id in (1,2)
group by p.entidadlegal_id,p.mf_producto_id,trim(p.producto_id),p.mf_organizacion_id;


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_entidadlegal_tipomoneda AS select 
o.entidadlegal_id
,coalesce(el.nombreentidadlegal,'s/i')                 as entidadlegal_ds
,coalesce(m.tipomoneda_id,-1)                          as tipomoneda_id
,coalesce(m.nombretipomoneda, 's/i')                   as tipomoneda_ds
,coalesce(m.nombrecorto, 's/i')                        as tipomoneda_dscorto
,o.organizacion_id
,coalesce(e.organizacion_desc, 's/i')                  as organizacion_ds
,coalesce(m.pais_id,-1)                                as pais_id
,coalesce(g.nombrepais,'s/i')                          as pais_ds
,coalesce(cast(g.organizacion_id as string),'s/i')     as organizaciong_id
,coalesce(og.nombreorganizacion,'s/i')                 as organizaciong_ds
,o.storeday
from gb_mdl_mexico_costoproducir.o_entidadlegal_organizacion o
left outer join gb_mdl_mexico_costoproducir.o_entidad_legal el on o.entidadlegal_id = el.entidadlegal_id
left outer join gb_mdl_mexico_costoproducir.v_tipo_moneda m on m.pais_id = o.pais_id
left outer join gb_mdl_mexico_costoproducir.g_pais g on o.pais_id = g.pais_id
left outer join gb_mdl_mexico_costoproducir.g_organizacion_geografica og on g.organizacion_id = og.organizacion_id
left outer join gb_mdl_mexico_costoproducir.e_organizacion e on e.organizacion_id = o.organizacion_id;


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_mf_formulas AS select 
substr(f.fecha,1, 7) as periodo
,f.fecha 
,f.entidadlegal_id
,f.mf_organizacion_id
,f.planta_id
,f.mf_producto_id
,f.ingrediente_id
,f.cantidad
,f.costoreal
,f.costoestandar
,f.tipomoneda_id
,f.ajuste_flag
,f.fecha_alta
,f.fecha_mod
,f.storeday
from gb_mdl_mexico_manufactura.mf_formulas f
inner join gb_smntc_mexico_costoproducir.v_mf_plantas  p 
on f.entidadlegal_id = p.entidadlegal_id 
and f.mf_organizacion_id = p.mf_organizacion_id
and f.planta_id = p.planta_id;


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_mf_formulas_se AS select
substr(f.fecha, 1, 7) as periodo,
f.fecha, 
f.entidadlegal_id, 
f.mf_organizacion_id, 
f.planta_id, 
f.mf_producto_id, 
f.subensamble_id, 
f.cantidad, 
f.costoestandar, 
f.tipomoneda_id, 
f.fecha_alta, 
f.fecha_mod,
f.storeday
from gb_mdl_mexico_manufactura.mf_formulas_se f
inner join gb_smntc_mexico_costoproducir.v_mf_plantas p 
on f.entidadlegal_id = p.entidadlegal_id 
and f.mf_organizacion_id = p.mf_organizacion_id 
and f.planta_id = p.planta_id;




CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.f_formulas AS select
     f.periodo              as periodo
     ,f.entidadlegal_id     as entidadlegal_id
     ,f.mf_organizacion_id  as planta_id
     ,f.mf_producto_id      as producto_id
     ,f.ingrediente_id      as ingrediente_id
     ,f.cantidad            as cantidad
     ,f.costoreal           as costoreal
     ,f.costoestandar       as costoestandar     
     ,tm.tipomoneda_id       as tmoneda_id
    ,concat(substr(f.periodo,1,4),substr(f.periodo,6,2)) as aniomes
    ,f.fecha_mod            as fecha_carga
from gb_smntc_mexico_costoproducir.v_mf_formulas f 
     inner join gb_smntc_mexico_costoproducir.v_entidadlegal_tipomoneda tm  on f.entidadlegal_id = tm.entidadlegal_id;


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_a_rubros_fsg AS SELECT v_a_rubros_fsg.aniosaldo AS aniosaldo, v_a_rubros_fsg.messaldo AS messaldo, v_a_rubros_fsg.linea_id AS linea_id, v_a_rubros_fsg.nombreconcepto AS nombreconcepto, v_a_rubros_fsg.areanegocio_id AS areanegocio_id, v_a_rubros_fsg.cuentanatural_id AS cuentanatural_id, v_a_rubros_fsg.centrocostos_id AS centrocostos_id, v_a_rubros_fsg.analisislocal_id AS analisislocal_id, v_a_rubros_fsg.tot_mactividaddelperiodo AS tot_mactividaddelperiodo, v_a_rubros_fsg.storeday AS storeday, v_a_rubros_fsg.entidadlegal_id AS entidadlegal_id FROM (SELECT
fsg.aniosaldo
,fsg.messaldo
,fsg.linea_id
,fsg.nombreconcepto
,CASE
WHEN fsg.entidadlegal_id = '096' AND fsg.areanegocio_id = '0001' AND fsg.linea_id = 45 THEN '1146'    -- Tambien en la vista de V_MX_Depreciacion_Equipo_Cta esta aplicada esta condicion para Haz Pan 
ELSE fsg.areanegocio_id
END AS AreaNegocio_ID     
,fsg.cuentanatural_id
,fsg.centrocostos_id
,fsg.analisislocal_id
,fsg.tot_mactividaddelperiodo
,fsg.storeday
,fsg.entidadlegal_id
FROM gb_smntc_mexico_costoproducir.A_RUBROS_FSG FSG
WHERE fsg.linea_id IN (25,30,45)
AND FSG.EntidadLegal_Id IN
(
SELECT Condicion AS EntidadLegal_ID
FROM gb_smntc_mexico_costoproducir.CP_Parametros
WHERE lower(EntidadLegal_ID) = lower('MEX') AND Subrubro_ID IS NULL AND lower(Objeto) = lower('V_A_Rubros_FSG') AND lower(Campo) = lower('EntidadLegal_ID')
GROUP BY Condicion
)) v_a_rubros_fsg;


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_estructura_jerarquica AS select
coalesce(og.organizacion_id,-1)             as organizaciong_id 
,coalesce(og.nombreorganizacion,'s/i')      as organizaciong_ds
,coalesce(r.pais_id,-1)                     as pais_id
,coalesce(gp.nombrepais, 's/i')             as pais_ds
,p.entidadlegal_id
,coalesce(el.nombreentidadlegal , 's/i')    as entidadlegal_ds
,p.gerencia_id
,upper(coalesce(g.gerencia_ds, 's/i'))      as gerencia_ds
,g.region_id
,upper(coalesce(r.region_ds, 's/i'))        as region_ds
,p.mf_organizacion_id
,coalesce(o.organizacion_tipo, 's/i')       as mf_organizacion_tipo
,p.planta_id
,p.planta_ds
from gb_smntc_mexico_costoproducir.v_mf_plantas p
left outer join gb_mdl_mexico_costoproducir.o_entidad_legal el on p.entidadlegal_id = el.entidadlegal_id
left outer join gb_mdl_mexico_manufactura.mf_organizacion o on p.entidadlegal_id = o.entidadlegal_id and p.mf_organizacion_id = o.mf_organizacion_id 
left outer join gb_mdl_mexico_manufactura.mf_gerencias g on p.entidadlegal_id = g.entidadlegal_id and p.gerencia_id = g.gerencia_id
left outer join gb_mdl_mexico_manufactura.mf_regiones r on g.entidadlegal_id = r.entidadlegal_id and g.region_id = r.region_id
left outer join gb_mdl_mexico_costoproducir.g_pais gp on r.pais_id = gp.pais_id 
left outer join gb_mdl_mexico_costoproducir.g_organizacion_geografica og on og.organizacion_id = gp.organizacion_id;


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_mf_compras AS select 
regexp_replace(substr(c.fecha, 1, 7),'/','-') as periodo
,regexp_replace(c.fecha,'/','-') as fecha
,c.entidadlegal_id               
,c.mf_organizacion_id            
,c.planta_id                     
,c.mf_producto_id                
,case
when lower(c.codigo_sub) = 'pa' and c.entidadlegal_id = '089' then 'pt'
when lower(c.codigo_sub) like '%fg%' and c.entidadlegal_id = '122' then 'pt'
else c.codigo_sub
end as codigo_sub
,c.cantidad                      
,c.costo                         
,c.tipomoneda_id                 
,c.fecha_alta                    
,c.fecha_mod                     
from gb_mdl_mexico_manufactura.MF_Compras c
join gb_smntc_mexico_costoproducir.V_MF_Plantas p on c.entidadlegal_id = p.entidadlegal_id and c.mf_organizacion_id = p.mf_organizacion_id and c.planta_id = p.planta_id;


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_mf_costo_prod AS select cp.mf_organizacion_id, cp.planta_id, cp.mf_producto_id, cp.periodo, cp.tipomoneda_id, cp.tipo_costo_id, cp.costo, cp.fecha_alta, cp.fecha_mod, cp.storeday, cp.entidadlegal_id
from gb_mdl_mexico_manufactura.mf_costo_prod cp
inner join gb_smntc_mexico_costoproducir.v_mf_plantas p on cp.entidadlegal_id = p.entidadlegal_id and cp.mf_organizacion_id = p.mf_organizacion_id and cp.planta_id = p.planta_id;



CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_mf_lineas_prod_centro_costos AS select 
lc.mf_organizacion_id,
lc.planta_id,
lc.centrocostos_id,
lc.linea_prod_id,
lc.dl,
substr(lc.fecha_ini, 1, 10) as fecha_ini,
lc.fecha_fin,
lc.storeday,
lc.entidadlegal_id
from gb_mdl_mexico_manufactura.mf_lineas_prod_centro_costos lc
join gb_smntc_mexico_costoproducir.v_mf_plantas p on 
lc.entidadlegal_id = p.entidadlegal_id and 
lc.mf_organizacion_id = p.mf_organizacion_id and 
lc.planta_id = p.planta_id
group by 
lc.mf_organizacion_id,
lc.planta_id,
lc.centrocostos_id,
lc.linea_prod_id,
lc.dl,
substr(lc.fecha_ini, 1, 10),
lc.fecha_fin,
lc.storeday,
lc.entidadlegal_id;


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_mx_depreciacion_equipo_cta AS select
x.activoequipoerp_id
,x.fecha
,x.segment1 as entidadlegal_id
,x.segment2 as areanegocio_id
,x.segment3 as cuentanatural_id
,x.segment4 as analisislocal_id
,x.segment5 as segmentofiscal_id
,x.segment6 as centrocostos_id
,x.segment7 as icp
,x.segment8
,x.segment9
,x.segment10
,x.book_type_code
,sum(x.montodepreciado)              as montodepreciado
,sum(x.montodepreciadoreserva)       as montodepreciadoreserva
,sum(x.montodepreciadoacumulado)     as montodepreciadoacumulado
,sum(x.valorrevaluadoactivo)         as valorrevaluadoactivo
,x.storeday
from ( select
x_.activoequipoerp_id
,x_.fecha
,x_.segment1
,case
when x_.segment1 = '096' and x_.segment2 = '0001' then '1146'
else x_.segment2 
end as segment2 
,x_.segment3
,x_.segment4
,x_.segment5
,x_.segment6
,x_.segment7
,x_.segment8
,x_.segment9
,x_.segment10
,x_.book_type_code
,x_.montodepreciado
,x_.montodepreciadoreserva
,x_.montodepreciadoacumulado
,x_.valorrevaluadoactivo
,x_.storeday
from gb_mdl_mexico_costoproducir.mx_depreciacion_equipo_cuenta x_
) x
group by x.activoequipoerp_id, x.fecha, x.segment1, x.segment2, x.segment3, x.segment4, x.segment5, x.segment6, x.segment7, x.segment8, x.segment9, x.segment10, x.book_type_code, x.storeday;


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_producto_maquilado AS select 
     pm.periodo as periodo,
     pm.entidadlegal_id as entidadlegal_id,
     pm.mf_organizacion_id as mf_organizacion_id,
     pm.planta_id as planta_id,
     pm.mf_producto_id as mf_producto_id,
     sum(pm.costo) as costo,
     'real' as tipo_costo
from gb_mdl_mexico_costoproducir.cp_producto_maquilado  pm
where pm.fecha_fin is null
group by pm.periodo, pm.entidadlegal_id, pm.mf_organizacion_id, pm.planta_id, pm.mf_producto_id

union all

select 
     cp.periodo, 
     cp.entidadlegal_id, 
     cp.mf_organizacion_id, 
     cp.planta_id, 
     cp.mf_producto_id, 
     sum(cp.costo) as costo, 
     'estandar' as tipo_costo
from gb_smntc_mexico_costoproducir.v_mf_costo_prod cp
left outer join(
     select pm2.periodo, pm2.entidadlegal_id, pm2.mf_organizacion_id, pm2.mf_producto_id
          from gb_mdl_mexico_costoproducir.cp_producto_maquilado pm2
          where pm2.fecha_fin is null
          and pm2.costo <> 0
) cpm on
     cpm.periodo = cp.periodo and cpm.entidadlegal_id = cp.entidadlegal_id and 
     cpm.mf_organizacion_id = cp.mf_organizacion_id and cpm.mf_producto_id = cp.mf_producto_id
where
     cpm.periodo is null and cpm.entidadlegal_id is null and cpm.mf_organizacion_id is null and cpm.mf_producto_id is null
     and cp.tipo_costo_id = 9
group by cp.periodo, cp.entidadlegal_id, cp.mf_organizacion_id, cp.planta_id, cp.mf_producto_id;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_rubro81_inventarios AS select   
cal.periodo             as periodo
,pl.entidadlegal_id
,pl.mf_organizacion_id
,onh.inventory_item_id   as mf_producto 
,onh.subinventory_code   as codigosubinv
,sum(onh.primary_transaction_quantity)   as cantidad
,onh.storeday as storeday
from   gb_mdl_mexico_costoproducir.mtl_onhand_diario onh,  gb_smntc_mexico_costoproducir.v_mf_plantas pl,
(
select  mp.periodo, mp.entidadlegal_id, mp.mf_organizacion_id
from  gb_smntc_mexico_costoproducir.cp_medidas_prorrateo mp
where mp.tipomedida_id = 1
group by mp.periodo, mp.entidadlegal_id, mp.mf_organizacion_id
) mp, 
(
select 
mp2.periodo as periodo,
date_sub(add_months(concat(mp2.periodo,'-01'),1), 1) as fecha_max
from gb_smntc_mexico_costoproducir.cp_medidas_prorrateo mp2
group by mp2.periodo, date_sub(add_months(concat(mp2.periodo,'-01'),1), 1)
) cal
where pl.mf_organizacion_id = onh.organization_id 
and onh.fecha = cal.fecha_max
and mp.periodo = cal.periodo
and mp.entidadlegal_id = pl.entidadlegal_id and mp.mf_organizacion_id = pl.mf_organizacion_id
and onh.primary_transaction_quantity <> 0 and ( lower(onh.subinventory_code) like lower('pt%')  or lower(onh.subinventory_code) like lower('mp%')  or lower(onh.subinventory_code) like lower('sub%') )
group by cal.periodo, pl.entidadlegal_id, pl.mf_organizacion_id, onh.inventory_item_id, onh.subinventory_code, onh.storeday;




CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_rubro81_costo_inv_view AS select  
cp.entidadlegal_id, 
cp.mf_organizacion_id, 
cp.planta_id, 
cp.mf_producto_id, 
cp.periodo, 
cp.tipomoneda_id, 
case
when lower(inv.codigosubinv) like lower('%MP%') then 'MP'
when lower(inv.codigosubinv) like lower('%SUB%') then 'SE'
else 'PT' 
end as tsubinven,
c.costo_capital ,
sum(distinct cp.costo) as costo,
sum(distinct inv.cantidad) as cantidad,
sum(cp.costo*inv.cantidad) * c.costo_capital as importe,
cp.storeday
from  gb_smntc_mexico_costoproducir.v_mf_costo_prod cp, gb_mdl_mexico_costoproducir_views.v_rubro81_inventarios inv ,
(
select a.entidadlegal_id ,  a.valor/100 as costo_capital
from gb_mdl_mexico_manufactura.mf_parametro  a 
inner join 
(
select 
mf_parametro.entidadlegal_id
,mf_parametro.tipo_parametro_id
,max(mf_parametro.fecha)  as fecha 
from gb_mdl_mexico_manufactura.mf_parametro 
where (mf_parametro.tipo_parametro_id=12) 
group by mf_parametro.entidadlegal_id,mf_parametro.tipo_parametro_id
) b
on a.entidadlegal_id = b.entidadlegal_id and a.fecha = b.fecha and a.tipo_parametro_id = b.tipo_parametro_id
) c
where  cp.mf_organizacion_id = inv.mf_organizacion_id 
and cp.mf_producto_id    = inv.mf_producto 
and cp.periodo           = inv.periodo 
and cp.entidadlegal_id   = inv.entidadlegal_id 
and cp.entidadlegal_id   = c.entidadlegal_id
and cp.tipo_costo_id in (1,2,3,4)
group by cp.entidadlegal_id, cp.mf_organizacion_id, cp.planta_id, cp.mf_producto_id, cp.periodo, cp.tipomoneda_id, 
case
when lower(inv.codigosubinv) like lower('%MP%') then 'MP'
when lower(inv.codigosubinv) like lower('%SUB%') then 'SE'
else 'PT' 
end,
c.costo_capital,cp.storeday;


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_rubro81_ing_se AS select 
a.periodo, 
a.entidadlegal_id,
a.mf_organizacion_id, 
a.planta_id,  
a.subensamble_id, 
a.costoestandar as monto,
sum( a.cantidad * b.total_registrado) as medida
from gb_smntc_mexico_costoproducir.v_mf_formulas_se a, 
(
select 
cp_medidas_prorrateo.periodo
,cp_medidas_prorrateo.entidadlegal_id
,cp_medidas_prorrateo.mf_organizacion_id
,cp_medidas_prorrateo.planta_id
,cp_medidas_prorrateo.mf_producto_id
,cp_medidas_prorrateo.medida_producto as total_registrado
from gb_smntc_mexico_costoproducir.cp_medidas_prorrateo 
where cp_medidas_prorrateo.tipomedida_id = 5 
) b
where a.periodo               =    b.periodo  
and a.entidadlegal_id    =    b.entidadlegal_id  
and a.mf_organizacion_id =    b.mf_organizacion_id  
and a.mf_producto_id     =    b.mf_producto_id
and a.cantidad * b.total_registrado <> 0
group by a.periodo, a.entidadlegal_id, a.mf_organizacion_id, a.planta_id, a.subensamble_id, a.costoestandar;


CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_rubro81_plantas AS select  
pro.periodo, 
pro.entidadlegal_id, 
pro.mf_organizacion_id, 
pro.planta_id, 
vform.ingrediente_id, 
rb2.importe as costo,
sum(pro.factor*vform.cantidad) as medida
from  gb_smntc_mexico_costoproducir.cp_medidas_prorrateo pro, gb_smntc_mexico_costoproducir.v_mf_formulas vform, gb_smntc_mexico_costoproducir.v_rubro81_costo_inv rb2
where pro.entidadlegal_id          = vform.entidadlegal_id 
and pro.mf_organizacion_id    = vform.mf_organizacion_id 
and pro.mf_producto_id        = vform.mf_producto_id 
and pro.periodo               = vform.periodo 
and rb2.entidadlegal_id       = pro.entidadlegal_id 
and rb2.mf_organizacion_id    = pro.mf_organizacion_id 
and rb2.periodo               = pro.periodo 
and rb2.mf_producto_id        = vform.ingrediente_id 
and pro.tipomedida_id         = 5
and lower(rb2.tsubinven)      = lower('mp')
group by pro.periodo, pro.entidadlegal_id, pro.mf_organizacion_id, pro.planta_id, vform.ingrediente_id, rb2.importe;



CREATE VIEW IF NOT EXISTS gb_smntc_mexico_costoproducir.v_rubro81_prods AS select  
pro.periodo, 
pro.entidadlegal_id, 
pro.mf_organizacion_id, 
pro.planta_id, 
pro.linea_prod_id, 
pro.turno_id, 
vform.mf_producto_id, 
vform.ingrediente_id, 
rb2.importe as costo,
sum(pro.factor*vform.cantidad) as medida
--,pro.storeday
from gb_smntc_mexico_costoproducir.cp_medidas_prorrateo pro,  gb_smntc_mexico_costoproducir.v_mf_formulas vform,  gb_smntc_mexico_costoproducir.v_rubro81_costo_inv rb2
where pro.entidadlegal_id = vform.entidadlegal_id 
and pro.mf_organizacion_id    = vform.mf_organizacion_id 
and pro.mf_producto_id        = vform.mf_producto_id 
and pro.periodo               = vform.periodo 
and rb2.entidadlegal_id       = pro.entidadlegal_id 
and rb2.mf_organizacion_id    = pro.mf_organizacion_id 
and rb2.periodo               = pro.periodo 
and rb2.mf_producto_id        = vform.ingrediente_id 
and pro.tipomedida_id         = 5
and lower(rb2.tsubinven)      = lower('mp')
group by pro.periodo, pro.entidadlegal_id, pro.mf_organizacion_id, pro.planta_id, pro.linea_prod_id, pro.turno_id, vform.mf_producto_id, vform.ingrediente_id, rb2.importe;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.a_pago_empleado AS select 
a_pago_empleado.tiponomina_id    
,a_pago_empleado.empleado_id     
,a_pago_empleado.fechapago       
,a_pago_empleado.cuentanatural_id
,a_pago_empleado.analisislocal_id
,a_pago_empleado.concepto_id     
,a_pago_empleado.region_id       
,a_pago_empleado.montopago       
,a_pago_empleado.tipomoneda_id
,a_pago_empleado.storeday
from gb_mdl_mexico_erp.a_pago_empleado;

CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_a_pago_empleado AS select
ae1.tiponomina_id
,ae1.empleado_id
,ae1.fechapago
,ae1.cuentanatural_id
,ae1.analisislocal_id
,ae1.concepto_id
,ae1.region_id
,ae1.montopago
,ae1.tipomoneda_id
,ae1.storeday
from gb_mdl_mexico_costoproducir_views.a_pago_empleado ae1;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_mf_produccion AS select pr.mf_organizacion_id, pr.planta_id, pr.mf_producto_id, pr.linea_prod_id, pr.turno_id, pr.tipomoneda_id, pr.fecha, pr.total_registrado, pr.total_embarcado, pr.bajas, pr.valor_produccion, pr.costo_prod_teorico, pr.costo_prod_real, pr.costo_actual, pr.toneladas, pr.ritmo, pr.costo_precio, pr.gramaje, pr.fecha_alta, pr.fecha_mod, pr.storeday, pr.entidadlegal_id
from gb_mdl_mexico_manufactura.mf_produccion pr
inner join gb_smntc_mexico_costoproducir.v_mf_plantas p on pr.entidadlegal_id = p.entidadlegal_id and pr.mf_organizacion_id = p.mf_organizacion_id and pr.planta_id = p.planta_id
where pr.turno_id != -1;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_compras AS select
     tbl.periodo as periodo
     ,tbl.mf_organizacion_id as mf_organizacion_id
     ,coalesce(p.planta_id,'s/i') as planta_id
     ,tbl.producto_id as mf_producto_id
     ,tbl.codigo_sub as codigo_sub
     ,tbl.cantidad as cantidad
     ,abs(tbl.costo/tbl.cantidad) as costo
     ,coalesce(tm.tipomoneda_id,-1) as tipomoneda_id
     ,to_date(from_unixtime(unix_timestamp())) as fecha_alta
     ,to_date(from_unixtime(unix_timestamp())) as fecha_mod
     ,to_date(from_unixtime(unix_timestamp())) as storeday
     ,coalesce(p.entidadlegal_id,'s/i') as entidadlegal_id
     
from 
        (
          select 
               c.periodo
               ,c.mf_organizacion_id
               ,c.producto_id
               ,c.codigo_sub
               ,sum(c.cantidad) as cantidad
               ,sum(c.costo) as costo
               ,c.storeday
          from 
               ( 
                    select 
                    concat(regexp_replace(substr(a.transaction_date,1,7),'-','/'),'/01') as periodo,
                    a.organization_id as mf_organizacion_id,  
                    a.inventory_item_id as producto_id,  
                    a.subinventory_code as codigo_sub,
                    cast(a.primary_quantity as float) as cantidad,
                    (a.transaction_cost * a.primary_quantity) as costo,
                    a.storeday
                    from gb_mdl_mexico_costoproducir.mtl_transaccion_materiales a, gb_smntc_mexico_costoproducir.V_MF_Plantas b
                    ,gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf veam
                    ,gb_mdl_mexico_costoproducir_views.v_fechas_extraccion prm
                    where 
                    a.transaction_date between prm.fechaini and prm.fechafin
                    and a.organization_id = b.mf_organizacion_id 
                    and b.entidadlegal_id=veam.entidadlegal_id
                    and a.transaction_type_id in (18,36)
               ) c
          where cast(c.cantidad as decimal(18,5)) <> 0
          group by c.periodo,c.mf_organizacion_id,c.producto_id,c.codigo_sub,c.storeday
         ) tbl
          left outer join gb_smntc_mexico_costoproducir.v_mf_plantas p 
          on tbl.mf_organizacion_id = p.mf_organizacion_id 
          left outer join 
            (
          select 
               o.entidadlegal_id
               ,coalesce(m.tipomoneda_id,-1) as tipomoneda_id
               ,o.organizacion_id
               ,coalesce(m.pais_id,-1) as pais_id
          from gb_mdl_mexico_costoproducir.o_entidadlegal_organizacion o
               left outer join gb_mdl_mexico_costoproducir.o_entidad_legal el on o.entidadlegal_id = el.entidadlegal_id
               left outer join gb_mdl_mexico_costoproducir.v_tipo_moneda m on m.pais_id = o.pais_id
               left outer join gb_mdl_mexico_costoproducir.g_pais g on o.pais_id = g.pais_id
               left outer join gb_mdl_mexico_costoproducir.e_organizacion e on e.organizacion_id = o.organizacion_id
            ) tm
         on p.entidadlegal_id = tm.entidadlegal_id
          where tbl.cantidad<>0
          and p.entidadlegal_id in 
          (select entidadlegal_id 
            from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf 
            group by entidadlegal_id);


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_costo_prod AS select 
     p.mf_organizacion_id as mf_organizacion_id
     ,p.planta_id as planta_id
     ,cst.inventory_item_id as mf_producto_id
     ,concat(year(cst.fecha_actualizacion),'-',
        case 
             when length(month(cst.fecha_actualizacion))=1 then concat('0',month(cst.fecha_actualizacion))
             else month(cst.fecha_actualizacion)
        end) as periodo
     ,null as tipomoneda_id
     ,case
        when cst.cost_type_id = 1 and cst.cost_element_id = 1 then 1
        when cst.cost_type_id = 1 and cst.cost_element_id = 3 and bom.resource_type = 2 then 2
        when cst.cost_type_id = 1 and cst.cost_element_id = 3 and bom.resource_type = 1 then 3
        when cst.cost_type_id = 1 and cst.cost_element_id = 4  and bom.resource_type = 4  then 9
        when cst.cost_type_id = 1 and cst.cost_element_id = 5  then 4
        else null
   end as tipo_costo_id                 
     ,cst.item_cost as costo
     ,from_unixtime(unix_timestamp()) as fecha_alta
     ,from_unixtime(unix_timestamp()) as fecha_mod
     ,from_unixtime(unix_timestamp()) as storeday
     ,p.entidadlegal_id as entidadlegal_id
from gb_mdl_mexico_costoproducir_views.vwh_cst_item_cost_details cst, 
      (select bom_resources.resource_id, bom_resources.resource_type, max(bom_resources.last_update_date) as last_update_date
                from gb_mdl_mexico_costoproducir.bom_resources 
                group by bom_resources.resource_id, bom_resources.resource_type
      ) bom, 
      gb_mdl_mexico_costoproducir.mtl_catalogo_materiales mat, 
      gb_smntc_mexico_costoproducir.v_mf_plantas p,
      gb_mdl_mexico_costoproducir_views.v_fechas_extraccion prm
  where 
      cst.resource_id = bom.resource_id 
      and cst.organization_id = mat.organization_id  
      and cst.inventory_item_id = mat.inventory_item_id   
      and cst.organization_id = p.mf_organizacion_id
      and cst.fecha_actualizacion between prm.fechaini and date_sub(add_months(concat(prm.fechafin),1), 1)
      and trim(lower(mat.item_type)) in ('pt','semiter','mp' ) 
      and cst.cost_type_id = 1 
      and p.entidadlegal_id in (select entidadlegal_id from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf group by entidadlegal_id)
      and case
          when cst.cost_type_id = 1 and cst.cost_element_id = 1 then 1
          when cst.cost_type_id = 1 and cst.cost_element_id = 3 and bom.resource_type = 2 then 2
          when cst.cost_type_id = 1 and cst.cost_element_id = 3 and bom.resource_type = 1 then 3
          when cst.cost_type_id = 1 and cst.cost_element_id = 4  and bom.resource_type = 4  then 9
          when cst.cost_type_id = 1 and cst.cost_element_id = 5  then 4
          else null
     end is not null
  group by 
  p.mf_organizacion_id,
  p.planta_id,
  cst.inventory_item_id,
  concat(year(cst.fecha_actualizacion),'-',
        case 
             when length(month(cst.fecha_actualizacion))=1 then concat('0',month(cst.fecha_actualizacion))
             else month(cst.fecha_actualizacion)
        end),
  case
        when cst.cost_type_id = 1 and cst.cost_element_id = 1 then 1
        when cst.cost_type_id = 1 and cst.cost_element_id = 3 and bom.resource_type = 2 then 2
        when cst.cost_type_id = 1 and cst.cost_element_id = 3 and bom.resource_type = 1 then 3
        when cst.cost_type_id = 1 and cst.cost_element_id = 4  and bom.resource_type = 4  then 9
        when cst.cost_type_id = 1 and cst.cost_element_id = 5  then 4
        else null
   end,
   cst.item_cost,
   p.entidadlegal_id;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_v_tipo_moneda AS select
case when vtm.tipomoneda_id is null then rank()over (order by vtm.tipomoneda_id,vtm.nombrecorto) + c.tipomoneda_id else vtm.tipomoneda_id end as tipomoneda_id
,vtm.nombretipomoneda   as nombretipomoneda
,vtm.nombrecorto        as nombrecorto     
,vtm.pais_id              as pais_id         
,'oracle-mx-r11'                       as sistemafuente
,'user'                                as usuarioetl
,from_unixtime(unix_timestamp())       as fechacarga
,from_unixtime(unix_timestamp())       as fechacambio
,vtm.storeday
from
(
select 
case when lower(dwh.pais) = 'mxp' then 1 
when lower(dwh.pais) = 'usd' then 2
when lower(dwh.pais) = 'ars' then 3
when lower(dwh.pais) = 'brl' then 4
when lower(dwh.pais) = 'clp' then 5
when lower(dwh.pais) = 'cop' then 6
when lower(dwh.pais) = 'crc' then 7
when lower(dwh.pais) = 'gtq' then 8
when lower(dwh.pais) = 'hnl' then 9
when lower(dwh.pais) = 'pen' then 11
when lower(dwh.pais) = 'svc' then 12
when lower(dwh.pais) = 'uyu' then 13
when lower(dwh.pais) = 'veb' then 14
when lower(dwh.pais) = 'cny' then 15
when lower(dwh.pais) = 'pab' then 18
when lower(dwh.pais) = 'nic' then 19
when lower(dwh.pais) = 'gbp' then 20
when lower(dwh.pais) = 'eur1' then 21
when lower(dwh.pais) = 'hkd' then 22
when lower(dwh.pais) = 'uf'  then 23
when lower(dwh.pais) = 'eur2' then 24
when lower(dwh.pais) = 'eur3' then 25
when lower(dwh.pais) = 'eur' then 28
end      as tipomoneda_id
,case when lower(dwh.pais) = 'mxp' then 'peso mexicano'  
when lower(dwh.pais) = 'usd' then 'dolar usa' 
when lower(dwh.pais) = 'ars' then 'pesos argentinos'
when lower(dwh.pais) = 'brl' then 'real brasil'
when lower(dwh.pais) = 'clp' then 'peso chileno'
when lower(dwh.pais) = 'cop' then 'peso colombiano'
when lower(dwh.pais) = 'crc' then 'colon costa rica'
when lower(dwh.pais) = 'gtq' then 'quetzal guatemala'
when lower(dwh.pais) = 'hnl' then 'lempira honduras'
when lower(dwh.pais) = 'pen' then 'sol peruano'
when lower(dwh.pais) = 'svc' then 'colon el salvador'
when lower(dwh.pais) = 'uyu' then 'peso uruguay'
when lower(dwh.pais) = 'veb' then 'bol????var venezuela'
when lower(dwh.pais) = 'cny' then 'chino'
when lower(dwh.pais) = 'pab' then 'panama balboa'
when lower(dwh.pais) = 'nic' then 'nicaragua cordoba'
when lower(dwh.pais) = 'gbp' then 'uk libra esterlina'
when lower(dwh.pais) = 'eur' then 'euro global'
when lower(dwh.pais) = 'eur1' then 'euro'
when lower(dwh.pais) = 'eur2' then 'euro'
when lower(dwh.pais) = 'eur3' then 'euro'
when lower(dwh.pais) = 'hkd' then 'hong kong dollar'
when lower(dwh.pais) = 'uf'  then 'unidad de fomento'
end      as nombretipomoneda
,lower(dwh.pais) as nombrecorto
,case when lower(dwh.pais) = 'mxp' then 52 
when lower(dwh.pais) = 'usd' then 840
when lower(dwh.pais) = 'ars' then 32
when lower(dwh.pais) = 'brl' then 76
when lower(dwh.pais) = 'clp' then 152
when lower(dwh.pais) = 'cop' then 170
when lower(dwh.pais) = 'crc' then 188
when lower(dwh.pais) = 'gtq' then 320
when lower(dwh.pais) = 'hnl' then 340
when lower(dwh.pais) = 'pen' then 604
when lower(dwh.pais) = 'svc' then 222
when lower(dwh.pais) = 'uyu' then 858
when lower(dwh.pais) = 'veb' then 862
when lower(dwh.pais) = 'cny' then 86
when lower(dwh.pais) = 'pab' then 591
when lower(dwh.pais) = 'nic' then 558
when lower(dwh.pais) = 'gbp' then null 
when lower(dwh.pais) = 'eur' then 724
when lower(dwh.pais) = 'eur1' then 724
when lower(dwh.pais) = 'eur2' then 620
when lower(dwh.pais) = 'eur3' then 380
when lower(dwh.pais) = 'hkd' then null
when lower(dwh.pais) = 'uf'  then null 
end     as pais_id
,dwh.storeday as storeday
from 
(
select 
dwh.monedaorigen_id as pais,
dwh.storeday
from gb_smntc_mexico_costoproducir.a_tipo_cambio  dwh
where dwh.sistemafuente = 'oracle-mx-r11'
and lower(dwh.monedaorigen_id)  not in  ('cad','mxn','pyg','vef')
group by dwh.monedaorigen_id, dwh.storeday
) dwh

union all

select 
null as tipomoneda_id
,case 
when lower(sb.currency_code) = 'ats' then 'chel????n austriaco'
when lower(sb.currency_code) = 'dem' then 'marco aleman'
when lower(sb.currency_code) = 'nio' then 'c????rdoba nicaraguense'
when lower(sb.currency_code) = 'nlg' then 'flor????n holand????s'
when lower(sb.currency_code) = 'pag' then 'guaran???? paraguayo' else 's/i' 
end as              nombretipomoneda
,lower(sb.currency_code) as   nombrecorto
,case 
when lower(sb.currency_code) = 'ats' then 36
when lower(sb.currency_code) = 'dem' then 276
when lower(sb.currency_code) = 'nio' then 558
when lower(sb.currency_code) = 'nlg' then 528
when lower(sb.currency_code) = 'pag' then 600 else -1
end as pais_id
,max(sb.storeday) as storeday
from erp_mexico_sz.gl_sets_of_books sb
where sb.currency_code not in (select nombrecorto from gb_mdl_mexico_costoproducir.v_tipo_moneda group by nombrecorto)
group by 

null,
case 
when lower(sb.currency_code) = 'ats' then 'chel????n austriaco'
when lower(sb.currency_code) = 'dem' then 'marco aleman'
when lower(sb.currency_code) = 'nio' then 'c????rdoba nicaraguense'
when lower(sb.currency_code) = 'nlg' then 'flor????n holand????s'
when lower(sb.currency_code) = 'pag' then 'guaran???? paraguayo' else 's/i' 
end,
lower(sb.currency_code)
,case 
when lower(sb.currency_code) = 'ats' then 36
when lower(sb.currency_code) = 'dem' then 276
when lower(sb.currency_code) = 'nio' then 558
when lower(sb.currency_code) = 'nlg' then 528
when lower(sb.currency_code) = 'pag' then 600 else -1
end
)vtm
,(select coalesce(max(v_tipo_moneda.tipomoneda_id),0) as tipomoneda_id from gb_mdl_mexico_costoproducir.v_tipo_moneda where v_tipo_moneda.tipomoneda_id > -1) c;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vt_cp_beneficios_vol AS SELECT 
    VT_CP_Beneficios_Vol.periodo AS periodo,
    VT_CP_Beneficios_Vol.mf_organizacion_id AS mf_organizacion_id,
    VT_CP_Beneficios_Vol.planta_id AS planta_id,
    VT_CP_Beneficios_Vol.ingrediente_id AS ingrediente_id,
    VT_CP_Beneficios_Vol.cuentanatural_id AS cuentanatural_id,
    VT_CP_Beneficios_Vol.importe AS importe,
    VT_CP_Beneficios_Vol.fecha_ini AS fecha_ini,
    VT_CP_Beneficios_Vol.fecha_fin AS fecha_fin,
    VT_CP_Beneficios_Vol.storeday AS storeday,
    VT_CP_Beneficios_Vol.entidadlegal_id AS entidadlegal_id
FROM
  (
    SELECT 
        bv.periodo AS Periodo ,
        p.mf_organizacion_id AS MF_Organizacion_ID ,
        p.planta_id AS Planta_ID ,
        CAST(CASE WHEN bv.item IS NULL THEN -1 -- Se aplicara a nivel planta
                WHEN bv.item IS NOT NULL
                    AND pr.mf_producto_id IS NOT NULL THEN pr.mf_producto_id -- Existe en el catalogo
                WHEN bv.item IS NOT NULL
                    AND pr.mf_producto_id IS NULL THEN 0 -- No existe en el catalogo
            END AS INT) AS Ingrediente_ID ,
        SUBSTRING(bv.cuenta,10,4) AS CuentaNatural_ID ,
        bv.monto AS Importe ,
        FROM_UNIXTIME(UNIX_TIMESTAMP()) AS Fecha_Ini ,
        NULL AS Fecha_Fin ,
        FROM_UNIXTIME(UNIX_TIMESTAMP()) AS storeday ,
        bv.entidadlegal_id AS EntidadLegal_ID
   FROM cp_flat_files.cp_beneficios_volumen bv
   LEFT OUTER JOIN gb_smntc_mexico_costoproducir.V_MF_Plantas P ON bv.entidadlegal_id = p.entidadlegal_id
        AND SUBSTRING(TRIM(bv.cuenta),5,4) = p.planta_id
   LEFT OUTER JOIN gb_smntc_mexico_costoproducir.V_MF_Producto_Organizacion PR ON bv.entidadlegal_id = pr.entidadlegal_id
        AND p.mf_organizacion_id = pr.mf_organizacion_id
        AND SUBSTRING(TRIM(bv.cuenta),5,4) = pr.planta_id
        AND TRIM(bv.item)=pr.producto_id
        AND pr.tipo_producto_id=3) VT_CP_Beneficios_Vol;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vt_cp_gastos_fletes AS SELECT VT_CP_Gastos_Fletes.periodo AS periodo, VT_CP_Gastos_Fletes.mf_organizacion_id AS mf_organizacion_id, VT_CP_Gastos_Fletes.planta_id AS planta_id, VT_CP_Gastos_Fletes.ingrediente_id AS ingrediente_id, VT_CP_Gastos_Fletes.cuentanatural_id AS cuentanatural_id, VT_CP_Gastos_Fletes.analisislocal_id AS analisislocal_id, VT_CP_Gastos_Fletes.importe AS importe, VT_CP_Gastos_Fletes.fecha_ini AS fecha_ini, VT_CP_Gastos_Fletes.fecha_fin AS fecha_fin, VT_CP_Gastos_Fletes.storeday AS storeday, VT_CP_Gastos_Fletes.entidadlegal_id AS entidadlegal_id FROM (SELECT f.periodo AS periodo
,p.mf_organizacion_id AS MF_Organizacion_ID
,p.planta_id AS Planta_ID
,CAST(CASE 
WHEN f.item IS NULL THEN -1                                                         -- Se aplicara a nivel planta
WHEN f.item IS NOT NULL AND pr.mf_producto_id IS NOT NULL THEN pr.mf_producto_id    -- Existe en el catalogo
WHEN f.item IS NOT NULL AND pr.mf_producto_id IS NULL THEN 0                        -- No existe en el catalogo
END AS INT) as Ingrediente_ID
,split(f.cuenta,"\\.")[2] as CuentaNatural_ID
,split(f.cuenta,"\\.")[3] as AnalisisLocal_ID
,f.monto AS importe
,FROM_UNIXTIME(UNIX_TIMESTAMP()) as Fecha_Ini
,null as Fecha_Fin
,FROM_UNIXTIME(UNIX_TIMESTAMP()) as storeday
,f.entidadlegal_id as entidadlegal_id
from cp_flat_files.cp_gastos_fletes F
LEFT OUTER JOIN gb_smntc_mexico_costoproducir.V_MF_Plantas P
ON f.entidadlegal_id = p.entidadlegal_id 
AND SUBSTRING(TRIM(f.cuenta),5,4) = p.planta_id
LEFT OUTER JOIN gb_smntc_mexico_costoproducir.V_MF_Producto_Organizacion PR 
ON f.entidadlegal_id = pr.entidadlegal_id 
AND p.mf_organizacion_id = pr.mf_organizacion_id 
AND SUBSTRING(TRIM(f.cuenta),5,4) = pr.planta_id 
AND TRIM(f.item) = pr.producto_id 
AND pr.tipo_producto_id = 3 -- Traemos MP
--LEFT OUTER JOIN VDWH.A_ANALISIS_LOCAL AL 
--ON SUBSTRING(F.Cuenta FROM 10 FOR 4) = AL.CuentaNatural_ID 
--AND SUBSTRING(F.Cuenta FROM 15 FOR 4) = AL.AnalisisLocal_ID
WHERE f.periodo IS NOT NULL 
AND f.monto <> 0 
AND f.monto IS NOT NULL
AND (CAST(CASE 
WHEN (f.item IS NULL OR f.item = '') THEN -1                                                         -- Se aplicara a nivel planta
WHEN f.item IS NOT NULL AND pr.mf_producto_id IS NOT NULL THEN pr.mf_producto_id    -- Existe en el catalogo
WHEN f.item IS NOT NULL AND pr.mf_producto_id IS NULL THEN 0                        -- No existe en el catalogo
END AS INT)) <> 0) VT_CP_Gastos_Fletes;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vt_cp_gastos_importaciones AS SELECT VT_CP_Gastos_Importaciones.periodo AS periodo, VT_CP_Gastos_Importaciones.mf_organizacion_id AS mf_organizacion_id, VT_CP_Gastos_Importaciones.planta_id AS planta_id, VT_CP_Gastos_Importaciones.ingrediente_id AS ingrediente_id, VT_CP_Gastos_Importaciones.cuentanatural_id AS cuentanatural_id, VT_CP_Gastos_Importaciones.importe AS importe, VT_CP_Gastos_Importaciones.fecha_ini AS fecha_ini, VT_CP_Gastos_Importaciones.fecha_fin AS fecha_fin, VT_CP_Gastos_Importaciones.storeday AS storeday, VT_CP_Gastos_Importaciones.entidadlegal_id AS entidadlegal_id FROM (select ei.periodo AS periodo
,p.mf_organizacion_id AS mf_organizacion_id
,p.planta_id AS planta_id
,CAST(CASE 
WHEN ei.item IS NULL THEN -1                                                         -- Se aplicara a nivel planta
WHEN ei.item IS NOT NULL AND pr.mf_producto_id IS NOT NULL THEN pr.mf_producto_id    -- Existe en el catalogo
WHEN ei.item IS NOT NULL AND pr.mf_producto_id IS NULL THEN 0                        -- No existe en el catalogo
END 
AS INT) AS Ingrediente_ID
, split(ei.cuenta ,"\\.")[2] AS cuentanatural_id
, ei.monto AS importe
, FROM_UNIXTIME(UNIX_TIMESTAMP()) AS fecha_ini
, null AS fecha_fin
, FROM_UNIXTIME(UNIX_TIMESTAMP()) AS storeday
, ei.entidadlegal_id AS entidadlegal_id
from cp_flat_files.cp_importacion_items EI
INNER JOIN gb_smntc_mexico_costoproducir.V_MF_Plantas P 
ON TRIM(ei.entidadlegal_id) = p.entidadlegal_id 
AND SUBSTRING(TRIM(ei.cuenta),5,4) = p.planta_id
LEFT OUTER JOIN gb_smntc_mexico_costoproducir.V_MF_Producto_Organizacion PR 
ON ei.entidadlegal_id = pr.entidadlegal_id 
AND p.mf_organizacion_id = pr.mf_organizacion_id 
AND SUBSTRING(TRIM(ei.cuenta),5,4) = pr.planta_id 
AND TRIM(ei.item) = pr.producto_id 
AND pr.tipo_producto_id = 3     -- Traemos MP
--     LEFT OUTER JOIN VDWH.A_CUENTA_NATURAL CN 
--     ON SUBSTRING(EI.Cuenta FROM 10 FOR 4) = CN.CuentaNatural_ID
WHERE  ei.periodo IS NOT NULL 
AND ei.monto <> 0 
AND ei.monto IS NOT NULL
AND (CAST(CASE
WHEN (ei.item IS NULL OR ei.item = '') THEN -1                                                         -- Se aplicara a nivel planta
WHEN ei.item IS NOT NULL AND pr.mf_producto_id IS NOT NULL THEN pr.mf_producto_id    -- Existe en el catalogo
WHEN ei.item IS NOT NULL AND pr.mf_producto_id IS NULL THEN 0                        -- No existe en el catalogo
END 
AS INT)) <> 0) VT_CP_Gastos_Importaciones;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vt_cp_lineas_prod_metros AS SELECT VT_CP_Lineas_Prod_Metros.entidadlegal_id AS entidadlegal_id, VT_CP_Lineas_Prod_Metros.mf_organizacion_id AS mf_organizacion_id, VT_CP_Lineas_Prod_Metros.planta_id AS planta_id, VT_CP_Lineas_Prod_Metros.linea_prod_id AS linea_prod_id, VT_CP_Lineas_Prod_Metros.metros2 AS metros2, VT_CP_Lineas_Prod_Metros.fecha_ini AS fecha_ini, VT_CP_Lineas_Prod_Metros.fecha_fin AS fecha_fin FROM (SELECT
 p.entidadlegal_id as EntidadLegal_ID
 ,p.mf_organizacion_id as MF_Organizacion_ID
 ,p.planta_id as Planta_ID
 ,lp.linea_prod_id as Linea_Prod_ID
 ,m.mts2 as Metros2
 ,FROM_UNIXTIME(UNIX_TIMESTAMP()) as Fecha_Ini
 ,NULL as Fecha_Fin
FROM cp_flat_files.CP_Lineas_Prod_Metros M
 INNER JOIN gb_smntc_mexico_costoproducir.V_MF_Plantas P 
 ON m.entidadlegal_id=p.entidadlegal_id 
 AND m.codigoplanta = p.planta_id
 INNER JOIN gb_mdl_mexico_manufactura.MF_Lineas_Prod LP 
 ON m.entidadlegal_id=lp.entidadlegal_id 
 AND m.linea_id=lp.linea_prod_id
WHERE m.linea_id IS NOT NULL) VT_CP_Lineas_Prod_Metros;
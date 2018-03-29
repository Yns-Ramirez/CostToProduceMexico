
CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_mtl_transaccion_materiales AS SELECT
mtl_transaccion_materiales.transaction_id
, mtl_transaccion_materiales.last_update_date
, mtl_transaccion_materiales.last_update_date_h
, mtl_transaccion_materiales.creation_date
, mtl_transaccion_materiales.creation_date_h
, mtl_transaccion_materiales.inventory_item_id
, mtl_transaccion_materiales.organization_id
, mtl_transaccion_materiales.subinventory_code
, mtl_transaccion_materiales.transaction_type_id
, mtl_transaccion_materiales.transaction_source_type_id
, mtl_transaccion_materiales.transaction_source_id
, mtl_transaccion_materiales.transaction_action_id
, mtl_transaccion_materiales.transaction_quantity
, mtl_transaccion_materiales.transaction_uom
, mtl_transaccion_materiales.primary_quantity
, mtl_transaccion_materiales.transaction_date
, mtl_transaccion_materiales.transaction_date_h
, mtl_transaccion_materiales.transaction_reference
, mtl_transaccion_materiales.actual_cost
, mtl_transaccion_materiales.transaction_cost
, mtl_transaccion_materiales.prior_cost
, mtl_transaccion_materiales.new_cost
, mtl_transaccion_materiales.quantity_adjusted
, mtl_transaccion_materiales.department_id
, mtl_transaccion_materiales.transfer_organization_id
, mtl_transaccion_materiales.ship_to_location_id
, mtl_transaccion_materiales.reason_id
, mtl_transaccion_materiales.source_code
, mtl_transaccion_materiales.acct_period_id
, mtl_transaccion_materiales.completion_transaction_id
, (CASE WHEN( mtl_transaccion_materiales.transaction_date<'2011-04-18' AND mtl_transaccion_materiales.organization_id <>7122 )THEN mtl_transaccion_materiales.transaction_set_id 
WHEN (mtl_transaccion_materiales.transaction_date < '2011-04-13' AND  mtl_transaccion_materiales.organization_id = 7122 ) THEN mtl_transaccion_materiales.transaction_set_id
ELSE mtl_transaccion_materiales.completion_transaction_id END) TRANSACTION_SET_ID
FROM gb_mdl_mexico_costoproducir.mtl_transaccion_materiales;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_for AS SELECT gx_control_entidades_app.entidadlegal_id, gx_control_entidades_app.cadena, gx_control_entidades_app.aplicacion, gx_control_entidades_app.objeto, gx_control_entidades_app.campo, gx_control_entidades_app.condicion, gx_control_entidades_app.operador, gx_control_entidades_app.observaciones
FROM gb_mdl_mexico_costoproducir.GX_CONTROL_ENTIDADES_APP
WHERE TRIM(gx_control_entidades_app.cadena) = 'MF' AND gx_control_entidades_app.aplicacion = 'FORMULAS' 
AND gx_control_entidades_app.objeto = 'FORMULAS' AND gx_control_entidades_app.campo = 'Calculo' 
AND gx_control_entidades_app.condicion = 'A';



CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf AS select gx_control_entidadeslegales.entidadlegal_id,
gx_control_entidadeslegales.cadena,
gx_control_entidadeslegales.status
from gb_mdl_mexico_costoproducir.gx_control_entidadeslegales
where trim(lower(gx_control_entidadeslegales.status)) = lower('A')
and trim(lower(gx_control_entidadeslegales.cadena)) = lower('MF');


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_entidadlegal_activas AS select cp_control_entidadeslegales.entidadlegal_id, cp_control_entidadeslegales.organizaciong_ds, cp_control_entidadeslegales.status
from gb_mdl_mexico_costoproducir.cp_control_entidadeslegales
where lower(cp_control_entidadeslegales.status) = lower('a');



CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_fechas_extraccion_hist AS select
    v_fechas_extraccion.fechaini,
    to_date(from_unixtime(unix_timestamp())) as fechafin
  from gb_mdl_mexico_costoproducir_views.v_fechas_extraccion;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_form_all_subens AS select t_form_stg_subensform_cp_temp.periodo, t_form_stg_subensform_cp_temp.entidadlegal_id, t_form_stg_subensform_cp_temp.planta_id, t_form_stg_subensform_cp_temp.subensamble_id_ori, t_form_stg_subensform_cp_temp.subensamble_id, t_form_stg_subensform_cp_temp.codigosubinv, t_form_stg_subensform_cp_temp.ingrediente_id, t_form_stg_subensform_cp_temp.cantidad, t_form_stg_subensform_cp_temp.masa, t_form_stg_subensform_cp_temp.fact, t_form_stg_subensform_cp_temp.delnivel from gb_mdl_mexico_costoproducir.T_FORM_STG_SUBENSFORM_CP_TEMP;

CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_organizacion_inactiva_mf AS SELECT v_organizacion_inactiva_mf.entidadlegal_id,
v_organizacion_inactiva_mf.mf_organizacion_id,
v_organizacion_inactiva_mf.planta_id,
v_organizacion_inactiva_mf.gerencia_id,
v_organizacion_inactiva_mf.planta_ds,
v_organizacion_inactiva_mf.sistemafuente,
v_organizacion_inactiva_mf.usuarioetl,
v_organizacion_inactiva_mf.fechacarga,
v_organizacion_inactiva_mf.fechacambio 
from cp_flat_files.v_organizacion_inactiva_mf;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_get_plantas AS SELECT mfp.entidadlegal_id, mfp.mf_organizacion_id 
FROM gb_mdl_mexico_manufactura.MF_Plantas mfp
WHERE mfp.EntidadLegal_ID IN 
(
SELECT vea.EntidadLegal_ID 
FROM gb_mdl_mexico_costoproducir_views.V_ENTIDADESLEGALES_ACTIVAS_MF vea
GROUP BY vea.EntidadLegal_ID ) 
GROUP BY mfp.entidadlegal_id, mfp.mf_organizacion_id
UNION ALL
SELECT mfp2.entidadlegal_id, mfp2.org_id as MF_Organizacion_ID
FROM cp_flat_files.mf_plantas mfp2 
WHERE mfp2.EntidadLegal_ID IN 
(
SELECT vea2.EntidadLegal_ID
FROM gb_mdl_mexico_costoproducir_views.V_ENTIDADESLEGALES_ACTIVAS_MF vea2
GROUP BY vea2.EntidadLegal_ID) 
GROUP BY mfp2.entidadlegal_id, mfp2.org_id;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vwh_cst_item_cost_details AS Select       
cst_item_cost_details_hist.fecha_actualizacion
,cst_item_cost_details_hist.inventory_item_id
,cst_item_cost_details_hist.organization_id
,cst_item_cost_details_hist.cost_type_id
,cst_item_cost_details_hist.last_update_date
,cst_item_cost_details_hist.last_updated_by
,cst_item_cost_details_hist.creation_date
,cst_item_cost_details_hist.created_by
,cst_item_cost_details_hist.last_update_login
,cst_item_cost_details_hist.operation_sequence_id
,cst_item_cost_details_hist.operation_seq_num
,cst_item_cost_details_hist.department_id
,cst_item_cost_details_hist.level_type
,cst_item_cost_details_hist.activity_id
,cst_item_cost_details_hist.resource_seq_num
,cst_item_cost_details_hist.resource_id
,cst_item_cost_details_hist.resource_rate
,cst_item_cost_details_hist.item_units
,cst_item_cost_details_hist.activity_units
,cst_item_cost_details_hist.usage_rate_or_amount
,cst_item_cost_details_hist.basis_type
,cst_item_cost_details_hist.basis_resource_id
,cst_item_cost_details_hist.basis_factor
,cst_item_cost_details_hist.net_yield_or_shrinkage_factor
,cst_item_cost_details_hist.item_cost
,cst_item_cost_details_hist.cost_element_id
,cst_item_cost_details_hist.rollup_source_type
,cst_item_cost_details_hist.activity_context
,cst_item_cost_details_hist.request_id
,cst_item_cost_details_hist.program_application_id
,cst_item_cost_details_hist.program_id
,cst_item_cost_details_hist.program_update_date
,cst_item_cost_details_hist.attribute_category
,cst_item_cost_details_hist.attribute1
,cst_item_cost_details_hist.attribute2
,cst_item_cost_details_hist.attribute3
,cst_item_cost_details_hist.attribute4
,cst_item_cost_details_hist.attribute5
,cst_item_cost_details_hist.attribute6
,cst_item_cost_details_hist.attribute7
,cst_item_cost_details_hist.attribute8
,cst_item_cost_details_hist.attribute9
,cst_item_cost_details_hist.attribute10
,cst_item_cost_details_hist.attribute11
,cst_item_cost_details_hist.attribute12
,cst_item_cost_details_hist.attribute13
,cst_item_cost_details_hist.attribute14
,cst_item_cost_details_hist.attribute15
,cst_item_cost_details_hist.yielded_cost
,cst_item_cost_details_hist.source_organization_id
,cst_item_cost_details_hist.vendor_id
,cst_item_cost_details_hist.allocation_percent
,cst_item_cost_details_hist.vendor_site_id
,cst_item_cost_details_hist.ship_method
from gb_mdl_mexico_erp.cst_item_cost_details_hist;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_mf_turno_default_dia AS select  
mtl.entidadlegal_id
,mtl.mf_organizacion_id
,mtl.planta_id
,def.turno_id
,def.turno_ds
,cal.calendar_date as fechaini
,case 
when  def.turnohraini > def.turnohrafinal then date_add(cal.calendar_date,1)
else cal.calendar_date  
end                                                as fechafin
,substr(concat(cal.calendar_date,' ',def.turnohraini),1,19)     as fechahoraini
,substr(case 
when def.turnohraini < def.turnohrafinal then concat(cal.calendar_date,' ',def.turnohrafinal)
when def.turnohraini > def.turnohrafinal
then concat(date_add(cal.calendar_date, 1),' ',def.turnohrafinal)
end,1,19)                                                as fechahorafin
,substr(def.turnohraini,1,8)                                   as turnohraini
,substr(def.turnohrafinal,1,8)                                 as turnohrafin
from 
(
select 
b.entidadlegal_id, 
b.mf_organizacion_id, 
b.planta_id
from gb_mdl_mexico_manufactura.mf_plantas b
where b.entidadlegal_id  in (select entidadlegal_id from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf group by entidadlegal_id)
)mtl
cross join
(
select 
t.entidadlegal_id
,t.mf_organizacion_id
,t.turno_id
,t.turno_ds
,t.turnohraini
,t.turnohrafinal
from gb_mdl_mexico_manufactura.mf_turno_default  t 
where trim(t.entidadlegal_id)  in (select entidadlegal_id from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf group by entidadlegal_id)
)def
cross join cp_sys_calendar.calendar cal
where trim(mtl.entidadlegal_id)  = trim(def.entidadlegal_id) and cal.calendar_date between '2010-01-01' and to_date(from_unixtime(unix_timestamp()))
group by
mtl.entidadlegal_id
,mtl.mf_organizacion_id
,mtl.planta_id
,def.turno_id
,def.turno_ds
,cal.calendar_date
,case 
when  def.turnohraini > def.turnohrafinal then date_add(cal.calendar_date,1)
else cal.calendar_date  
end
,substr(concat(cal.calendar_date,' ',def.turnohraini),1,19)
,substr(case 
when def.turnohraini < def.turnohrafinal then concat(cal.calendar_date,' ',def.turnohrafinal)
when def.turnohraini > def.turnohrafinal
then concat(date_add(cal.calendar_date, 1),' ',def.turnohrafinal)
end,1,19)
,substr(def.turnohraini,1,8)
,substr(def.turnohrafinal,1,8);


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_mf_turno_dia AS select 
t.entidadlegal_id                              as entidadlegal_id
,t.mf_organizacion_id                          as mf_organizacion_id
,t.planta_id                                   as planta_id
,t.linea_prod_id                               as linea_prod_id
,t.turno_id                                    as turno_id 
,t.periodo                                     as periodo
,cal.calendar_date                             as fechaini
,case 
when  t.hora_ini > t.hora_fin then  date_add(cal.calendar_date,1)
else  cal.calendar_date
end                                            as fechafin
,concat (cal.calendar_date,' ',t.hora_ini )    as fechahoraini
,case 
when t.hora_ini < t.hora_fin then concat(cal.calendar_date,' ',t.hora_fin)
when t.hora_ini > t.hora_fin
then concat(to_date(date_add(cal.calendar_date, 1)),' ',t.hora_fin)
end                                            as fechahorafin
,t.hora_ini                                    as turnohraini 
,t.hora_fin                                    as turnohrafinal           
from gb_mdl_mexico_manufactura.mf_turnos t, cp_sys_calendar.calendar  cal 
where 
cal.calendar_date  between t.fecha_ini and t.fecha_fin
and t.fecha_vigencia is null
and t.entidadlegal_id  in (select entidadlegal_id from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf group by entidadlegal_id);


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_mf_turnos_cuenta AS select 
     t.entidadlegal_id
     ,t.planta_id
     ,t.linea_id as linea_prod_id
     ,t.turno_id
     ,t.periodo
     ,t.fecha_inicia
     ,t.fecha_fin
     ,t.hora_inicia
     ,t.hora_fin
     ,cast(tl.cuenta_tl as int) as cuenta_tl 
     ,cast(tt.cuenta_tt as int) as cuenta_tt
from cp_flat_files.mf_turnos t
     inner join 
          (
               select  
                    mf_turnos.entidadlegal_id
                    ,mf_turnos.planta_id
                    ,mf_turnos.linea_id
                    ,mf_turnos.periodo
                    ,count(*) as cuenta_tl
               from cp_flat_files.mf_turnos
               group by 
                    mf_turnos.entidadlegal_id
                    ,mf_turnos.planta_id
                    ,mf_turnos.linea_id
                    ,mf_turnos.periodo
          ) tl
     on   t.entidadlegal_id = tl.entidadlegal_id and t.planta_id = tl.planta_id and t.linea_id = tl.linea_id  and  t.periodo = tl.periodo
     inner join 
          (
               select  
                    mf_turnos.entidadlegal_id
                    ,mf_turnos.planta_id
                    ,mf_turnos.linea_id
                    ,mf_turnos.turno_id
                    ,mf_turnos.periodo
                    ,count(*) as cuenta_tt
               from cp_flat_files.mf_turnos
               group by
                    mf_turnos.entidadlegal_id
                    ,mf_turnos.planta_id
                    ,mf_turnos.linea_id
                    ,mf_turnos.turno_id
                    ,mf_turnos.periodo
          ) tt      
on t.entidadlegal_id = tt.entidadlegal_id
and t.planta_id = tt.planta_id
and t.linea_id = tt.linea_id
and t.turno_id = tt.turno_id
and t.periodo = tt.periodo;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_wip_repetitive_items_hist AS select
year(wip_repetitive_items_hist.fecha_actualizacion) as aniobimbo
,month(wip_repetitive_items_hist.fecha_actualizacion) as mesbimbo
,wip_repetitive_items_hist.fecha_actualizacion           
,wip_repetitive_items_hist.wip_entity_id                 
,wip_repetitive_items_hist.line_id                       
,wip_repetitive_items_hist.organization_id               
,wip_repetitive_items_hist.last_update_date              
,wip_repetitive_items_hist.last_update_date_h            
,wip_repetitive_items_hist.last_updated_by               
,wip_repetitive_items_hist.creation_date                 
,wip_repetitive_items_hist.creation_date_h               
,wip_repetitive_items_hist.created_by                    
,wip_repetitive_items_hist.last_update_login             
,wip_repetitive_items_hist.request_id                    
,wip_repetitive_items_hist.program_application_id        
,wip_repetitive_items_hist.program_id                    
,wip_repetitive_items_hist.program_update_date           
,wip_repetitive_items_hist.program_update_date_h         
,wip_repetitive_items_hist.primary_item_id               
,wip_repetitive_items_hist.alternate_bom_designator      
,wip_repetitive_items_hist.alternate_routing_designator  
,wip_repetitive_items_hist.class_code                    
,wip_repetitive_items_hist.wip_supply_type               
,wip_repetitive_items_hist.completion_subinventory       
,wip_repetitive_items_hist.completion_locator_id         
,wip_repetitive_items_hist.load_distribution_priority    
,wip_repetitive_items_hist.primary_line_flag             
,wip_repetitive_items_hist.production_line_rate          
,wip_repetitive_items_hist.overcompletion_tolerance_type 
,wip_repetitive_items_hist.overcompletion_tolerance_value
,wip_repetitive_items_hist.attribute1                    
,wip_repetitive_items_hist.attribute6
,wip_repetitive_items_hist.storeday                    
from gb_mdl_mexico_costoproducir.wip_repetitive_items_hist;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_a_saldo AS SELECT  
gl_balances_fix.aniosaldo
,gl_balances_fix.messaldo     
,gl_balances_fix.areanegocio_id
,gl_balances_fix.cuentanatural_id
,gl_balances_fix.analisislocal_id
,gl_balances_fix.centrocostos_id
,gl_balances_fix.intercost_id
,gl_balances_fix.segment8
,gl_balances_fix.segment9
,gl_balances_fix.segment10
,gl_balances_fix.juegolibros_id 
,gl_balances_fix.marca_id 
,gl_balances_fix.presupuesto 
,CASE 
WHEN gl_balances_fix.balinicial='-'
THEN 
CAST(gl_balances_fix.balinicial AS INT)
ELSE 
CAST(gl_balances_fix.balinicial AS FLOAT)
END AS BalanceInicial
,gl_balances_fix.actividaddelperiodo
,CASE 
WHEN gl_balances_fix.balfinal='-'
THEN 
CAST(gl_balances_fix.balfinal AS INT)
ELSE 
CAST(gl_balances_fix.balfinal AS FLOAT)
END AS BalanceFinal
,CASE 
WHEN gl_balances_fix.credper='-'
THEN 
CAST(gl_balances_fix.credper AS INT)
ELSE 
CAST(gl_balances_fix.credper AS FLOAT)
END CreditoDelPeriodo
,CASE 
WHEN gl_balances_fix.debper='-'
THEN 
CAST(gl_balances_fix.debper AS INT)
ELSE 
CAST(gl_balances_fix.debper AS FLOAT)  
END DebitoDelPeriodo
,FROM_UNIXTIME(UNIX_TIMESTAMP()) AS STOREDAY
,gl_balances_fix.entidadlegal_id
FROM
(
SELECT
t_a_saldo_gl.aniosaldo
,t_a_saldo_gl.messaldo 
,t_a_saldo_gl.areanegocio_id
,t_a_saldo_gl.cuentanatural_id
,t_a_saldo_gl.analisislocal_id
,t_a_saldo_gl.centrocostos_id
,t_a_saldo_gl.intercost_id
,t_a_saldo_gl.segment8
,t_a_saldo_gl.segment9
,t_a_saldo_gl.segment10
,t_a_saldo_gl.juegolibros_id
,t_a_saldo_gl.marca_id
,t_a_saldo_gl.presupuesto
,t_a_saldo_gl.actividaddelperiodo
,CASE
WHEN t_a_saldo_gl.balanceinicial < 0
THEN '-' 
ELSE TRIM(CAST(t_a_saldo_gl.balanceinicial AS STRING))
END  BalInicial
,CASE 
WHEN t_a_saldo_gl.balancefinal < 0 
THEN '-' 
ELSE TRIM(CAST(t_a_saldo_gl.balancefinal AS STRING)) 
END BalFinal
,CASE
WHEN(t_a_saldo_gl.balancefinal - t_a_saldo_gl.balanceinicial ) <0 
THEN '-' 
ELSE  TRIM(CAST( (t_a_saldo_gl.balancefinal - t_a_saldo_gl.balanceinicial) AS STRING))
END Dif_BalIni_Fin
,CASE 
WHEN t_a_saldo_gl.creditodelperiodo < 0 
THEN '-' 
ELSE TRIM(CAST(t_a_saldo_gl.creditodelperiodo AS STRING))
END CredPer
,CASE
WHEN t_a_saldo_gl.debitodelperiodo < 0 
THEN '-' 
ELSE TRIM(CAST(t_a_saldo_gl.debitodelperiodo AS STRING))
END DebPer
,CASE
WHEN(t_a_saldo_gl.creditodelperiodo - t_a_saldo_gl.debitodelperiodo) < 0 
THEN '-' 
ELSE TRIM(CAST( (t_a_saldo_gl.creditodelperiodo - t_a_saldo_gl.debitodelperiodo) AS STRING))
END Dif_CredDeb
,trim (t_a_saldo_gl.entidadlegal_id) as EntidadLegal_ID
FROM gb_mdl_mexico_costoproducir.T_A_SALDO_GL
) GL_BALANCES_FIX;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_a_saldo_gl AS SELECT vasg.aniosaldo AS aniosaldo
, vasg.messaldo AS messaldo
, vasg.entidadlegal_id AS entidadlegal_id
, vasg.areanegocio_id AS areanegocio_id
, vasg.cuentanatural_id AS cuentanatural_id
, vasg.analisislocal_id AS analisislocal_id
, vasg.centrocostos_id AS centrocostos_id
, vasg.intercost_id AS intercost_id
, vasg.segment8 AS segment8
, vasg.segment9 AS segment9
, vasg.segment10 AS segment10
, vasg.juegolibros_id AS juegolibros_id
, vasg.marca_id AS marca_id
, vasg.presupuesto AS presupuesto
, vasg.balanceinicial AS balanceinicial
, vasg.actividaddelperiodo AS actividaddelperiodo
, vasg.balancefinal AS balancefinal
, vasg.creditodelperiodo AS creditodelperiodo
, vasg.debitodelperiodo AS debitodelperiodo FROM 
(SELECT 
bal.period_year AS AnioSaldo
,bal.period_num AS MesSaldo
,cod.segment1 AS EntidadLegal_ID
,cod.segment2 AS AreaNegocio_ID
,cod.segment3 AS CuentaNatural_ID
,cod.segment4 AS AnalisisLocal_ID
,cod.segment6 AS CentroCostos_ID
,cod.segment7 AS Intercost_ID
,COALESCE (cod.segment8, 0) AS Segment8
,COALESCE (cod.segment9, 0) AS Segment9
,COALESCE (cod.segment10, 0) AS Segment10
,bal.set_of_books_id AS JuegoLibros_ID
,cod.segment5 AS Marca_ID
,CASE WHEN bal.actual_flag = 'A' THEN 0 ELSE 1 END AS Presupuesto
,(CAST (bal.begin_balance_dr AS DECIMAL(32,4)) - CAST( bal.begin_balance_cr AS DECIMAL(32,4))) AS BalanceInicial
,bal.period_net_dr - bal.period_net_cr AS ActividadDelPeriodo
,( CAST(bal.begin_balance_dr AS DECIMAL(32,4)) - CAST(bal.begin_balance_cr AS DECIMAL(32,4)) )   
+ ( bal.period_net_dr - bal.period_net_cr ) AS BalanceFinal
,bal.period_net_cr AS CreditoDelPeriodo
,bal.period_net_dr AS DebitoDelPeriodo
FROM erp_mexico_sz.GL_CODE_COMBINATIONS COD 
INNER JOIN gb_mdl_mexico_costoproducir_views.GL_BALANCES BAL 
ON cod.code_combination_id = bal.code_combination_id 
AND cod.segment1 <> 'T' 
AND cod.segment2 <> 'T' 
AND cod.segment3 <> 'T' 
AND cod.segment6 <> 'T' 
AND cod.segment4 <> 'T' 
AND cod.segment7 <> 'T' 
AND cod.segment5 <> 'T' 
AND cod.template_id IS NULL 
WHERE bal.actual_flag  IN ( 'A' , 'B' ) 
AND bal.currency_code <> 'STAT' 
AND (bal.translated_flag <> 'R' OR bal.translated_flag IS NULL )
AND cod.segment9 <> 'PT'
AND (
cod.chart_of_accounts_id=50304
OR  (cod.chart_of_accounts_id=50264 
AND cod.segment1 IN ('100','101','125','189','096','004','049') 
))
) vasg;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_e_empleado_posicion AS SELECT 
wrkt_empleado_posicion.e_empleado_id as Empleado_ID
,wrkt_empleado_posicion.e_numeroperiodo as NumeroPeriodo
,wrkt_empleado_posicion.e_fechainicioperiodo as FechaInicioPeriodo
,MAX(wrkt_empleado_posicion.e_fechafinperiodo) as FechaFinPeriodo
,wrkt_empleado_posicion.e_posicion_id as Posicion_ID
,MAX(wrkt_empleado_posicion.e_fechainicioposicion) as FechaInicioPosicion
,MAX(wrkt_empleado_posicion.e_fechafinposicion) as FechaFinPosicion
,MAX(wrkt_empleado_posicion.e_numerocolaboradorinterno) as NumeroColaboradorInterno
,MAX(wrkt_empleado_posicion.e_tipocolaborador_id) as TipoColaborador_ID
,MAX(wrkt_empleado_posicion.e_motivobaja_id) as MotivoBaja_ID
,MAX(wrkt_empleado_posicion.e_fechaantiguedad) as FechaAntiguedad
,MAX(wrkt_empleado_posicion.sistema_fuente) as Sistema_Fuente
,FROM_UNIXTIME(UNIX_TIMESTAMP()) as Fecha_Alta
,MAX(wrkt_empleado_posicion.p_entidadlegal_id) as EntidadLegal_ID
FROM gb_mdl_mexico_costoproducir.WRKT_EMPLEADO_POSICION
GROUP BY wrkt_empleado_posicion.e_empleado_id,wrkt_empleado_posicion.e_numeroperiodo,wrkt_empleado_posicion.e_fechainicioperiodo,wrkt_empleado_posicion.e_posicion_id;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_e_organizacion AS select 
cast(std_work_location.std_id_work_locat as string),
max(cast(coalesce(std_work_location.std_work_locesp,'s/i') as string)),
cast('peoplenet' as string) as sistema_origen,
from_unixtime(unix_timestamp()) as current_time,
std_work_location.storeday
from erp_mexico_sz.std_work_location 
where std_work_location.std_id_wl_type = 5 and std_work_location.std_work_locesp not in ('organizacion latin sur','organizacion latinoamerica sur','organizacion bimbo','latinoamerica sur','organizacion iberia')
group by std_work_location.std_id_work_locat, std_work_location.storeday;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_e_posicion AS select
     pos.e_posicion_id as posicion_id
     ,max(pos.p_fechainicio) as fechainicio
     ,max(pos.p_fechafin) as fechafin
     ,max(pos.p_nombreposicion) as nombreposicion
     ,max(pos.p_puesto_id) as puesto_id
     ,max(pos.p_plan_id) as plan_id
     ,max(pos.p_areanegocio_id) as areanegocio_id
     ,max(pos.p_unidadtrabajo_id) as unidadtrabajo_id
     ,max(pos.p_categoria_id) as categoria_id
     ,max(pos.p_nivel_id) as nivel_id
     ,max(pos.p_centrotrabajo_id) as centrotrabajo_id
     ,max(pos.p_centrocostos_id) as centrocosto_id
     ,max(pos.p_convenio_id) as convenio_id
     ,max(pos.p_horasxsemana) as horasxsemana
     ,max(pos.p_tiempocompleto) as tiempocompleto
     ,max(pos.p_posicion_id) as posicion_id_original
     ,max(pos.sistema_fuente) as sistema_fuente
     ,from_unixtime(unix_timestamp()) as fecha_alta
     ,max(pos.storeday) as storeday
     ,max(pos.p_entidadlegal_id) as entidadlegal_id
from gb_mdl_mexico_costoproducir.wrkt_empleado_posicion pos
group by pos.e_posicion_id;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_g_pais AS select 
sc_.pais_id,
sc_.organizacion_id,
sc_.nombrepais,
sc_.sistema_fuente,
sc_.fecha_alta,
sc_.storeday
from 
(
select
      cast(sc.std_id_count_group as int) as pais_id,
      case when lower(sc.std_id_country) = 'mex' then 1
      when lower(sc.std_id_country) in ('per','arg','pry','bol','bra','ury','chl') then 2
      when lower(sc.std_id_country) in ('can','usa') then 3
      when lower(sc.std_id_country) in ('pan','cub','slv','nic','blz' ,'cri','gtm','hnd','col','ven','ecu')  then 4
      when lower(sc.std_id_country) in ('cze','grc','nld','fra','dnk','bel' ,'aut' ,'prt','esp','che','gbr','ita','deu') then 5
      when lower(sc.std_id_country) in ('lbn','chn') then 6
      else -1
      end as organizacion_id,
      sc.std_n_countryesp as nombrepais,
      cast('peoplenet v7' as string) as sistema_fuente,
      from_unixtime(unix_timestamp()) as fecha_alta,
      row_number() over (partition by sc.std_id_count_group  order by sc.std_id_country desc) as num,
      from_unixtime(unix_timestamp()) as storeday
from erp_mexico_sz.std_country sc where sc.std_id_count_group is not null and sc.std_id_count_group != 'null'
)sc_ where  sc_.num = 1;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_cargasocial AS select 
  12 as tipo_parametro_id  -- hard code asignado en dwh.mf_tipo_parametros 
  ,css.periodo as fecha --(format 'yyyy-mm-dd')
  ,cast(null as string) as parametro_desc
  ,coalesce(css.valor ,-1) as valor
  ,el.storeday
  ,css.entidadlegal_id as entidadlegal_id
from cp_flat_files.cp_costo_capital css
  join gb_mdl_mexico_costoproducir.o_entidad_legal el on css.entidadlegal_id = el.entidadlegal_id
where css.entidadlegal_id in (select entidadlegal_id from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf group by entidadlegal_id);


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_categoria AS SELECT VDW_MF_CATEGORIA.categoria_id AS categoria_id, VDW_MF_CATEGORIA.tipo_categ_id AS tipo_categ_id, VDW_MF_CATEGORIA.categoria_desc AS categoria_desc, VDW_MF_CATEGORIA.storeday AS storeday FROM (
      SELECT RANK()OVER (ORDER BY stg.categoria_desc) + c.categoria_id AS Categoria_ID,1 AS Tipo_Categ_ID
,stg.categoria_desc,FROM_UNIXTIME(UNIX_TIMESTAMP()) AS storeday
FROM (
SELECT COALESCE(b.segment2,'S/I') AS Categoria_Desc
FROM 
gb_mdl_mexico_costoproducir.MTL_CATEGORIAS B
INNER JOIN gb_mdl_mexico_costoproducir.MTL_Categoria_Materiales C 
ON b.category_id = c.category_id 
WHERE UPPER(TRIM(COALESCE(B.SEGMENT2,'S/I'))) NOT IN 
(SELECT UPPER(TRIM(Categoria_Desc)) FROM gb_mdl_mexico_manufactura.MF_Categoria)
AND c.category_set_id = 27 
AND b.segment2 IS NOT NULL
GROUP BY b.segment2
)STG
,(SELECT COALESCE(MAX(mf_categoria.categoria_id),0) AS Categoria_ID
FROM gb_mdl_mexico_manufactura.MF_Categoria
WHERE mf_categoria.categoria_id > -1
) C) VDW_MF_CATEGORIA;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_cross_references AS select 
coalesce(pvm.entidadlegal_id, -1)                              as entidadlegal_id  
,coalesce(pe.inventory_item_id,pvm.inventory_item_id)           as mf_producto_id
--,coalesce(pe.fecha,pvm.fecha)                                   as fecha
,pe.pe_fechaini                                                 as pe_fechaini
,pe.pe_fechafin                                                 as pe_fechafin
,cast (coalesce (pe.cross_reference_pe, 0) as float)            as cross_reference_pe
,cast (coalesce (pe.cross_reference_pe_fl, 0) as float)         as cross_reference_pe_fl
,coalesce (pe.description_pe, 's/i')                            as description_pe
,pvm.pvm_fechaini                                               as pvm_fechaini 
,pvm.pvm_fechafin                                               as pvm_fechafin 
,cast (coalesce (pvm.cross_reference_pvm, 0) as float)          as cross_reference_pvm
,cast (coalesce (pvm.cross_reference_pvm_fl, 0) as float)       as cross_reference_pvm_fl
,coalesce (pvm.description_pvm, 's/i')                          as description_pvm
,coalesce(pe.pe_fechaini, pvm.pvm_fechaini)                     as fechaini
,coalesce(pe.pe_fechafin, pvm.pvm_fechafin)                     as fechafin
from 
( 
select 
cr2.inventory_item_id                   
--,cal.calendar_date                       as fecha
,cr2.fechaini                            as pe_fechaini
,cr2.fechafin                            as pe_fechafin
,cast(regexp_replace(cr2.cross_reference_pe,',','') as float)   as cross_reference_pe
,cast(regexp_replace(cr2.cross_reference_pe,',','') as float)   as cross_reference_pe_fl
,cr2.description_pe
from
(
select 
cr.inventory_item_id
,cr.last_update_date    as fechaini
,coalesce(date_sub(min(cr.last_update_date) over (partition by cr.inventory_item_id order by cr.last_update_date desc rows between 1 preceding and 1 preceding), 1), '2999-12-31') as fechafin
,coalesce(cr.cross_reference_pe,'0.00')    as cross_reference_pe
,cr.description_pe
from
(

select 
cm1.inventory_item_id as inventory_item_id
,cm1.last_update_date as last_update_date
,max(case when lower(cm1.cross_reference_type) like 'row_pe%' then cm1.cross_reference end)    as cross_reference_pe
,max(case when lower(cm1.cross_reference_type) like 'row_pe%' then cm1.description end)        as description_pe
from gb_mdl_mexico_costoproducir.mtl_referencia_cruzada_mat cm1
where lower(cm1.cross_reference_type) like  ('row_pe%')
group by cm1.inventory_item_id
,cm1.last_update_date

union all

select 
a.inventory_item_id as inventory_item_id
,a.last_update_date2 as last_update_date
,a.cross_reference_pe as cross_reference_pe
,a.description_pe as description_pe
from 
(                                       
select 
rcm.inventory_item_id
,rcm.last_update_date 
,max(case when lower(rcm.cross_reference_type) like 'row_pe%' then rcm.cross_reference end) as cross_reference_pe
,max(case when lower(rcm.cross_reference_type) like 'row_pe%' then rcm.description end)     as description_pe
,add_months (rcm.last_update_date ,-24) as last_update_date2
from gb_mdl_mexico_costoproducir.mtl_referencia_cruzada_mat rcm
inner join 
(
select 
rcm2.inventory_item_id 
,min(rcm2.last_update_date) as mini
from gb_mdl_mexico_costoproducir.mtl_referencia_cruzada_mat rcm2
where lower(rcm2.cross_reference_type) like  ('row_pe%') 
group by rcm2.inventory_item_id
)rcm2 on rcm.inventory_item_id = rcm2.inventory_item_id and rcm.last_update_date = rcm2.mini
where lower(rcm.cross_reference_type) like  ('row_pe%')
group by rcm.inventory_item_id
,rcm.last_update_date
,add_months (rcm.last_update_date ,-24)                                   
)a
group by a.inventory_item_id 
,a.last_update_date2
,a.cross_reference_pe
,a.description_pe
)cr

)cr2
--,cp_sys_calendar.calendar cal
--where (cal.calendar_date between cr2.fechaini and cr2.fechafin) and cal.calendar_date < date_add(to_date(from_unixtime(unix_timestamp())), 1)
)pe
full outer join
(

select 
cr2.entidadlegal_id
,cr2.inventory_item_id
--,cal.calendar_date                  as fecha
,cr2.fechaini                           as pvm_fechaini
,cr2.fechafin                           as pvm_fechafin
,cast(regexp_replace(cr2.cross_reference_pvm,',','') as float) as cross_reference_pvm
,cast(regexp_replace(cr2.cross_reference_pvm,',','') as float) as cross_reference_pvm_fl
,cr2.description_pvm
from 
(
select 
cr.entidadlegal_id    
,cr.inventory_item_id
,cr.last_update_date    as fechaini
,coalesce(date_sub(min(cr.last_update_date) over (partition by cr.entidadlegal_id, cr.inventory_item_id order by cr.last_update_date desc rows between 1 preceding and 1 preceding), 1), '2999-12-31') as fechafin
,coalesce(cr.cross_reference_pvm,'0.00')   as cross_reference_pvm
,cr.description_pvm
from 
(

select 
b.entidadlegal_id
,a.inventory_item_id
,a.last_update_date
,case when lower(a.cross_reference_type) like 'row_pvm%' then a.cross_reference end as cross_reference_pvm
,case when lower(a.cross_reference_type) like 'row_pvm%' then case 
when lower(a.description) = 'ss.' or lower(a.description) = 'ss' or lower(a.description) = 's'   then 'pen'
when lower(a.description) = 'pv' or lower(a.description) = 'pcp' or lower(a.description) = 'pco' then 'cop'
when lower(a.description) = 'pur' then 'uyu'
when lower(a.description) = 'pch' then 'clp'
when lower(a.description) = 'par' then 'ars'
when lower(a.description) = 'bs' then 'veb'
else a.description end
end as description_pvm
from gb_mdl_mexico_costoproducir.mtl_referencia_cruzada_mat a
left outer join
(
select gx_control_entidades_app.entidadlegal_id, gx_control_entidades_app.cadena, gx_control_entidades_app.aplicacion, gx_control_entidades_app.objeto, gx_control_entidades_app.campo, gx_control_entidades_app.condicion, gx_control_entidades_app.operador, gx_control_entidades_app.observaciones 
from gb_mdl_mexico_costoproducir.gx_control_entidades_app 
where lower(gx_control_entidades_app.aplicacion) = 'mf_produccion' and lower(gx_control_entidades_app.objeto) = 'cross_references' and lower(gx_control_entidades_app.campo) = 'valor_produccion'
) b on a.cross_reference_type = b.condicion  
where lower(a.cross_reference_type) like  ('row_pvm%')
and b.entidadlegal_id is not null
group by
b.entidadlegal_id
,a.inventory_item_id
,a.last_update_date
,case when lower(a.cross_reference_type) like 'row_pvm%' then a.cross_reference end
,case when lower(a.cross_reference_type) like 'row_pvm%' then case 
when lower(a.description) = 'ss.' or lower(a.description) = 'ss' or lower(a.description) = 's'   then 'pen'
when lower(a.description) = 'pv' or lower(a.description) = 'pcp' or lower(a.description) = 'pco' then 'cop'
when lower(a.description) = 'pur' then 'uyu'
when lower(a.description) = 'pch' then 'clp'
when lower(a.description) = 'par' then 'ars'
when lower(a.description) = 'bs' then 'veb'
else a.description end
end

union all

select
b2.entidadlegal_id
,b2.inventory_item_id
,b2.last_update_date2 as last_update_date
,b2.cross_reference_pvm
,b2.description_pvm
from
(
select
case when lower(rcm.cross_reference_type) like 'row_pvm%' then b.entidadlegal_id end as entidadlegal_id
,rcm.inventory_item_id
,rcm.last_update_date
,case when lower(rcm.cross_reference_type) like 'row_pvm%' then rcm.cross_reference end as cross_reference_pvm
,case when lower(rcm.cross_reference_type) like 'row_pvm%' then case 
 when lower(rcm.description) = 'ss.' or lower(rcm.description) = 'ss' or lower(rcm.description) = 's'   then 'pen'
 when lower(rcm.description) = 'pv' or lower(rcm.description) = 'pcp' or lower(rcm.description) = 'pco' then 'cop'
 when lower(rcm.description) = 'pur' then 'uyu'
 when lower(rcm.description) = 'pch' then 'clp'
 when lower(rcm.description) = 'par' then 'ars'
 when lower(rcm.description) = 'bs' then 'veb'
else rcm.description end
end as description_pvm
,add_months (rcm.last_update_date ,-24) as last_update_date2
from gb_mdl_mexico_costoproducir.mtl_referencia_cruzada_mat rcm
left outer join
(
select gx_control_entidades_app.entidadlegal_id, gx_control_entidades_app.cadena, gx_control_entidades_app.aplicacion, gx_control_entidades_app.objeto, gx_control_entidades_app.campo, gx_control_entidades_app.condicion, gx_control_entidades_app.operador, gx_control_entidades_app.observaciones 
from gb_mdl_mexico_costoproducir.gx_control_entidades_app 
where lower(gx_control_entidades_app.aplicacion) = 'mf_produccion' and lower(gx_control_entidades_app.objeto) = 'cross_references' and lower(gx_control_entidades_app.campo) = 'valor_produccion'
) b on rcm.cross_reference_type = b.condicion
join 
(
select 
rcm2.inventory_item_id 
,min(rcm2.last_update_date) as mini
from gb_mdl_mexico_costoproducir.mtl_referencia_cruzada_mat rcm2
where lower(rcm2.cross_reference_type) like ('row_pvm%')
group by rcm2.inventory_item_id
)rcm2 on rcm.inventory_item_id = rcm2.inventory_item_id and rcm.last_update_date = rcm2.mini
where lower(rcm.cross_reference_type) like  ('row_pvm%')
group by
case when lower(rcm.cross_reference_type) like 'row_pvm%' then b.entidadlegal_id end
,rcm.inventory_item_id
,rcm.last_update_date
,case when lower(rcm.cross_reference_type) like 'row_pvm%' then rcm.cross_reference end
,case when lower(rcm.cross_reference_type) like 'row_pvm%' then case 
 when lower(rcm.description) = 'ss.' or lower(rcm.description) = 'ss' or lower(rcm.description) = 's'   then 'pen'
 when lower(rcm.description) = 'pv' or lower(rcm.description) = 'pcp' or lower(rcm.description) = 'pco' then 'cop'
 when lower(rcm.description) = 'pur' then 'uyu'
 when lower(rcm.description) = 'pch' then 'clp'
 when lower(rcm.description) = 'par' then 'ars'
 when lower(rcm.description) = 'bs' then 'veb'
else rcm.description end
end
,add_months (rcm.last_update_date ,-24)
)b2                                        
)cr
)cr2
--,cp_sys_calendar.calendar cal
--where (cal.calendar_date between cr2.fechaini and cr2.fechafin) and cal.calendar_date < date_add(to_date(from_unixtime(unix_timestamp())), 1)
)pvm on pe.inventory_item_id = pvm.inventory_item_id;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_gerencias AS select 
    a.gerencia_id as gerencia_id,
    coalesce(a.region_id,-1) as region_id,
    a.gerencia_ds as gerencia_ds,
    from_unixtime(unix_timestamp()) as storeday,
    a.entidadlegal_id as entidadlegal_id
from cp_flat_files.mf_gerencias a
inner join gb_mdl_mexico_costoproducir.o_entidad_legal el on a.entidadlegal_id = el.entidadlegal_id
inner join gb_mdl_mexico_manufactura.mf_regiones r on a.entidadlegal_id = r.entidadlegal_id
and a.region_id = r.region_id
where a.gerencia_id is not null
  and a.entidadlegal_id in
    (select entidadlegal_id
     from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf
     group by entidadlegal_id)
group by
      a.gerencia_id ,
      coalesce(a.region_id,-1),
      a.gerencia_ds ,
      a.storeday,
      a.entidadlegal_id;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_linea AS SELECT VDW_MF_LINEA.linea_id AS linea_id, VDW_MF_LINEA.linea_desc AS linea_desc, VDW_MF_LINEA.storeday AS storeday FROM (SELECT RANK() OVER (ORDER BY stg.linea_desc) + l.linea_id AS Linea_ID,stg.linea_desc as linea_desc,FROM_UNIXTIME(UNIX_TIMESTAMP()) AS storeday
FROM (SELECT l.linea_desc
FROM (SELECT COALESCE(b.segment3,'S/I') AS Linea_Desc
FROM gb_mdl_mexico_costoproducir.MTL_CATEGORIAS B
INNER JOIN gb_mdl_mexico_costoproducir.MTL_Categoria_Materiales C 
ON b.category_id = c.category_id 
AND c.category_set_id = 27
WHERE UPPER(TRIM(COALESCE(B.SEGMENT3,'S/I'))) NOT IN 
(SELECT UPPER(TRIM(Linea_Desc)) FROM gb_mdl_mexico_manufactura.MF_Linea) GROUP BY b.segment3)L) STG,
(SELECT COALESCE(MAX(mf_linea.linea_id),0) AS Linea_ID
FROM gb_mdl_mexico_manufactura.MF_Linea
WHERE mf_linea.linea_id > -1)L) VDW_MF_LINEA;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_lineas_prod AS select tbl.entidadlegal_id, tbl.linea_prod_id, tbl.grupo_lineas_prod_id, tbl.linea_prod_ds, tbl.storeday
from
(
select ldp.entidadlegal_id as entidadlegal_id ,
ldp.line_id as linea_prod_id ,
coalesce(mlp.grupo_cat_linea_id, -1) as grupo_lineas_prod_id ,
upper(ldp.description) as linea_prod_ds,
ldp.storeday
from
(
select coalesce(pla.entidadlegal_id,'-1') as entidadlegal_id ,
wl.line_id ,
max(wl.description) as description,
wl.storeday
from gb_mdl_mexico_costoproducir.wip_lineas wl
left join gb_mdl_mexico_manufactura.mf_plantas pla on wl.organization_id = pla.mf_organizacion_id
where pla.entidadlegal_id in
(select entidadlegal_id
from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf
group by entidadlegal_id)
group by 
coalesce(pla.entidadlegal_id,'-1') ,
wl.line_id,
wl.storeday
)ldp
left join cp_flat_files.mf_grupo_lineas_produccion mlp on ldp.entidadlegal_id = mlp.entidadlegal_id
and ldp.line_id = mlp.linea_prod_id
)tbl
left outer join gb_mdl_mexico_manufactura.mf_lineas_prod a on a.entidadlegal_id = tbl.entidadlegal_id
and a.linea_prod_id = tbl.linea_prod_id;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_lineas_prod_cc AS SELECT vdw_mf_lineas_prod_cc.entidadlegal_id AS entidadlegal_id, vdw_mf_lineas_prod_cc.mf_organizacion_id AS mf_organizacion_id, vdw_mf_lineas_prod_cc.planta_id AS planta_id, vdw_mf_lineas_prod_cc.cc AS centrocostos_id, vdw_mf_lineas_prod_cc.linea_id AS linea_prod_id, vdw_mf_lineas_prod_cc.dl AS dl, vdw_mf_lineas_prod_cc.fecha_ini AS fecha_ini, vdw_mf_lineas_prod_cc.fecha_fin AS fecha_fin, vdw_mf_lineas_prod_cc.storeday AS storeday FROM (select 
     cc.entidadlegal_id
     ,p.mf_organizacion_id
     ,p.planta_id
     ,cc.cc
     ,coalesce(cc.linea_id,-1) as linea_id
     ,cc.dl
     ,from_unixtime(unix_timestamp()) as fecha_ini
     ,null as fecha_fin
     ,cc.storeday
from cp_flat_files.mf_lineascc cc
     join gb_mdl_mexico_costoproducir.o_entidad_legal el on cc.entidadlegal_id = el.entidadlegal_id
     join gb_mdl_mexico_costoproducir.a_centro_costos c on c.centrocostos_id= cc.cc
     join gb_mdl_mexico_manufactura.mf_plantas p on cc.entidadlegal_id = p.entidadlegal_id and cc.codigoplanta = p.planta_id and lower(p.sistema_fuente) = 'cp'
where cc.entidadlegal_id is not null  and cc.codigoplanta is not null  and cc.cc is not null  and cc.dl is not null
and cc.entidadlegal_id in (select entidadlegal_id from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf group by entidadlegal_id)) vdw_mf_lineas_prod_cc;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_marca AS SELECT VDW_MF_MARCA.marca_id AS marca_id, VDW_MF_MARCA.marca_desc AS marca_desc, VDW_MF_MARCA.storeday AS storeday FROM (SELECT RANK()OVER (ORDER BY stg.marca_desc) + m.marca_id as Marca_ID
,stg.marca_desc,FROM_UNIXTIME(UNIX_TIMESTAMP()) as storeday
FROM(SELECT COALESCE(b.segment5,'S/I') AS Marca_Desc
FROM gb_mdl_mexico_costoproducir.MTL_CATEGORIAS B
INNER JOIN gb_mdl_mexico_costoproducir.MTL_Categoria_Materiales C 
ON b.category_id = c.category_id 
AND c.category_set_id = 27
WHERE UPPER(TRIM(COALESCE(B.SEGMENT5,'S/I'))) NOT IN 
(SELECT UPPER(TRIM(mm.Marca_Desc)) FROM gb_mdl_mexico_manufactura.MF_Marca mm) 
AND b.segment5 IS NOT NULL GROUP BY b.segment5) STG,
(
SELECT COALESCE(MAX(mf_marca.marca_id),0) AS Marca_ID
FROM gb_mdl_mexico_manufactura.MF_Marca
WHERE mf_marca.marca_id > -1
)M) VDW_MF_MARCA;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_organizacion AS select tbl_fte.entidadlegal_id,
tbl_fte.mf_organizacion_id,
tbl_fte.host_id,
tbl_fte.organizacion_desc,
tbl_fte.organizacion_tipo,
tbl_fte.storeday
from
( select max(substr(hr_location_0.location_code ,1 ,3)) as entidadlegal_id ,
case
when hr_organizacion_0.organization_id is null then cast(concat(substr(hr_location_0.location_code ,1 ,3),substr(hr_location_0.location_code ,5 ,4)) as int) * -1
else hr_organizacion_0.organization_id
end as mf_organizacion_id ,
max(coalesce( case when trim(substr(hr_location_0.location_code ,5 ,4)) = '' then null else trim(substr(hr_location_0.location_code ,5 ,4)) end ,'-1') ) as host_id ,
max(coalesce(hr_organizacion_0.name , hr_location_0.description, 's/i')) as organizacion_desc ,
max(coalesce(hr_organizacion_0.type_x, 's/i')) as organizacion_tipo,
max(coalesce(hr_organizacion_0.storeday , hr_location_0.storeday)) as storeday
from gb_mdl_mexico_costoproducir.hr_location hr_location_0
full outer join gb_mdl_mexico_costoproducir.hr_organizacion hr_organizacion_0 on hr_location_0.location_id = hr_organizacion_0.location_id
where case when hr_organizacion_0.organization_id is null then cast(concat(substr(hr_location_0.location_code ,1 ,3),substr(hr_location_0.location_code ,5 ,4)) as int) * -1 else hr_organizacion_0.organization_id end <> 0
and substr(hr_location_0.location_code,1,3) in
(select entidadlegal_id
from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf
group by entidadlegal_id)
group by case
when hr_organizacion_0.organization_id is null then cast(concat(substr(hr_location_0.location_code ,1 ,3),substr(hr_location_0.location_code ,5 ,4)) as int) * -1
else hr_organizacion_0.organization_id
end,
hr_organizacion_0.storeday
)tbl_fte;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_plantas AS SELECT a.mf_organizacion_id AS mf_organizacion_id, 
a.planta_id AS planta_id, 
a.gerencia_id AS gerencia_id, 
a.planta_ds AS planta_ds, 
a.sistema_fuente AS sistema_fuente, 
a.organizacioninventario AS organizacioninventario, 
a.siglas AS siglas, 
a.storeday AS storeday, 
a.entidadlegal_id AS entidadlegal_id 
FROM (SELECT CAST(MF_Organizacion_ID AS INT) AS MF_Organizacion_ID,
CAST(Planta_ID AS STRING) AS Planta_ID,
CAST(Gerencia_ID AS INT) AS Gerencia_ID,
CAST(MAX(tblfte.planta_ds) AS STRING) AS Planta_DS,
CAST(Origen AS STRING) AS Sistema_Fuente,
CAST(OrganizacionInventario AS STRING) AS OrganizacionInventario,
CAST(Siglas AS STRING) AS Siglas,
FROM_UNIXTIME(UNIX_TIMESTAMP()) AS storeday,
CAST(EntidadLegal_ID AS STRING) AS EntidadLegal_ID
FROM
(SELECT mp.entidadlegal_id AS EntidadLegal_ID,
CASE
WHEN mp.org_id= -1 THEN CAST(CONCAT('-',mp.planta_id) AS INT)
ELSE CAST(mp.org_id AS INT)
END AS MF_Organizacion_ID,
mp.planta_id AS Planta_ID,
CAST(COALESCE(TRIM(mp.gerencia_id),'-2') AS INT) AS Gerencia_ID,
COALESCE(mp.planta_ds, 'S/I') AS Planta_DS,
CAST('CP' AS STRING) AS Origen,
mp.orginv AS OrganizacionInventario,
mp.siglas AS Siglas
FROM cp_flat_files.mf_plantas mp,
gb_mdl_mexico_costoproducir_views.V_ENTIDADESLEGALES_ACTIVAS_MF vea
WHERE mp.entidadlegal_id IS NOT NULL
AND mp.org_id IS NOT NULL
AND mp.planta_id IS NOT NULL
AND mp.entidadlegal_id = vea.entidadlegal_id
UNION ALL
SELECT SUBSTR(hr_location_0.location_code,1,3) AS EntidadLegal_ID,
hr_organizacion_0.organization_id AS MF_Organizacion_ID,
SUBSTR(hr_location_0.location_code,5,4) AS Planta_ID,
-1 AS Gerencia_ID,
hr_organizacion_0.name AS Planta_DS,
CAST('ERP' AS STRING) AS Origen,
CAST(NULL AS STRING) AS OrganizacionInventario,
CAST(NULL AS STRING) AS Siglas
FROM gb_mdl_mexico_costoproducir.HR_Location HR_Location_0,
gb_mdl_mexico_costoproducir.HR_Organizacion HR_Organizacion_0,
gb_mdl_mexico_costoproducir_views.V_GET_PLANTAS vgp WHERE hr_location_0.location_id = hr_organizacion_0.location_id
AND hr_organizacion_0.type_x = 'PLANTA'
AND CAST(SUBSTR(hr_location_0.location_code,1,3) AS INT) <> vgp.entidadlegal_id
AND hr_organizacion_0.organization_id <> CAST(vgp.mf_organizacion_id AS INT)
AND CAST(SUBSTR(HR_Location_0.LOCATION_CODE,1,3) AS INT) IN
(SELECT vea3.EntidadLegal_ID
FROM gb_mdl_mexico_costoproducir_views.V_ENTIDADESLEGALES_ACTIVAS_MF vea3
GROUP BY vea3.EntidadLegal_ID) ) TBLFTE
LEFT OUTER JOIN cp_flat_files.V_ORGANIZACION_INACTIVA_MF voi ON tblfte.entidadlegal_id = voi.entidadlegal_id
AND tblfte.mf_organizacion_id = voi.mf_organizacion_id
GROUP BY tblfte.entidadlegal_id,
tblfte.mf_organizacion_id,
tblfte.planta_id,
tblfte.gerencia_id,
tblfte.origen,
tblfte.organizacioninventario,
tblfte.siglas) A;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_presentacion AS SELECT VDW_MF_PRESENTACION.presentacion_id AS presentacion_id, VDW_MF_PRESENTACION.presentacion_desc AS presentacion_desc, VDW_MF_PRESENTACION.storeday AS storeday FROM (SELECT RANK()OVER (ORDER BY stg.presentacion_desc) + p.presentacion_id AS Presentacion_ID
,stg.presentacion_desc,FROM_UNIXTIME(UNIX_TIMESTAMP()) AS storeday
FROM(SELECT COALESCE(b.segment7,'S/I') AS Presentacion_Desc
FROM gb_mdl_mexico_costoproducir.MTL_CATEGORIAS B
INNER JOIN gb_mdl_mexico_costoproducir.MTL_Categoria_Materiales C ON b.category_id = c.category_id 
AND c.category_set_id = 27 
WHERE UPPER(TRIM(COALESCE(B.SEGMENT7,'S/I'))) NOT IN 
(SELECT UPPER(TRIM(Presentacion_Desc)) FROM gb_mdl_mexico_manufactura.MF_Presentacion) 
AND b.segment7 IS NOT NULL 
GROUP BY b.segment7
) STG
,
(
SELECT COALESCE(MAX(mf_presentacion.presentacion_id),0) AS Presentacion_ID
FROM gb_mdl_mexico_manufactura.MF_Presentacion
WHERE mf_presentacion.presentacion_id > -1
) P) VDW_MF_PRESENTACION;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_producto_organizacion AS SELECT VDW_MF_PRODUCTO_ORGANIZACION.organization_id AS mf_organizacion_id, VDW_MF_PRODUCTO_ORGANIZACION.planta_id AS planta_id, VDW_MF_PRODUCTO_ORGANIZACION.inventory_item_id AS mf_producto_id, VDW_MF_PRODUCTO_ORGANIZACION.segment1 AS producto_id, VDW_MF_PRODUCTO_ORGANIZACION.item_type AS tipo_producto_id, VDW_MF_PRODUCTO_ORGANIZACION.description AS descripcion, VDW_MF_PRODUCTO_ORGANIZACION.marca_id AS marca_id, VDW_MF_PRODUCTO_ORGANIZACION.presentacion_id AS presentacion_id, VDW_MF_PRODUCTO_ORGANIZACION.categoria_id AS categoria_id, VDW_MF_PRODUCTO_ORGANIZACION.linea_id AS linea_id, VDW_MF_PRODUCTO_ORGANIZACION.sublinea_id AS sublinea_id, VDW_MF_PRODUCTO_ORGANIZACION.gramaje AS gramaje, VDW_MF_PRODUCTO_ORGANIZACION.mf_unidadmedida_id AS mf_unidadmedida_id, VDW_MF_PRODUCTO_ORGANIZACION.mf_envase_id AS mf_envase_id, VDW_MF_PRODUCTO_ORGANIZACION.vida_anaquel AS vida_anaquel, VDW_MF_PRODUCTO_ORGANIZACION.contenedor_desc AS contenedor_desc, VDW_MF_PRODUCTO_ORGANIZACION.cupo_contenedor AS cupo_contenedor, VDW_MF_PRODUCTO_ORGANIZACION.cupo_envase AS cupo_envase, VDW_MF_PRODUCTO_ORGANIZACION.tope_devolucion AS tope_devolucion, VDW_MF_PRODUCTO_ORGANIZACION.indicador_eye AS indicador_eye, VDW_MF_PRODUCTO_ORGANIZACION.origen AS origen, VDW_MF_PRODUCTO_ORGANIZACION.fecha_alta AS fecha_alta, VDW_MF_PRODUCTO_ORGANIZACION.fecha_mod AS fecha_mod, VDW_MF_PRODUCTO_ORGANIZACION.storeday AS storeday, VDW_MF_PRODUCTO_ORGANIZACION.entidadlegal_id AS entidadlegal_id FROM (SELECT 
ma.organization_id            
,ma.planta_id                     
,ma.inventory_item_id
,ma.segment1
,ma.item_type
,ma.description
,ma.marca_id
,ma.presentacion_id
,ma.categoria_id
,ma.linea_id
,ma.sublinea_id
,ma.gramaje
,ma.mf_unidadmedida_id
,ma.mf_envase_id
,ma.vida_anaquel
,CASE
WHEN ma.contenedor_desc = 'S/I' THEN MAX(ma.contenedor_desc) OVER(PARTITION BY ma.entidadlegal_id, ma.inventory_item_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
ELSE ma.contenedor_desc
END AS Contenedor_Desc
,ma.cupo_contenedor 
,ma.cupo_envase 
,ma.tope_devolucion
,ma.indicador_eye
,ma.origen
,ma.fecha_alta
,ma.fecha_mod
,FROM_UNIXTIME(UNIX_TIMESTAMP()) as storeday
,ma.entidadlegal_id        
FROM 
(
SELECT 
b.entidadlegal_id AS EntidadLegal_ID
,b.organization_id AS Organization_ID      
,b.planta_id AS Planta_ID      
,b.inventory_item_id AS Inventory_Item_ID
,COALESCE(TRIM(b.segment1),'-1') AS Segment1
,CASE 
WHEN TRIM(b.item_type) = 'PT' THEN 1
WHEN TRIM(b.item_type) = 'SEMITER' THEN 2
WHEN TRIM(b.item_type) = 'MP' THEN 3
ELSE -1
END AS ITEM_TYPE
,COALESCE(b.description,'S/I') AS Description
,COALESCE(MAX(m.marca_id), -1) AS Marca_ID
,COALESCE(MAX(p.presentacion_id), -1) AS Presentacion_ID
,COALESCE(MAX(cr.categoria_id), -1) AS Categoria_ID
,COALESCE(MAX(l.linea_id), -1) AS Linea_ID
,COALESCE(MAX(sl.sublinea_id), -1) AS Sublinea_ID              
,MAX(COALESCE(g.contenido_neto,po.gramaje,cf.cross_reference,pr.contenidoneto,0)) AS Gramaje
,COALESCE(MAX(uom.mf_unidadmedida_id), -1) AS MF_UnidadMedida_ID
,COALESCE(MAX(eom.mf_unidadmedida_id), MAX(va.mf_unidadmedida_idalterno), -1) AS MF_Envase_ID
,COALESCE(MAX(pr.vidaanaquel),MAX(va.vidaanaquel),0) AS Vida_Anaquel 
,COALESCE(MAX(b.attribute4), MAX(va.contenedor),'S/I') AS Contenedor_Desc
,COALESCE(MAX(eom.conversion_rate),0) AS Cupo_Contenedor
,COALESCE(MAX(b.attribute5), MAX(va.cupo_envasealterno),0) AS Cupo_Envase
,COALESCE(MAX(pr.topedevolucion),0) AS Tope_Devolucion
,MAX(CAST(CASE
WHEN TRIM(b.item_type) = 'MP' AND (UPPER(eye.resource_code) <> 'ENV Y ENV' OR eye.resource_code IS NULL) THEN 0
WHEN TRIM(b.item_type) = 'MP' AND UPPER(eye.resource_code) = 'ENV Y ENV' THEN 1
WHEN TRIM(b.item_type) <> 'MP' THEN -1 -- Se trata de PT o SEMITER
END AS INT)) AS Indicador_EyE
,MAX('MEXICO') AS Origen
,FROM_UNIXTIME(UNIX_TIMESTAMP()) AS Fecha_Alta
,FROM_UNIXTIME(UNIX_TIMESTAMP()) AS Fecha_Mod
FROM
(
SELECT 
x.entidadlegal_id
,x.organization_id
,x.planta_id      
,x.inventory_item_id
,x.segment1
,x.item_type AS ITEM_TYPE
,x.primary_uom_code
,x.description
,CASE
WHEN x.attribute4 IS NULL 
THEN MAX(x.attribute4) OVER(PARTITION BY x.entidadlegal_id, x.inventory_item_id ROWS BETWEEN UNBOUNDED PRECEDING 
AND UNBOUNDED FOLLOWING)
ELSE x.attribute4 
END AS Attribute4
,CASE
WHEN x.attribute5 IS NULL 
THEN MAX(x.attribute5) OVER(PARTITION BY x.entidadlegal_id, x.inventory_item_id ROWS BETWEEN UNBOUNDED PRECEDING 
AND UNBOUNDED FOLLOWING)
ELSE x.attribute5 
END AS Attribute5
FROM 
(
SELECT
p.entidadlegal_id
,a.organization_id
,p.planta_id      
,a.inventory_item_id
,a.segment1
,a.item_type AS ITEM_TYPE
,a.primary_uom_code
,MAX(TRIM(a.description)) AS Description
,MAX(TRIM(a.attribute4))  AS Attribute4
,MAX(TRIM(a.attribute5))  AS Attribute5
FROM gb_mdl_mexico_costoproducir.MTL_CATALOGO_MATERIALES A
INNER JOIN 
(
SELECT p.entidadlegal_id, p.mf_organizacion_id, p.planta_id
FROM gb_mdl_mexico_manufactura.MF_Plantas P 
WHERE P.EntidadLegal_ID IN (SELECT EntidadLegal_ID 
FROM gb_mdl_mexico_costoproducir_views.V_ENTIDADESLEGALES_ACTIVAS_MF GROUP BY EntidadLegal_ID)
GROUP BY p.entidadlegal_id, p.mf_organizacion_id, p.planta_id
) P ON a.organization_id = p.mf_organizacion_id
WHERE TRIM(a.item_type) IN ('MP','PT','SEMITER') 
AND FIND_IN_SET('-',a.segment1) = 0 
AND SUBSTRING(a.segment1,1,1) IN ('0','1','2','3','4','5','6','7','8','9') 
AND SUBSTRING(a.segment1,LENGTH(a.segment1),1) IN ('0','1','2','3','4','5','6','7','8','9')
AND a.inventory_item_id NOT IN (252100, 191537) -- Valores no validos
GROUP BY p.entidadlegal_id,a.organization_id,p.planta_id,a.inventory_item_id,a.segment1,a.item_type,a.primary_uom_code           
) X
) B
LEFT OUTER JOIN gb_mdl_mexico_costoproducir.MTL_Categoria_Materiales   IC 
ON b.inventory_item_id = ic.inventory_item_id
AND b.organization_id = ic.organization_id  
AND ic.category_set_id = 27

LEFT OUTER JOIN gb_mdl_mexico_costoproducir.MTL_Categorias C
ON ic.category_id = c.category_id     
LEFT OUTER JOIN gb_mdl_mexico_manufactura.MF_Marca M
ON UPPER(TRIM(COALESCE(c.segment5,'S/I')))=UPPER(TRIM(m.marca_desc))
LEFT OUTER JOIN gb_mdl_mexico_manufactura.MF_Presentacion P
ON UPPER(TRIM(COALESCE(c.segment7,'S/I')))=UPPER(TRIM(p.presentacion_desc))
LEFT OUTER JOIN gb_mdl_mexico_manufactura.MF_Categoria CR
ON UPPER(TRIM(COALESCE(c.segment2,'S/I')))=UPPER(TRIM(cr.categoria_desc))
LEFT OUTER JOIN gb_mdl_mexico_manufactura.MF_Linea L
ON UPPER(TRIM(COALESCE(c.segment3,'S/I')))=UPPER(TRIM(l.linea_desc))
LEFT OUTER JOIN gb_mdl_mexico_manufactura.MF_Sublinea SL
ON UPPER(TRIM(COALESCE(c.segment4,'S/I')))=UPPER(TRIM(sl.sublinea_desc))
LEFT OUTER JOIN gb_mdl_mexico_costoproducir.MF_GRAMAJES G
ON b.entidadlegal_id=g.entidadlegal_id
AND b.segment1=g.item
LEFT OUTER JOIN 
(
SELECT a.entidadlegal_id, a.mf_organizacion_id, a.planta_id, a.mf_producto_id, a.gramaje
FROM gb_mdl_mexico_manufactura.MF_Producto_Organizacion a
WHERE a.EntidadLegal_ID IN (SELECT EntidadLegal_ID FROM gb_mdl_mexico_costoproducir_views.V_ENTIDADESLEGALES_ACTIVAS_MF GROUP BY EntidadLegal_ID)
AND a.gramaje <> 0 AND a.origen = 'MEXICO'
GROUP BY a.entidadlegal_id, a.mf_organizacion_id, a.planta_id, a.mf_producto_id, a.gramaje
) PO
ON b.entidadlegal_id=po.entidadlegal_id 
AND b.organization_id=po.mf_organizacion_id
AND b.inventory_item_id=po.mf_producto_id
LEFT OUTER JOIN 
(
SELECT 
cr.organization_id
,cr.inventory_item_id
,CAST(
CASE
WHEN TRIM(cr.description) IN ('KG','KG (GLO)','KG.','KG_OBL','KILOGRAMO','KILOGRAMOS') THEN (cr.cross_reference * 1000)  
ELSE cr.cross_reference 
END AS FLOAT
) AS Cross_Reference 
FROM gb_mdl_mexico_costoproducir_views.vdw_mtl_referencia_cruzada_mat CR 
INNER JOIN
(
SELECT cr.organization_id, cr.inventory_item_id,MAX(cr.last_update_date) AS Last_Update_Date
FROM gb_mdl_mexico_costoproducir_views.vdw_mtl_referencia_cruzada_mat CR 
WHERE cr.cross_reference_type = 'ROW PE'
AND SUBSTR(cr.cross_reference,LENGTH(cr.cross_reference),1) IN ('0','1','2','3','4','5','6','7','8','9')
GROUP BY cr.organization_id, cr.inventory_item_id,cr.cross_reference
) M
ON cr.organization_id = m.organization_id 
AND cr.inventory_item_id = m.inventory_item_id 
AND cr.last_update_date = m.last_update_date 
WHERE cr.cross_reference_type = 'ROW PE' 
AND SUBSTR(cr.cross_reference,LENGTH(cr.cross_reference),1) IN ('0','1','2','3','4','5','6','7','8','9')
) CF 
ON CAST(b.organization_id AS STRING)  =  CAST(cf.organization_id AS STRING)
AND b.inventory_item_id  =  cf.inventory_item_id
LEFT OUTER JOIN gb_mdl_mexico_costoproducir.P_PRODUCTO PR 
ON CAST(b.segment1 AS INT) = pr.producto_id
LEFT OUTER JOIN gb_mdl_mexico_manufactura.MF_Unidad_Medida  UOM 
ON UPPER(TRIM(b.primary_uom_code)) =  UPPER(TRIM(uom.host_codigo))
LEFT OUTER JOIN 
(
SELECT 
b.organization_id
, eom.inventory_item_id
, eom.uom_code
, eom.conversion_rate
, COALESCE(uom.mf_unidadmedida_id,-1)  AS MF_UnidadMedida_ID
, eom.last_update_date
, eom.default_conversion_flag
FROM 
(
SELECT 
eom.inventory_item_id
,eom.uom_code
,eom.conversion_rate
,eom.uom_class   
,eom.last_update_date
,eom.default_conversion_flag
FROM gb_mdl_mexico_costoproducir.MTL_CONVERSIONES_UDM  EOM
INNER JOIN
(
SELECT eom.inventory_item_id, MAX(concat(eom.last_update_date,eom.last_update_date_h)) AS Last_Update_Date
FROM gb_mdl_mexico_costoproducir.MTL_CONVERSIONES_UDM  EOM
INNER JOIN
(
SELECT mtl_conversiones_udm.inventory_item_id, MIN(concat(mtl_conversiones_udm.disable_date,mtl_conversiones_udm.disable_date_h)) AS Disable_Date
FROM gb_mdl_mexico_costoproducir.MTL_CONVERSIONES_UDM  
WHERE mtl_conversiones_udm.uom_class = 'CANTIDAD'  AND mtl_conversiones_udm.uom_code <> 'Pza'
GROUP BY mtl_conversiones_udm.inventory_item_id
) M ON eom.inventory_item_id = m.inventory_item_id 
AND concat(eom.disable_date,eom.disable_date_h) = m.disable_date
WHERE eom.uom_class = 'CANTIDAD'  AND eom.uom_code <> 'Pza'
GROUP BY eom.inventory_item_id
) M ON eom.inventory_item_id = m.inventory_item_id 
AND concat(eom.last_update_date,eom.last_update_date_h) = m.last_update_date
) EOM  
INNER JOIN gb_mdl_mexico_manufactura.MF_UNIDAD_MEDIDA UOM     
ON TRIM(eom.uom_code)=TRIM(uom.host_codigo)
INNER JOIN gb_mdl_mexico_costoproducir.MTL_CATALOGO_MATERIALES B 
ON eom.inventory_item_id  = b.inventory_item_id  
WHERE eom.uom_class='CANTIDAD'  
AND TRIM(b.item_type) IN ('MP','PT','SEMITER')  
AND uom.categoria_medida = 'CANTIDAD' 
GROUP BY b.organization_id,eom.inventory_item_id, eom.uom_code,eom.conversion_rate,uom.mf_unidadmedida_id, eom.last_update_date, eom.default_conversion_flag
) EOM
ON b.organization_id=eom.organization_id 
AND b.inventory_item_id=eom.inventory_item_id   
LEFT OUTER JOIN
(
SELECT 
fm.organization_id
,fm.inventory_item_id
,fm.shelf_life_days   AS VidaAnaquel
,fm.attribute4 AS Contenedor
,fm.attribute5 AS Cupo_EnvaseAlterno
,fm.attribute6 AS EnvaseAlterno
,COALESCE(uom.mf_unidadmedida_id,-1)  AS MF_UnidadMedida_IDAlterno
FROM gb_mdl_mexico_costoproducir.MTL_FLEXFIELDS_MATERIALES  FM 
INNER JOIN
(
SELECT mtl_flexfields_materiales.organization_id, mtl_flexfields_materiales.inventory_item_id, MAX(mtl_flexfields_materiales.last_update_date) AS Last_Update_Date 
FROM gb_mdl_mexico_costoproducir.MTL_FLEXFIELDS_MATERIALES
GROUP BY mtl_flexfields_materiales.organization_id, mtl_flexfields_materiales.inventory_item_id
) M ON fm.organization_id = m.organization_id 
AND fm.inventory_item_id = m.inventory_item_id 
AND fm.last_update_date = m.last_update_date
LEFT JOIN gb_mdl_mexico_manufactura.MF_UNIDAD_MEDIDA UOM 
ON TRIM(fm.attribute6)=TRIM(uom.host_codigo)
) VA
ON b.organization_id=va.organization_id 
AND b.inventory_item_id=va.inventory_item_id
LEFT OUTER JOIN 
(
SELECT bom.organization_id, cst.inventory_item_id,bom.resource_code,cst.cost_element_id,cst.cost_type_id
FROM gb_mdl_mexico_costoproducir.BOM_RESOURCES BOM
INNER JOIN gb_mdl_mexico_costoproducir_views.VWH_CST_ITEM_COST_DETAILS CST 
ON cst.organization_id = bom.organization_id 
AND cst.resource_id = bom.resource_id           
INNER JOIN 
(
SELECT p.entidadlegal_id, p.mf_organizacion_id, p.planta_id
FROM gb_mdl_mexico_manufactura.MF_Plantas P 
WHERE P.EntidadLegal_ID IN (SELECT EntidadLegal_ID 
FROM gb_mdl_mexico_costoproducir_views.V_ENTIDADESLEGALES_ACTIVAS_MF 
GROUP BY EntidadLegal_ID)
GROUP BY p.entidadlegal_id, p.mf_organizacion_id, p.planta_id
) P ON bom.organization_id = p.mf_organizacion_id
WHERE cst.cost_element_id=1
AND cst.cost_type_id = 1
AND cst.item_cost <> 0
AND UPPER(bom.resource_code) IN ('ENV Y ENV')
GROUP BY bom.organization_id, cst.inventory_item_id,bom.resource_code,cst.cost_element_id,cst.cost_type_id
) EyE
ON B.Organization_ID = eye.organization_id 
AND B.Inventory_Item_ID = eye.inventory_item_id
GROUP BY b.entidadlegal_id,b.organization_id,b.planta_id,b.inventory_item_id,b.item_type,b.segment1,b.description
) MA) VDW_MF_PRODUCTO_ORGANIZACION;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_regiones AS select 
     a.region_id  as region_id,
     coalesce(a.pais_id,-1) as pais_id ,
     coalesce(a.region_ds, 's/i') as region_ds ,
     a.storeday as storeday,
     a.entidadlegal_id as entidadlegal_id
from cp_flat_files.mf_regiones a
join gb_mdl_mexico_costoproducir.g_pais g on a.pais_id = g.pais_id
join gb_mdl_mexico_costoproducir.o_entidad_legal el on a.entidadlegal_id = el.entidadlegal_id
left outer join
  (select mf_regiones.entidadlegal_id,
          mf_regiones.region_id,
          mf_regiones.pais_id
   from gb_mdl_mexico_manufactura.mf_regiones) mfr on a.entidadlegal_id = mfr.entidadlegal_id
and a.region_id = mfr.region_id
and a.pais_id = mfr.pais_id
where mfr.entidadlegal_id is null
  and mfr.region_id is null
  and mfr.pais_id is null
  and a.entidadlegal_id is not null
  and a.region_id is not null
  and a.entidadlegal_id in
    (select entidadlegal_id
     from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf
     group by entidadlegal_id);



CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_sublinea AS SELECT VDW_MF_SUBLINEA.sublinea_id AS sublinea_id, VDW_MF_SUBLINEA.sublinea_desc AS sublinea_desc, VDW_MF_SUBLINEA.storeday AS storeday FROM (SELECT RANK()OVER (ORDER BY stg.sublinea_desc) + sl.sublinea_id AS SubLinea_ID
,stg.sublinea_desc,FROM_UNIXTIME(UNIX_TIMESTAMP()) AS storeday
FROM(SELECT sl.sublinea_desc
FROM (SELECT COALESCE(b.segment4,'S/I') AS SubLinea_Desc
FROM gb_mdl_mexico_costoproducir.MTL_CATEGORIAS B
INNER JOIN gb_mdl_mexico_costoproducir.MTL_Categoria_Materiales C 
ON b.category_id = c.category_id 
AND c.category_set_id = 27
INNER JOIN gb_mdl_mexico_costoproducir.MTL_CATALOGO_MATERIALES S 
ON c.inventory_item_id = s.inventory_item_id 
AND TRIM(s.item_type) IN ('MP','PT','SEMITER')
WHERE UPPER(TRIM(COALESCE(B.SEGMENT4,'S/I'))) NOT IN 
(SELECT UPPER(TRIM(SubLinea_Desc)) FROM gb_mdl_mexico_manufactura.MF_SubLinea) 
GROUP BY b.segment4
)SL
)STG
,(
SELECT COALESCE(MAX(mf_sublinea.sublinea_id),0) AS SubLinea_ID
FROM gb_mdl_mexico_manufactura.MF_SubLinea
WHERE mf_sublinea.sublinea_id > -1
)SL) VDW_MF_SUBLINEA;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_transferencias AS select
     concat(substr(regexp_replace( b.transaction_date, '/', '-'),1,7), '-01') as fecha
     ,b.organization_id as mf_organizacion_id
     ,coalesce(c.planta_id,'s/i') as planta_id
     ,coalesce(d.entidadlegal_id,'s/i') as recibe_entidadlegal_id
     ,coalesce(b.transfer_organization_id,-1) as recibe_mf_organizacion_id
     ,coalesce(d.planta_id,'s/i') as recibe_planta_id
     ,b.inventory_item_id as mf_producto_id
     ,sum(b.primary_quantity) * -1 as cantidad
     ,0 as costo
     ,coalesce(max(um.mf_unidadmedida_id), -1) as mf_unidadmedida_id
     ,coalesce(tm.tipomoneda_id,-1) as tipomoneda_id
     ,from_unixtime(unix_timestamp()) as fecha_alta
     ,from_unixtime(unix_timestamp()) as fecha_mod
     ,from_unixtime(unix_timestamp()) as storeday
     ,coalesce(c.entidadlegal_id,'s/i') as entidadlegal_id
from gb_mdl_mexico_costoproducir.mtl_transaccion_materiales b, gb_mdl_mexico_costoproducir_views.v_fechas_extraccion tbl_ftro  
     join gb_mdl_mexico_costoproducir.mtl_catalogo_materiales a on b.organization_id  = a.organization_id  and b.inventory_item_id=a.inventory_item_id
     left outer join gb_mdl_mexico_manufactura.mf_unidad_medida um on um.host_codigo = trim(b.transaction_uom)
     left outer join gb_mdl_mexico_manufactura.mf_plantas c on b.organization_id = c.mf_organizacion_id and lower(c.sistema_fuente) = lower('CP')
     join (select veamf.entidadlegal_id from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf veamf group by veamf.entidadlegal_id) elat on 
     c.entidadlegal_id = elat.entidadlegal_id
     left outer join gb_mdl_mexico_manufactura.mf_plantas d on b.transfer_organization_id = d.mf_organizacion_id and lower(d.sistema_fuente) = lower('CP')
     left outer join 
          (
               select 
                    o.entidadlegal_id
                    ,coalesce(m.tipomoneda_id,-1)           as tipomoneda_id
                    ,o.organizacion_id
                    ,coalesce(m.pais_id,-1)                 as pais_id
               from gb_mdl_mexico_costoproducir.o_entidadlegal_organizacion o
                    left outer join gb_mdl_mexico_costoproducir.o_entidad_legal el on o.entidadlegal_id = el.entidadlegal_id
                    left outer join gb_mdl_mexico_costoproducir.v_tipo_moneda m on m.pais_id = o.pais_id
                    left outer join gb_mdl_mexico_costoproducir.g_pais g on o.pais_id = g.pais_id
                    left outer join gb_mdl_mexico_costoproducir.e_organizacion e on e.organizacion_id = o.organizacion_id
          ) tm
          on c.entidadlegal_id = tm.entidadlegal_id

where 
    b.transaction_date >= tbl_ftro.fechaini and b.transaction_date <= tbl_ftro.fechafin and
     ((b.transaction_source_type_id = 13  and b.transaction_type_id = 21)  or  (b.transaction_source_type_id = 8 and b.transaction_type_id = 62)) 
     and (lower(a.item_type) like 'semi%'  or lower(a.item_type) like 'phan%'  or lower(a.item_type) like '%pt%')
     and lower(b.subinventory_code) not in ('contenedor', 'barredura', 'dev')
     
     
group by
     concat(substr(regexp_replace( b.transaction_date, '/', '-'),1,7), '-01')
     ,coalesce(c.entidadlegal_id,'s/i')
     ,b.organization_id
     ,coalesce(c.planta_id,'s/i')
     ,coalesce(d.entidadlegal_id,'s/i')
     ,coalesce(b.transfer_organization_id,-1)
     ,coalesce(d.planta_id,'s/i')
     ,b.inventory_item_id
     ,coalesce(tm.tipomoneda_id,-1);


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_turnos AS select 
ttf.mf_organizacion_id                       as mf_organizacion_id
,ttf.planta_id                                             as planta_id
,ttf.linea_prod_id                            as linea_prod_id
,ttf.turno_id                                 as turno_id
,case 
WHEN ttf.turno_id = 1 THEN 'Turno Matutino'
WHEN ttf.turno_id = 2 THEN 'Turno Vespertino'
WHEN ttf.turno_id = 3 THEN 'Turno Nocturno'
END                                                        AS Turno_DS     
,ttf.periodo                                               AS Periodo
,ttf.fechaini                                              AS FechaIni
,ttf.fechafin                                              AS FechaFin 
,ttf.turno_hrinicio                                        AS Turno_HrInicio
,ttf.turno_hrfin                                           AS Turno_HrFin
,CASE  
WHEN ttf.tipo_caso = 1 THEN  'TXT'
WHEN ttf.tipo_caso = 2 THEN  'TXT - Rotativos'
END                                                        AS Origen  
,ttf.observaciones                                         AS Observaciones                                                 
,from_unixtime(unix_timestamp())                           AS Fecha_Carga
,NULL                                                      AS Fecha_Vigencia
,from_unixtime(unix_timestamp())                           AS storeday
,ttf.entidadlegal_id                                        as entidadlegal_id
FROM 
(
SELECT 
tf.entidadlegal_id                              AS  EntidadLegal_ID
,tf.mf_organizacion_id                          AS MF_Organizacion_ID 
,tf.planta_id                                   AS Planta_ID
,tf.linea_prod_id                               AS Linea_Prod_ID
,tf.turno_id                                    AS Turno_ID
,tf.periodo                                     AS Periodo
,tf.turno_hrinicio                              AS Turno_HrInicio
,tf.turno_hrfin                                 AS Turno_HrFin
,tf.turnofechaini                               AS TurnoFechaIni
,tf.turnofechafin                               AS TurnoFechaFin
,COALESCE(tf.fecini_sig_ti  - tf.turnofechafin,-1) AS Diferencia
,CASE 
WHEN tf.cuenta_turnos=1 AND tf.cuenta IN (1,2,3) AND tf.turnofechaini <> tf.minfecha THEN tf.minfecha
ELSE tf.turnofechaini
END                                          AS FechaIni 
,CASE 
WHEN tf.cuenta_turnos>1 AND COALESCE(tf.fecini_sig_ti  - tf.turnofechafin,-1) >= 0 THEN date_sub(tf.fecini_sig_ti,1)
WHEN COALESCE(tf.fecini_sig_ti  - tf.turnofechafin,-1) = -1 AND tf.turnofechafin < tf.fecfin_sig_td THEN tf.maxfecha
WHEN tf.cuenta_turnos=1 AND tf.cuenta IN (1,2,3) AND tf.turnofechafin <> tf.maxfecha THEN tf.maxfecha
ELSE tf.turnofechafin
END                                          AS FechaFin   
,tf.tipo_caso                                   AS Tipo_Caso   
,CASE 
WHEN tf.turnofechaini <> 
CASE 
WHEN tf.cuenta_turnos=1 AND tf.cuenta IN (1,2,3) AND tf.turnofechaini <> tf.minfecha THEN tf.minfecha
ELSE tf.turnofechaini
END
THEN concat('Tiempo: ', tf.observaciones ,', Fecha: Se corrigio la fecha')

WHEN tf.turnofechafin <> 
CASE 
WHEN tf.cuenta_turnos>1 AND COALESCE(tf.fecini_sig_ti  - tf.turnofechafin,-1) >= 0 THEN date_sub(tf.fecini_sig_ti,1)
WHEN COALESCE(tf.fecini_sig_ti  - tf.turnofechafin,-1) = -1 AND tf.turnofechafin < tf.fecfin_sig_td THEN tf.maxfecha
WHEN tf.cuenta_turnos=1 AND tf.cuenta IN (1,2,3) AND tf.turnofechafin <> tf.maxfecha THEN tf.maxfecha
ELSE tf.turnofechafin
END
THEN concat('Tiempo: ', tf.observaciones ,', Fecha: Se corrigio la fecha')
ELSE tf.observaciones                           
END                                          AS Observaciones 
FROM
(
SELECT  
T.EntidadLegal_ID
,T.MF_Organizacion_ID
,T.Planta_ID
,T.Linea_Prod_ID
,T.Turno_ID
,T.Periodo
,T.Turno_HrInicio
,T.Turno_HrFin
,T.TurnoFechaIni
,T.TurnoFechaFin
,T.Tipo_Caso
,T.Observaciones
,concat(T.Periodo,'-01')                             as Minfecha
,date_sub(add_months(concat(T.Periodo,'-01'),1), 1)  as Maxfecha

,MIN(t.turnofechaini)  OVER(PARTITION BY  t.entidadlegal_id, t.mf_organizacion_id, t.planta_id, t.linea_prod_id, t.turno_id, t.periodo ORDER BY t.turnofechaini ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)             AS FecIni_Sig_TI
,MIN(t.turnofechafin)  OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID, T.Linea_Prod_ID, T.Turno_ID, T.Periodo ORDER BY T.TurnoFechaIni ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)            AS FecFin_Sig_TI

,MIN(T.TurnoFechaIni)  OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID, T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)    AS FecIni_Sig_TD
,MIN(T.TurnoFechaFin)  OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID, T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)   AS FecFin_Sig_TD

,COUNT(*) OVER (PARTITION BY T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID, T.Linea_Prod_ID, T.Turno_ID, T.Periodo ORDER BY t.turnofechaini)                         AS cuenta_turnos
,COUNT(*) OVER (PARTITION BY T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID, T.Linea_Prod_ID, T.Periodo ORDER BY T.TurnoFechaIni)                        AS cuenta
FROM gb_mdl_mexico_costoproducir_views.wrkt_mf_turnos T

WHERE T.EntidadLegal_ID IN  (SELECT EntidadLegal_ID FROM gb_mdl_mexico_costoproducir_views.V_ENTIDADESLEGALES_ACTIVAS_MF GROUP BY EntidadLegal_ID) 
GROUP BY 
t.entidadlegal_id
,t.mf_organizacion_id
,t.planta_id
,t.linea_prod_id
,t.turno_id
,t.periodo
,t.turno_hrinicio
,t.turno_hrfin
,t.turnofechaini
,t.turnofechafin
,t.tipo_caso
,t.observaciones
,concat(t.periodo,'-01')
,date_sub(add_months(concat(t.periodo,'-01'),1), 1)
) TF
) TTF;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_unidad_medida AS select rank()over (order by stg.uom_code, stg.unit_of_measure, stg.description ) + um.mf_unidadmedida_id as mf_unidadmedida_id
,trim(stg.uom_code) as host_codigo
,trim(stg.unit_of_measure) as desc_corta
,trim(stg.description) as desc_larga
,trim(stg.uom_class) as categoria_medida
,FROM_UNIXTIME(UNIX_TIMESTAMP()) AS storeday
from(select dwhi.uom_code
,dwhi.unit_of_measure
,dwhi.description
,dwhi.uom_class
,dwhi.storeday
from  gb_mdl_mexico_costoproducir.mtl_unidad_medida dwhi
join 
(
select 
uom_code
,max(um6.description) as description
from gb_mdl_mexico_costoproducir.mtl_unidad_medida um6
where (trim(um6.uom_code)) not in (select trim(um5.host_codigo)  from gb_mdl_mexico_manufactura.mf_unidad_medida um5)
group by um6.uom_code
) m 
on dwhi.uom_code = m.uom_code and dwhi.description = m.description
where (trim(dwhi.uom_code)) not in (select trim(host_codigo) from gb_mdl_mexico_manufactura.mf_unidad_medida)
group by dwhi.uom_code,dwhi.unit_of_measure,dwhi.description,dwhi.uom_class, dwhi.storeday

union all

select t2.transaction_uom as uom_code
,'s/i' as unit_of_measure
,'s/i' as description
,'s/i' as uom_class
, t2.storeday
from  gb_mdl_mexico_costoproducir.mtl_transaccion_materiales t2, gb_mdl_mexico_costoproducir_views.v_fechas_extraccion tbl_ftro 
left outer join gb_mdl_mexico_costoproducir.mtl_unidad_medida um1
on (t2.transaction_uom = um1.uom_code)
left outer join gb_mdl_mexico_manufactura.mf_unidad_medida um2
on (t2.transaction_uom = um2.host_codigo)
where um1.uom_code is null
and   um2.host_codigo is null
and t2.transaction_date >= tbl_ftro.fechaini and t2.transaction_date <= tbl_ftro.fechafin
group by t2.transaction_uom, t2.storeday

union all

select t3.primary_uom as uom_code
,'s/i' as unit_of_measure
,'s/i' as description
,'s/i' as uom_class
,t3.storeday
from  erp_mexico_sz.wip_transactions t3

left outer join gb_mdl_mexico_costoproducir.mtl_unidad_medida um3
on (t3.primary_uom = um3.uom_code)
left outer join gb_mdl_mexico_manufactura.mf_unidad_medida um4
on (t3.primary_uom = um4.host_codigo)
where um3.uom_code is null
and   um4.host_codigo is null
group by t3.primary_uom, t3.storeday
)stg
,
(select coalesce(max(mf_unidad_medida.mf_unidadmedida_id),0) as mf_unidadmedida_id
from gb_mdl_mexico_manufactura.mf_unidad_medida
where mf_unidad_medida.mf_unidadmedida_id> -1
)um;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_o_entidad_legal AS select  
  max(vl.description) as nombreentidadlegal
  ,'oracle-mx-r11'         as sistemafuente
  ,'user'                  as usuarioetl
  ,from_unixtime(unix_timestamp())          as fechacarga
  ,from_unixtime(unix_timestamp())          as fechacambio
  ,from_unixtime(unix_timestamp())          as storeday
  ,vl.flex_value           as entidadlegal_id
from erp_mexico_sz.fnd_flex_values_vl vl
  join erp_mexico_sz.fnd_id_flex_segments fs   on cast(vl.flex_value_set_id as double) = fs.flex_value_set_id 
  join erp_mexico_sz.gl_sets_of_books sb       on fs.id_flex_num = sb.chart_of_accounts_id
where
  lower(fs.id_flex_code) = lower('gl#')
  and lower(fs.enabled_flag) = lower('y')
  and fs.application_id = 101 
  and lower(fs.application_column_name) = lower('segment1')
  and substr ( vl.flex_value , 1 , 1 ) between '0' and '9' 
  and substr ( vl.flex_value , 2 , 1 ) between '0' and '9' 
  and substr ( vl.flex_value , 3 , 1 ) between '0' and '9' 
  and length(vl.flex_value) <=3
group by vl.storeday,vl.flex_value;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_v_tipo_cambio_erp AS select
x.monedaorigen_id     as monedaorigen_id
,x.monedadestino_id    as monedadestino_id
,x.fechatipocambio     as fechatipocambio
,cast(x.tipocambio as decimal(18,5))   as tipocambio
,cast(x.fuente_id as string)           as fuente_id
,cast('oracle-mx-r11' as string)       as sistemafuente
,cast('user' as string)                as usuarioetl
,from_unixtime(unix_timestamp())       as fechacarga
,from_unixtime(unix_timestamp())       as fechacambio
,x.storeday
from
(
select
lower(acum_gl_daily_rates.from_currency)      as monedaorigen_id
,lower(acum_gl_daily_rates.to_currency)        as monedadestino_id
,acum_gl_daily_rates.conversion_date           as fechatipocambio
,acum_gl_daily_rates.conversion_rate           as tipocambio
,'gl'                      as fuente_id
,acum_gl_daily_rates.storeday
from gb_mdl_mexico_costoproducir.acum_gl_daily_rates
) x;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_form_ods_cm_ocurrencias_sub AS SELECT
a.periodo as Periodo,  
TRIM(a.entidadlegal_id) AS EntidadLegal_ID,  
a.planta_id as Planta_ID,  
a.subensamble_id as SubEnsamble_ID,  
CAST(a.masa AS DECIMAL (20,10)) AS Masa,
COUNT(a.masa) AS Ocurrencias
FROM gb_mdl_mexico_costoproducir.FORM_STG_SUBENSAMBLES_CP A
GROUP BY a.periodo,a.entidadlegal_id,a.planta_id,a.subensamble_id,a.masa;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_form_ods_cm_max AS SELECT    
A.Periodo,
TRIM(A.EntidadLegal_ID)  AS EntidadLegal_ID,  
A.Planta_ID, 
A.Subensamble_ID  AS Producto_ID,  
MAX(a.masa)  AS Cantidad,  
'SUB' AS Origen
FROM gb_mdl_mexico_costoproducir_views.V_FORM_ODS_CM_OCURRENCIAS_SUB A
INNER JOIN gb_mdl_mexico_costoproducir_views.V_FORM_ODS_CM_OCURRENCIAS_SUB B
ON a.periodo = b.periodo 
AND a.planta_id = b.planta_id 
AND a.subensamble_id = b.subensamble_id
WHERE a.ocurrencias = b.ocurrencias
GROUP BY a.periodo,a.entidadlegal_id,a.planta_id,a.subensamble_id
UNION ALL
SELECT    
a.periodo,  
TRIM(a.entidadlegal_id) AS EntidadLegal_ID,  
a.planta_id,  
a.producto_id,  
MAX(a.cantidadpzasp) Cantidad,  
'PT' as Origen
FROM gb_mdl_mexico_costoproducir.FORM_STG_PRODS_TERMINADOS_CP A  
WHERE (a.cantidadpzasp <> 0)  
GROUP BY a.periodo,a.entidadlegal_id,a.planta_id,a.producto_id;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_form_ods_cm_a_00 AS SELECT TRIM(a.entidadlegal_id) EntidadLegal_ID,  
a.planta_id,  
a.producto_id,  
a.invitem_hijos MateriaPrima_ID,  
a.cantidadpzash/c.cantidad Cantidad,
a.periodo  
FROM  gb_mdl_mexico_costoproducir.FORM_STG_PRODS_TERMINADOS_CP A
LEFT OUTER JOIN gb_mdl_mexico_costoproducir.FORM_STG_SUBENSAMBLES_CP B 
ON a.planta_id = b.planta_id  
AND a.invitem_hijos = b.subensamble_id 
AND a.entidadlegal_id = b.entidadlegal_id 
AND a.periodo=b.periodo
LEFT OUTER JOIN gb_mdl_mexico_costoproducir_views.V_FORM_ODS_CM_MAX C
ON (a.periodo=c.periodo 
AND a.entidadlegal_id=c.entidadlegal_id 
AND a.planta_id=c.planta_id 
AND a.producto_id=c.producto_id) 
WHERE (b.subensamble_id IS NULL)  
AND (a.cantidadpzasp <> 0 AND a.cantidadpzash <> 0)  
AND (c.origen = 'PT' AND c.cantidad <> 0);


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_form_ods_cm_a_01_2 AS SELECT DISTINCT   
a.periodo,  
a.entidadlegal_id,  
a.planta_id,  
a.invitem_hijos,  
a.producto_id,  
a.tranuom,  
a.codigosubinv,  
a.cantidadpzasp,  
a.cantidadpzash  
FROM gb_mdl_mexico_costoproducir.FORM_STG_PRODS_TERMINADOS_CP A
LEFT OUTER JOIN gb_mdl_mexico_costoproducir_views.V_FORM_ODS_CM_A_00 B 
ON a.periodo=b.periodo 
AND a.entidadlegal_id=b.entidadlegal_id 
AND a.planta_id=b.planta_id 
AND a.producto_id=b.producto_id 
AND a.invitem_hijos=b.materiaprima_id
WHERE b.planta_id IS NULL;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_form_ods_cm_a_01_3 AS SELECT
c.periodo,  
c.entidadlegal_id,  
c.planta_id,  
c.producto_id,  
--D.Ingrediente_ID,
CASE WHEN c.codigosubinv = 'PT' THEN d.subensamble_id ELSE d.ingrediente_id END AS Ingrediente_ID,-- Enero, 2013. Se agrego linea de codigo    
--D.CodigoSubInv,  
CASE WHEN c.codigosubinv = 'PT' THEN c.codigosubinv ELSE d.codigosubinv END AS CodigoSubInv,  -- Enero, 2013. Se agrego linea de codigo  
SUM(c.cantidadpzash/f.cantidad) AS Factor,  
e.cantidad AS Masa,  
SUM(d.cantidad) AS Cantidad
FROM gb_mdl_mexico_costoproducir.FORM_ODS_CM_A_01_2  C,
gb_mdl_mexico_costoproducir_views.V_FORM_ODS_CM_MAX E,
gb_mdl_mexico_costoproducir.FORM_STG_SUBENSAMBLES_CP D,
gb_mdl_mexico_costoproducir_views.V_FORM_ODS_CM_MAX F
WHERE  (c.periodo = d.periodo 
AND  c.entidadlegal_id=d.entidadlegal_id  
AND  c.planta_id = d.planta_id  
AND  c.invitem_hijos = d.subensamble_id) 
AND (c.periodo=e.periodo 
AND  c.entidadlegal_id=e.entidadlegal_id  
AND  c.planta_id=e.planta_id    
AND  c.invitem_hijos=e.producto_id) 
AND (c.periodo=f.periodo 
AND  c.entidadlegal_id=f.entidadlegal_id  
AND  c.planta_id=f.planta_id    
AND  c.producto_id=f.producto_id) 
AND (d.subensamble_id <> d.ingrediente_id)  
AND (c.cantidadpzasp <> 0)
AND (e.origen = 'SUB')  
AND (f.origen = 'PT' 
AND f.cantidad <> 0)
GROUP BY c.periodo,c.entidadlegal_id,c.planta_id,c.producto_id,
CASE WHEN c.codigosubinv = 'PT' THEN d.subensamble_id ELSE d.ingrediente_id END,
CASE WHEN c.codigosubinv = 'PT' THEN c.codigosubinv ELSE d.codigosubinv END,e.cantidad;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_form_stg_subs_cp_masa AS select d.periodo
,d.entidadlegal_id
,d.planta_id
,d.subensamble_id
,d.masa
FROM
(
SELECT b.periodo
,b.entidadlegal_id
,b.planta_id
,b.subensamble_id
,b.cuenta
,CASE WHEN B.cuenta = 1 THEN MAX(b.masa)
ELSE MAX(b.masa)
END AS Masa
FROM
(
SELECT a.periodo,a.entidadlegal_id,a.planta_id,a.subensamble_id,a.masa, COUNT(*) AS cuenta
FROM gb_mdl_mexico_costoproducir.FORM_STG_SUBENSAMBLES_CP A
WHERE (UPPER(a.codigosubinv) NOT LIKE 'MASAS%' OR UPPER(a.codigosubinv) NOT LIKE 'SUB%')
GROUP BY a.periodo,a.entidadlegal_id,a.planta_id,a.subensamble_id,a.masa
) B
GROUP BY b.periodo,b.entidadlegal_id,b.planta_id,b.subensamble_id,b.cuenta
) D, (
SELECT b.periodo,b.entidadlegal_id
,b.planta_id
,b.subensamble_id
,MAX(b.cuenta) as cuenta
FROM
(
SELECT c.periodo, c.entidadlegal_id,c.planta_id,c.subensamble_id,c.masa, COUNT(*) AS cuenta
FROM gb_mdl_mexico_costoproducir.FORM_STG_SUBENSAMBLES_CP c
WHERE (UPPER(c.codigosubinv) NOT LIKE 'MASAS%' OR UPPER(c.codigosubinv) NOT LIKE 'SUB%')
GROUP BY c.periodo, c.entidadlegal_id,c.planta_id,c.subensamble_id,c.masa
) B
GROUP BY b.periodo,b.entidadlegal_id,b.planta_id,b.subensamble_id
) E
WHERE d.periodo=e.periodo
AND d.entidadlegal_id=e.entidadlegal_id
AND d.planta_id=e.planta_id
AND d.subensamble_id=e.subensamble_id
AND d.cuenta=e.cuenta
GROUP BY d.periodo,d.entidadlegal_id,d.planta_id,d.subensamble_id,d.masa;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_form_subformulas_cp AS SELECT  
a.periodo as Periodo, 
a.entidadlegal_id as entidadlegal_id, 
a.planta_id as planta_id, 
a.subensamble_id_ori as SubEnsamble_ID_Ori, 
a.subensamble_id as SubEnsamble_ID, 
a.codigosubinv as CodigoSubInv,
a.ingrediente_id as Ingrediente_ID,
a.fact as fact, 
a.delnivel as Delnivel
FROM gb_mdl_mexico_costoproducir.T_FORM_STG_SUBENSFORM_CP a 
LEFT JOIN gb_mdl_mexico_costoproducir.T_FORM_STG_SUBENSFORM_CP b 
ON b.planta_id = a.planta_id 
AND a.entidadlegal_id = b.entidadlegal_id 
AND a.periodo = b.periodo 
AND a.ingrediente_id = b.subensamble_id
WHERE b.periodo IS NULL 
AND UPPER(a.codigosubinv) NOT LIKE '%SUB%'

UNION ALL
--   Obtenemos los items que se obtuvieron en el nivel cero en el PASO 1.
--   Eliminamos Items con factor duplicado por el caso del tipo de dato flot
SELECT  
a.periodo as Periodo, 
a.entidadlegal_id as entidadlegal_id, 
a.planta_id as planta_id, 
a.producto_id as SubEnsamble_ID_Ori,
CAST(NULL AS INT) as SubEnsamble_ID,
CAST(NULL AS STRING) as CodigoSubInv, 
a.materiaprima_id as Ingrediente_ID, 
CAST( ABS(a.cantidad) AS DECIMAL(18,5)) as fact,
CAST(-1 AS INT) as Delnivel -- Tipo de excepcion
FROM gb_mdl_mexico_costoproducir_views.V_FORM_ODS_CM_A_00 a
LEFT OUTER JOIN
(SELECT --- REVISAR ESTE SELECT Y AGREGAR CANTIDA A SELECT STATMENT
a.periodo, 
a.entidadlegal_id, 
a.planta_id, 
a.subensamble_id_ori, 
a.ingrediente_id
,a.fact as fact
FROM gb_mdl_mexico_costoproducir.T_FORM_STG_SUBENSFORM_CP a 
LEFT JOIN gb_mdl_mexico_costoproducir.T_FORM_STG_SUBENSFORM_CP b 
ON b.planta_id = a.planta_id 

AND a.entidadlegal_id = b.entidadlegal_id 
AND a.periodo = b.periodo 
AND a.ingrediente_id = b.subensamble_id
WHERE b.periodo IS NULL
AND UPPER(a.codigosubinv) NOT LIKE '%SUB%' 
) b
ON a.periodo=b.periodo
and a.entidadlegal_id=b.entidadlegal_id
and a.planta_id=b.planta_id
and a.producto_id=b.subensamble_id_ori
and a.materiaprima_id=b.ingrediente_id
and CAST(ABS(a.cantidad) AS DOUBLE) = CAST(b.fact AS DOUBLE)
where b.periodo is null
and b.entidadlegal_id is null
and b.planta_id is null
and b.subensamble_id_ori is null
and b.ingrediente_id is NULL
and b.fact is null

UNION ALL
--Excepcion 2 . Todos los productos de tipo materia prima pero en bimbo tenemos como materia prima al PANCAK,
--es decir, no son subensambles, esto es para el calculo del ultimo nivel

SELECT  
a.periodo as Periodo, 
a.entidadlegal_id as entidadlegal_id, 
a.planta_id as planta_id, 
a.producto_id as SubEnsamble_ID_Ori, 
CAST(NULL AS INT) as SubEnsamble_ID,
CAST(NULL AS STRING) as CodigoSubInv, 
a.ingrediente_id as Ingrediente_ID, 
CAST(ABS(a.cantidad/a.masa*a.factor) AS DOUBLE) fact,
CAST(-2 AS INT) as Delnivel -- Tipo de excepcion
FROM gb_mdl_mexico_costoproducir_views.V_FORM_ODS_CM_A_01_3 A
LEFT OUTER JOIN
(
SELECT  
form_stg_subensambles_cp.periodo,  
form_stg_subensambles_cp.entidadlegal_id, 
form_stg_subensambles_cp.planta_id,
form_stg_subensambles_cp.subensamble_id 
FROM gb_mdl_mexico_costoproducir.FORM_STG_SUBENSAMBLES_CP 
WHERE UPPER(form_stg_subensambles_cp.codigosubinv) LIKE 'SUB%'
GROUP BY form_stg_subensambles_cp.periodo,form_stg_subensambles_cp.entidadlegal_id,form_stg_subensambles_cp.planta_id,form_stg_subensambles_cp.subensamble_id  
) B
ON a.periodo=b.periodo
AND a.entidadlegal_id=b.entidadlegal_id
AND a.planta_id=b.planta_id
AND a.ingrediente_id=b.subensamble_id
LEFT OUTER JOIN
(
SELECT    
a.periodo, 
a.entidadlegal_id, 
a.planta_id, 
a.subensamble_id_ori, 
a.ingrediente_id
,CAST( a.fact AS DECIMAL(18,5)) fact
FROM gb_mdl_mexico_costoproducir.T_FORM_STG_SUBENSFORM_CP a 
LEFT JOIN gb_mdl_mexico_costoproducir.T_FORM_STG_SUBENSFORM_CP b 
ON b.planta_id = a.planta_id 
AND a.entidadlegal_id = b.entidadlegal_id 
AND a.periodo = b.periodo 
AND a.ingrediente_id = b.subensamble_id
WHERE b.periodo IS NULL  
AND UPPER(a.codigosubinv) NOT LIKE '%SUB%' 
) C
ON a.periodo=c.periodo
AND a.entidadlegal_id=c.entidadlegal_id
AND a.planta_id=c.planta_id
AND a.producto_id=c.subensamble_id_ori
AND a.ingrediente_id=c.ingrediente_id
AND CAST(ABS(a.cantidad/a.masa*a.factor) AS DOUBLE)=c.fact
WHERE  (
(a.entidadlegal_id IN ('100','114','153','096') AND UPPER(a.codigosubinv) LIKE'MP%' OR UPPER(a.codigosubinv) LIKE 'PANCAK%')  
OR ( a.entidadlegal_id IN ('101','125') AND UPPER(a.codigosubinv) LIKE 'MP%' OR UPPER(a.codigosubinv) LIKE'PANCAK%' OR UPPER(a.codigosubinv) LIKE'CUSTODI%') -- se agrego custodia para el caso de Barcel
) 
AND (UPPER(a.codigosubinv) NOT LIKE 'PANCAK_SUB' OR UPPER(a.codigosubinv) NOT LIKE 'SUB%') 
AND b.periodo is null
AND b.entidadlegal_id is null
AND b.planta_id is null
AND b.subensamble_id is null
AND c.periodo is null
AND c.entidadlegal_id is null
AND c.planta_id IS null
AND c.subensamble_id_ori is null
AND c.ingrediente_id is null
AND c.fact is null

UNION ALL

SELECT 
a.periodo as Periodo, 
a.entidadlegal_id as entidadlegal_id, 
a.planta_id as planta_id, 
a.producto_id as SubEnsamble_ID_Ori, 
CAST(null AS INT) as SubEnsamble_ID,
CAST(null AS STRING) as CodigoSubInv, 
a.ingrediente_id as Ingrediente_ID, 
CAST(ABS(a.cantidad/a.masa*a.factor) AS DOUBLE) as fact,
CAST(-3 AS INT) as Delnivel -- Tipo de excepcion
FROM gb_mdl_mexico_costoproducir_views.V_FORM_ODS_CM_A_01_3 a
LEFT OUTER JOIN (
SELECT
t_form_stg_subensform_cp.periodo, 
t_form_stg_subensform_cp.entidadlegal_id, 
t_form_stg_subensform_cp.planta_id, 
t_form_stg_subensform_cp.subensamble_id_ori 
FROM gb_mdl_mexico_costoproducir.T_FORM_STG_SUBENSFORM_CP 
WHERE UPPER(t_form_stg_subensform_cp.codigosubinv) NOT LIKE '%SUB%'
) b
on a.periodo=b.periodo
and a.entidadlegal_id=b.entidadlegal_id
and a.planta_id=b.planta_id
and a.producto_id=b.subensamble_id_ori
WHERE UPPER(a.codigosubinv) LIKE '%SUB%' 
and b.periodo is null
and b.entidadlegal_id is null 
and b.planta_id is null
and b.subensamble_id_ori is null

UNION ALL
-- Inserta los SUB que estan como PT en la planta pero que el catalogo esta como MP por lo tante tenemos que contar para la formula
SELECT  
a.periodo as Periodo, 
a.entidadlegal_id as entidadlegal_id, 
a.planta_id as planta_id, 
a.producto_id as SubEnsamble_ID_Ori, 
CAST(NULL AS INT) as SubEnsamble_ID,
CAST(NULL AS STRING) as CodigoSubInv, 
a.ingrediente_id as Ingrediente_ID,
CAST(ABS(a.cantidad/a.masa*a.factor) AS FLOAT) as fact,
CAST(-4 AS INT) as Delnivel -- Tipo de excepcion
FROM gb_mdl_mexico_costoproducir_views.V_FORM_ODS_CM_A_01_3 A,
gb_mdl_mexico_costoproducir.mtl_catalogo_materiales B
WHERE b.organization_id = a.planta_id
AND b.inventory_item_id = a.ingrediente_id
AND LTRIM(UPPER(a.codigosubinv)) LIKE 'SUB%'
AND LTRIM(UPPER(b.item_type)) LIKE 'MP%'

UNION ALL
-- Se agrego esta parte de codigo 2013-01-21       
-- Inserta los PT que son parte de la formula 

SELECT a.periodo as Periodo,
a.entidadlegal_id as entidadlegal_id,
a.planta_id as planta_id,
a.subensamble_id_ori as SubEnsamble_ID_Ori,
a.subensamble_id as SubEnsamble_ID,
a.codigosubinv as CodigoSubInv,
a.ingrediente_id as Ingrediente_ID,
a.fact as fact,
a.delnivel as Delnivel
FROM gb_mdl_mexico_costoproducir.T_FORM_STG_SUBENSFORM_CP a 
WHERE UPPER(a.codigosubinv) LIKE '%PT%';
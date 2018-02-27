
CREATE TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir_views.t_mf_turno_default_dia(
  entidadlegal_id string, 
  mf_organizacion_id int, 
  planta_id string, 
  turno_id int, 
  turno_ds string, 
  fechaini string, 
  fechafin string, 
  fechahoraini string, 
  fechahorafin string, 
  turnohraini string, 
  turnohrafin string);


CREATE TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_fechas_extraccion(
  fechaini string, 
  fechafin string);



CREATE TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_produccion_mt_1(
  transaction_source_id int, 
  organization_id int, 
  inventory_item_id int, 
  transaction_date string, 
  transaction_date_h string, 
  line_id int, 
  production_line_rate float, 
  pt_productoregistrado decimal(15,5), 
  pt_productoembarcado decimal(17,5), 
  pt_productoregistrado_ decimal(31,10), 
  pt_bajas decimal(15,5), 
  pt_bajas_ decimal(31,10), 
  pt_costoproduccionreal int, 
  toneladas int, 
  pt_valor_produccion int);


CREATE TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_produccion_p1_2(
  transaction_source_id int, 
  transaction_date string, 
  transaction_date_h string, 
  organization_id int, 
  inventory_item_id int, 
  turno_id int, 
  line_id int, 
  production_line_rate float, 
  pt_productoregistrado decimal(15,5), 
  pt_productoembarcado decimal(17,5), 
  pt_productoregistrado_ decimal(31,10), 
  pt_bajas decimal(15,5), 
  pt_bajas_ decimal(31,10), 
  pt_costoproduccionreal int, 
  toneladas int, 
  pt_valor_produccion int);


CREATE TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mf_produccion_principal_3(
  transaction_source_id int, 
  transaction_date string, 
  transaction_date_h string, 
  organization_id int, 
  inventory_item_id int, 
  turno_id int, 
  line_id int, 
  production_line_rate float, 
  pt_productoregistrado decimal(15,5), 
  pt_productoembarcado decimal(17,5), 
  pt_productoregistrado_ decimal(31,10), 
  pt_bajas decimal(15,5), 
  pt_bajas_ decimal(31,10), 
  pt_costoproduccionreal int, 
  toneladas int, 
  pt_valor_produccion int);


CREATE TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir_views.wrkt_mf_cross_reference(
  entidadlegal_id string, 
  mf_producto_id int, 
  pe_fechaini string, 
  pe_fechafin string, 
  cross_reference_pe float, 
  cross_reference_pe_fl float, 
  description_pe string, 
  pvm_fechaini string, 
  pvm_fechafin string, 
  cross_reference_pvm float, 
  cross_reference_pvm_fl float, 
  description_pvm string, 
  fechaini string, 
  fechafin string);


CREATE TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir_views.wrkt_mf_turnos(
  mf_organizacion_id int, 
  planta_id string, 
  linea_prod_id int, 
  turno_id int, 
  periodo string, 
  turnofechaini string, 
  turnofechafin string, 
  fechainicio_ant string, 
  fechafin_ant string, 
  fechainicio_sig string, 
  fechafin_sig string, 
  turnohraini string, 
  turnohrafinal string, 
  horainicio_ant string, 
  horafin_ant string, 
  horainicio_sig string, 
  horafin_sig string, 
  turno_hrinicio string, 
  turno_hrfin string, 
  tipo_caso int, 
  observaciones string, 
  fecha_carga string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir_views.wrkt_mf_turnos_default(
  mf_organizacion_id int, 
  planta_id string, 
  linea_prod_id int, 
  turno_id int, 
  turno_ds string, 
  periodo string, 
  fecha_inicia string, 
  fecha_fin string, 
  hora_inicia string, 
  hora_fin string, 
  origen string, 
  observaciones string, 
  fecha_carga string, 
  fecha_vigencia string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir_views.wrkt_mf_turnos_rotativos(
  mf_organizacion_id int, 
  planta_id string, 
  linea_prod_id int, 
  turno_id int, 
  periodo string, 
  fecha_inicia string, 
  fecha_fin string, 
  hora_inicia string, 
  hora_fin string, 
  orden_turno int, 
  procesado string, 
  observaciones string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);

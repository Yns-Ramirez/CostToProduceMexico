CREATE TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir_wrkt.wkrt_cp_operarios_e_2(
  periodo string, 
  entidadlegal_id string, 
  mf_organizacion_id int, 
  planta_id string, 
  centrocostos_id string, 
  total_empleados_cc bigint, 
  total_pago_cc double);


CREATE TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir_wrkt.wkrt_cp_operarios_m_3(
  periodo string, 
  entidadlegal_id string, 
  mf_organizacion_id int, 
  planta_id string, 
  centrocostos_id string, 
  total_empleados_cc bigint, 
  total_pago_cc double, 
  linea_prod_id int, 
  tipomedida_id int, 
  medida_linea float, 
  medida_planta float, 
  medida_cc double, 
  factor_cc_linea_prod double, 
  empleados_asignados_linea double, 
  tipo_asignacion string);


CREATE TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir_wrkt.wkrt_cp_operarios_p_1(
  empleado_id string, 
  posicion_id int, 
  puesto_id string, 
  entidadlegal_id string, 
  areanegocio_id string, 
  mf_organizacion_id int, 
  planta_id string, 
  centrocosto_id string, 
  unidadtrabajo_id string, 
  centrotrabajo_id string, 
  motivobaja_id string, 
  fechainicioposicion string, 
  fechafinposicion string);


CREATE TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir_wrkt.wkrt_cp_operarios_t_4(
  periodo string, 
  entidadlegal_id string, 
  mf_organizacion_id int, 
  planta_id string, 
  linea_prod_id int, 
  turnos_planta_asignados bigint, 
  turnos_planta_produccion bigint, 
  turnos_linea_default bigint, 
  turnos_linea_produccion bigint);
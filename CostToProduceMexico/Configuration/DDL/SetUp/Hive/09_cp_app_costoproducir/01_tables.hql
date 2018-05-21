
-- CREATE TABLE IF NOT EXISTS gb_mdl_mexico_erp.a_pago_empleado(
--   tiponomina_id string, 
--   empleado_id string, 
--   fechapago string, 
--   cuentanatural_id string, 
--   analisislocal_id string, 
--   concepto_id int, 
--   region_id string, 
--   montopago float, 
--   tipomoneda_id int, 
--   sistemafuente string, 
--   usuarioetl string, 
--   fechacarga string, 
--   fechacambio string, 
--   storeday string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.a_saldo_nomina(
  encabezado_id int, 
  detalle_id int, 
  fechamovimiento string, 
  periodo string, 
  areanegocio_id string, 
  cuentanatural_id string, 
  analisislocal_id string, 
  marca_id string, 
  centrocostos_id string, 
  intercost_id string, 
  segment8 string, 
  segment9 string, 
  segment10 string, 
  tipocuenta string, 
  juegolibros_id int, 
  categoriaencabezado_je string, 
  flagencabezado string, 
  nombreencabezado string, 
  descencabezado string, 
  descmovimiento string, 
  montocredito float, 
  montodebito float, 
  tipomoneda_id int, 
  hdr_ultimafechamodificacion string, 
  ultimafechamodificacion string, 
  hdr_status string, 
  status string, 
  hdr_je_source string, 
  hdr_je_batch_id int, 
  hdr_posted_date string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.a_tipo_cambio(
  monedaorigen_id string, 
  monedadestino_id string, 
  fechatipocambio string, 
  tipocambio decimal(18,5), 
  fuente_id string, 
  sistemafuente string, 
  usuarioetl string, 
  fechacarga string, 
  fechacambio string, 
  storeday string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.cp_beneficios_vol(
  periodo string, 
  mf_organizacion_id int, 
  planta_id string, 
  ingrediente_id int, 
  cuentanatural_id string, 
  importe float, 
  fecha_ini string, 
  fecha_fin string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.cp_derivados_fin(
  periodo string, 
  mf_organizacion_id int, 
  planta_id string, 
  ingrediente_id int, 
  importe float, 
  fecha_ini string, 
  fecha_fin string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.cp_factores_prorrateo(
  cuentanatural_id string, 
  analisislocal_id string, 
  centrocostos_id string, 
  factor_id int, 
  dl int, 
  fecha_ini string, 
  fecha_fin string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.cp_gastos_fletes(
  periodo string, 
  mf_organizacion_id int, 
  planta_id string, 
  ingrediente_id int, 
  cuentanatural_id string, 
  analisislocal_id string, 
  importe float, 
  fecha_ini string, 
  fecha_fin string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.cp_gastos_importacion(
  periodo string, 
  mf_organizacion_id int, 
  planta_id string, 
  ingrediente_id int, 
  cuentanatural_id string, 
  importe float, 
  fecha_ini string, 
  fecha_fin string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.cp_lineas_prod_metros(
  mf_organizacion_id int, 
  planta_id string, 
  linea_prod_id int, 
  metros2 float, 
  fecha_ini string, 
  fecha_fin string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.cp_medidas_prorrateo(
  periodo string, 
  mf_organizacion_id int, 
  planta_id string, 
  linea_prod_id int, 
  turno_id int, 
  mf_producto_id int, 
  tipomedida_id int, 
  turnos_linea_default int, 
  turnos_linea_produccion int, 
  turnos_planta_asignados int, 
  turnos_planta_produccion int, 
  factor float, 
  medida_factor float, 
  medida_producto float, 
  medida_turno float, 
  medida_linea float, 
  medida_planta float, 
  medida_entidadlegal float, 
  fecha_carga string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);



CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.cp_operarios(
  periodo string, 
  mf_organizacion_id int, 
  planta_id string, 
  centrocostos_id string, 
  linea_prod_id int, 
  turno_id int, 
  mf_producto_id int, 
  tipomedida_id int, 
  turnos_linea_default int, 
  turnos_linea_produccion int, 
  turnos_planta_asignados int, 
  turnos_planta_produccion int, 
  total_cc_empleado int, 
  total_cc_pago float, 
  medida_factor float, 
  medida_producto float, 
  medida_turno float, 
  medida_linea float, 
  medida_planta float, 
  medida_entidadlegal float, 
  observaciones string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.cp_parametros(
  subrubro_id int, 
  objeto string, 
  campo string, 
  condicion string, 
  operador string, 
  observaciones string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.e_empleado_posicion(
  empleado_id string, 
  numeroperiodo int, 
  fechainicioperiodo string, 
  fechafinperiodo string, 
  posicion_id int, 
  fechainicioposicion string, 
  fechafinposicion string, 
  numerocolaboradorinterno string, 
  tipocolaborador_id string, 
  motivobaja_id string, 
  fechaantiguedad string, 
  sistema_fuente string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.e_posicion(
  posicion_id int, 
  fechainicio string, 
  fechafin string, 
  nombreposicion string, 
  puesto_id string, 
  plan_id string, 
  areanegocio_id string, 
  unidadtrabajo_id string, 
  categoria_id string, 
  nivel_id string, 
  centrotrabajo_id string, 
  centrocosto_id string, 
  convenio_id string, 
  horasxsemana string, 
  tiempocompleto string, 
  posicion_id_original string, 
  sistema_fuente string, 
  fecha_alta string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.e_puesto(
  puesto_id string, 
  descpuesto string, 
  sistema_fuente string, 
  fecha_alta string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.mf_tipo_costo(
  tipo_costo_id int, 
  indicador_costo string, 
  formula string, 
  tipo_costo_desc string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.t_a_rubros_fsg(
  aniosaldo int, 
  messaldo int, 
  linea_id int, 
  nombreconcepto string, 
  areanegocio_id string, 
  cuentanatural_id string, 
  centrocostos_id string, 
  analisislocal_id string, 
  tot_mactividaddelperiodo double, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);

CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.rubros_fsg(
  aniosaldo int, 
  messaldo int, 
  linea_id int, 
  nombreconcepto string, 
  areanegocio_id string, 
  cuentanatural_id string, 
  analisislocal_id string,
  segmentofiscal_id string,
  centrocostos_id string,
  intercost_id string,
  tot_mactividaddelperiodo double, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.v_rubro25_diferencia_inv(
  periodo string, 
  centrocostos_id string, 
  mf_organizacion_id int, 
  mf_producto_id int, 
  monto float, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.v_rubro81_costo_inv(
  mf_organizacion_id int, 
  planta_id string, 
  mf_producto_id int, 
  periodo string, 
  tipomoneda_id int, 
  tsubinven string, 
  costo_capital double, 
  costo double, 
  cantidad double, 
  importe double, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);
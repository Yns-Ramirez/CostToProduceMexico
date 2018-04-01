

CREATE TABLE mig.cp_control_entidadeslegales(
  entidadlegal_id string, 
  organizaciong_ds string, 
  status string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/cp_control_entidadeslegales'
;
CREATE TABLE mig.gx_control_el_peoplenet(
  entidadlegal_id_dwh string, 
  entidadlegal_id_pnet int, 
  organizacion_id string, 
  cadena string, 
  fecha_migracion string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/gx_control_el_peoplenet'
;
CREATE TABLE mig.gx_control_entidades_app(
  entidadlegal_id string, 
  cadena string, 
  aplicacion string, 
  objeto string, 
  campo string, 
  condicion string, 
  operador string, 
  observaciones string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/gx_control_entidades_app'
;
CREATE TABLE mig.gx_control_entidadeslegales(
  entidadlegal_id string, 
  cadena string, 
  status string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/gx_control_entidadeslegales'
;
CREATE TABLE mig.g_organizacion_geografica(
  organizacion_id int, 
  nombreorganizacion string, 
  sistemafuente string, 
  usuarioetl string, 
  fechacarga string, 
  fechacambio string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/g_organizacion_geografica'
;
CREATE TABLE mig.mf_turno_default(
  entidadlegal_id string, 
  mf_organizacion_id int, 
  planta_id string, 
  turno_id int, 
  turno_ds string, 
  turnohraini string, 
  turnohrafinal string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_turno_default'
;
CREATE TABLE mig.mf_tipo_costo(
  tipo_costo_id int, 
  indicador_costo string, 
  formula string, 
  tipo_costo_desc string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_tipo_costo'
;
CREATE TABLE mig.cp_factores(
  factor_id int, 
  factor_ds string, 
  prefijo string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/cp_factores'
;
CREATE TABLE mig.ft_jer_producto(
  categoria_id string, 
  nombrecategoria string, 
  linea_id string, 
  nombrelinea string, 
  sublinea_id string, 
  nombresublinea string, 
  storeday string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/ft_jer_producto'
;
CREATE TABLE mig.ft_producto(
  producto_id string, 
  nombreproducto string, 
  linea_id string, 
  sublinea_id string, 
  marca_id string, 
  nombremarca string, 
  unidadmedida_id string, 
  descripcionunidadmedida string, 
  topedevolucion string, 
  vidaanaquel string, 
  codigobarras string, 
  contenidoneto string, 
  unidadesequivalentes string, 
  presentacion string, 
  id_presentacion string, 
  gramaje string, 
  categoriaburbuja string, 
  categoriahomologadora string, 
  storeday string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/ft_producto'
;
CREATE TABLE mig.ic_calendario_dim(
  fecha string, 
  aniosemanabimbo int, 
  aniomes int, 
  aniotrimestre int, 
  aniosemestre int, 
  anio int)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/ic_calendario_dim'
;
CREATE TABLE mig.v_organizacion_inactiva_mf(
  entidadlegal_id string, 
  mf_organizacion_id int, 
  planta_id string, 
  gerencia_id int, 
  planta_ds string, 
  sistemafuente string, 
  usuarioetl string, 
  fechacarga string, 
  fechacambio string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/v_organizacion_inactiva_mf'
;
CREATE TABLE mig.cp_parametros(
  subrubro_id int, 
  objeto string, 
  campo string, 
  condicion string, 
  operador string, 
  observaciones string, 
  storeday string, 
  entidadlegal_id string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/cp_parametros'
;
CREATE TABLE mig.calendar(
  calendar_date string, 
  day_of_week string, 
  day_of_month string, 
  day_of_year string, 
  day_of_calendar string, 
  weekday_of_month string, 
  week_of_month string, 
  week_of_year string, 
  week_of_calendar string, 
  month_of_quarter string, 
  month_of_year string, 
  month_of_calendar string, 
  quarter_of_year string, 
  quarter_of_calendar string, 
  year_of_calendar string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/calendar'
;
CREATE TABLE mig.wip_repetitive_items(
  wip_entity_id int, 
  line_id int, 
  organization_id int, 
  last_update_date string, 
  last_update_date_h string, 
  last_updated_by float, 
  creation_date string, 
  creation_date_h string, 
  created_by float, 
  last_update_login float, 
  request_id int, 
  program_application_id int, 
  program_id int, 
  program_update_date string, 
  program_update_date_h string, 
  primary_item_id int, 
  alternate_bom_designator string, 
  alternate_routing_designator string, 
  class_code string, 
  wip_supply_type float, 
  completion_subinventory string, 
  completion_locator_id int, 
  load_distribution_priority float, 
  primary_line_flag float, 
  production_line_rate float, 
  overcompletion_tolerance_type float, 
  overcompletion_tolerance_value float, 
  attribute1 string, 
  attribute6 string, 
  storeday string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/wip_repetitive_items'
;
CREATE TABLE mig.mtl_referencia_cruzada_mat(
  inventory_item_id int, 
  organization_id int, 
  cross_reference_type string, 
  cross_reference string, 
  last_update_date string, 
  last_update_date_h string, 
  last_updated_by float, 
  creation_date string, 
  creation_date_h string, 
  created_by float, 
  last_update_login float, 
  description string, 
  org_independent_flag string, 
  request_id float, 
  program_application_id float, 
  program_id float, 
  program_update_date string, 
  program_update_date_h string, 
  attribute1 string, 
  attribute2 string, 
  attribute3 string, 
  attribute4 string, 
  attribute5 string, 
  attribute6 string, 
  attribute7 string, 
  attribute8 string, 
  attribute9 string, 
  attribute10 string, 
  attribute11 string, 
  attribute12 string, 
  attribute13 string, 
  attribute14 string, 
  attribute15 string, 
  attribute_category string, 
  storeday string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mtl_referencia_cruzada_mat'
;
CREATE TABLE mig.mf_unidad_medida(
  mf_unidadmedida_id int, 
  host_codigo string, 
  desc_corta string, 
  desc_larga string, 
  categoria_medida string, 
  storeday string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_unidad_medida'
;
CREATE TABLE mig.mf_linea(
  linea_id int, 
  linea_desc string, 
  storeday string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_linea'
;
CREATE TABLE mig.mf_marca(
  marca_id int, 
  marca_desc string, 
  storeday string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_marca'
;
CREATE TABLE mig.mf_presentacion(
  presentacion_id int, 
  presentacion_desc string, 
  storeday string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_presentacion'
;
CREATE TABLE mig.mf_categoria(
  categoria_id int, 
  tipo_categ_id int, 
  categoria_desc string, 
  storeday string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_categoria'
;
CREATE TABLE mig.mf_sublinea(
  sublinea_id int, 
  sublinea_desc string, 
  storeday string)
LOCATION
  's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_sublinea'
;
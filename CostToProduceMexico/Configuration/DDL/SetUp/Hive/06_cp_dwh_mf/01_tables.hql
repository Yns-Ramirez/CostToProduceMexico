CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_categoria(
  categoria_id int, 
  tipo_categ_id int, 
  categoria_desc string, 
  storeday string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_compras(
  fecha string, 
  mf_organizacion_id int, 
  planta_id string, 
  mf_producto_id int, 
  codigo_sub string, 
  cantidad float, 
  costo float, 
  tipomoneda_id int, 
  fecha_alta string, 
  fecha_mod string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_costo_prod(
  mf_organizacion_id int, 
  planta_id string, 
  mf_producto_id int, 
  periodo string, 
  tipomoneda_id int, 
  tipo_costo_id int, 
  costo float, 
  fecha_alta string, 
  fecha_mod string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_formulas(
  fecha string, 
  mf_organizacion_id int, 
  planta_id string, 
  mf_producto_id int, 
  ingrediente_id int, 
  cantidad float, 
  costoreal float, 
  costoestandar float, 
  tipomoneda_id int, 
  ajuste_flag int, 
  fecha_alta string, 
  fecha_mod string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_formulas_se(
  fecha string, 
  mf_organizacion_id int, 
  planta_id string, 
  mf_producto_id int, 
  subensamble_id int, 
  cantidad float, 
  costoestandar float, 
  tipomoneda_id int, 
  fecha_alta string, 
  fecha_mod string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_gerencias(
  gerencia_id int, 
  region_id int, 
  gerencia_ds string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_gestion(
  periodo string, 
  entidadlegal_id string, 
  mf_organizacion_id int, 
  planta_id string, 
  indicadorconcepto_id int, 
  valor string, 
  fecha_ini timestamp, 
  fecha_fin timestamp)
PARTITIONED BY ( 
  entidad string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_gramajes(
  item string, 
  contenido_neto float, 
  storeday string, 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_linea(
  linea_id int, 
  linea_desc string, 
  storeday string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_lineas_prod(
  linea_prod_id int, 
  grupo_lineas_prod_id int, 
  linea_prod_ds string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_lineas_prod_centro_costos(
  mf_organizacion_id int, 
  planta_id string, 
  centrocostos_id string, 
  linea_prod_id int, 
  dl int, 
  fecha_ini string, 
  fecha_fin string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_marca(
  marca_id int, 
  marca_desc string, 
  storeday string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_organizacion(
  mf_organizacion_id int, 
  host_id string, 
  organizacion_desc string, 
  organizacion_tipo string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_parametro(
  tipo_parametro_id int, 
  fecha string, 
  parametro_desc string, 
  valor string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_plantas(
  mf_organizacion_id int, 
  planta_id string, 
  gerencia_id int, 
  planta_ds string, 
  sistema_fuente string, 
  organizacioninventario string, 
  siglas string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_presentacion(
  presentacion_id int, 
  presentacion_desc string, 
  storeday string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_produccion(
  mf_organizacion_id int, 
  planta_id string, 
  mf_producto_id int, 
  linea_prod_id int, 
  turno_id int, 
  tipomoneda_id int, 
  fecha string, 
  total_registrado float, 
  total_embarcado float, 
  bajas float, 
  valor_produccion float, 
  costo_prod_teorico float, 
  costo_prod_real float, 
  costo_actual float, 
  toneladas float, 
  ritmo float, 
  costo_precio float, 
  gramaje float, 
  fecha_alta string, 
  fecha_mod string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_producto_organizacion(
  mf_organizacion_id int, 
  planta_id string, 
  mf_producto_id int, 
  producto_id string, 
  tipo_producto_id int, 
  descripcion string, 
  marca_id int, 
  presentacion_id int, 
  categoria_id int, 
  linea_id int, 
  sublinea_id int, 
  gramaje float, 
  mf_unidadmedida_id int, 
  mf_envase_id int, 
  vida_anaquel int, 
  contenedor_desc string, 
  cupo_contenedor decimal(18,2), 
  cupo_envase decimal(18,2), 
  tope_devolucion float, 
  indicador_eye int, 
  origen string, 
  fecha_alta string, 
  fecha_mod string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_regiones(
  region_id int, 
  pais_id int, 
  region_ds string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_sublinea(
  sublinea_id int, 
  sublinea_desc string, 
  storeday string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_transferencias(
  fecha string, 
  mf_organizacion_id int, 
  planta_id string, 
  recibe_entidadlegal_id string, 
  recibe_mf_organizacion_id int, 
  recibe_planta_id string, 
  mf_producto_id int, 
  cantidad float, 
  costo float, 
  mf_unidadmedida_id int, 
  tipomoneda_id int, 
  fecha_alta string, 
  fecha_mod string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_turno_default(
  entidadlegal_id string, 
  mf_organizacion_id int, 
  planta_id string, 
  turno_id int, 
  turno_ds string, 
  turnohraini string, 
  turnohrafinal string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_turnos(
  mf_organizacion_id int, 
  planta_id string, 
  linea_prod_id int, 
  turno_id int, 
  turno_ds string, 
  periodo string, 
  fecha_ini string, 
  fecha_fin string, 
  hora_ini string, 
  hora_fin string, 
  origen string, 
  observaciones string, 
  fecha_carga string, 
  fecha_vigencia string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_turnos_espejo(
  mf_organizacion_id int, 
  planta_id string, 
  linea_prod_id int, 
  turno_id int, 
  turno_ds string, 
  periodo string, 
  fecha_ini string, 
  fecha_fin string, 
  hora_ini string, 
  hora_fin string, 
  origen string, 
  observaciones string, 
  fecha_carga string, 
  fecha_vigencia string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS cp_dwh_mf.mf_unidad_medida(
  mf_unidadmedida_id int, 
  host_codigo string, 
  desc_corta string, 
  desc_larga string, 
  categoria_medida string, 
  storeday string);
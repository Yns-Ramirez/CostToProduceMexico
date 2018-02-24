CREATE TABLE IF NOT EXISTS cp_flat_files.cp_beneficios_volumen(
  entidadlegal_id string, 
  periodo string, 
  cuenta string, 
  no_produce decimal(1,0), 
  item string, 
  monto decimal(18,5), 
  storeday string)
PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


CREATE TABLE IF NOT EXISTS cp_flat_files.cp_costo_capital(
  periodo string, 
  entidadlegal_id string, 
  valor decimal(18,2), 
  storeday string)
PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


CREATE TABLE IF NOT EXISTS cp_flat_files.cp_d_fact_prorrateo(
  entidadlegal_id string, 
  cuentanatural_id string, 
  analisislocal_id string, 
  centrocostos_id string, 
  nombrecc string, 
  factor string, 
  directolinea string, 
  storeday string)
PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


CREATE TABLE IF NOT EXISTS cp_flat_files.cp_derivadosfinancieros(
  entidadlegal_id string, 
  mes string, 
  cta string, 
  item string, 
  total string, 
  storeday string)
PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


CREATE TABLE IF NOT EXISTS cp_flat_files.cp_gastos_fletes(
  entidadlegal_id string, 
  periodo string, 
  cuenta string, 
  no_produce string, 
  item string, 
  monto string, 
  storeday string)
PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;



CREATE TABLE IF NOT EXISTS cp_flat_files.cp_importacion_items(
  entidadlegal_id string, 
  periodo string, 
  cuenta string, 
  no_produce string, 
  item string, 
  monto float, 
  storeday string)
PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;



CREATE TABLE IF NOT EXISTS cp_flat_files.cp_lineas_prod_metros(
  entidadlegal_id string, 
  codigoplanta string, 
  linea_id int, 
  mts2 float, 
  storeday string)
PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


CREATE TABLE IF NOT EXISTS cp_flat_files.mf_familiaingrediente(
  entidadlegal_id string, 
  item_id int, 
  familia string, 
  storeday string)
PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


CREATE TABLE IF NOT EXISTS cp_flat_files.mf_gerencias(
  entidadlegal_id string, 
  gerencia_id int, 
  region_id int, 
  gerencia_ds string, 
  storeday string)
PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


CREATE TABLE IF NOT EXISTS cp_flat_files.mf_gramajes(
  entidadlegal_id string, 
  item string, 
  contenidoneto string, 
  storeday string)
PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


CREATE TABLE IF NOT EXISTS cp_flat_files.mf_lineascc(
  entidadlegal_id string, 
  codigoplanta string, 
  linea_id int, 
  cc string, 
  dl int, 
  storeday string)
PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


CREATE TABLE IF NOT EXISTS cp_flat_files.mf_plantas(
  entidadlegal_id string, 
  gerencia_id string, 
  org_id string, 
  planta_id string, 
  planta_ds string, 
  orginv string, 
  siglas string, 
  storeday string)
PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


CREATE TABLE IF NOT EXISTS cp_flat_files.mf_regiones(
  entidadlegal_id string, 
  region_id int, 
  pais_id int, 
  region_ds string, 
  storeday string)
PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


CREATE TABLE IF NOT EXISTS cp_flat_files.mf_turnos(
  entidadlegal_id string, 
  planta_id string, 
  linea_id int, 
  turno_id int, 
  periodo string, 
  fecha_inicia string, 
  fecha_fin string, 
  hora_inicia string, 
  hora_fin string, 
  storeday string)
PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;


CREATE TABLE IF NOT EXISTS cp_flat_files.cp_factores(
  factor_id int, 
  factor_ds string, 
  prefijo string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;

-- verificar si no requiere el delimitador
CREATE TABLE IF NOT EXISTS cp_flat_files.ft_jer_producto(
  categoria_id string, 
  nombrecategoria string, 
  linea_id string, 
  nombrelinea string, 
  sublinea_id string, 
  nombresublinea string, 
  storeday string);

-- verificar si no requiere el delimitador
CREATE TABLE IF NOT EXISTS cp_flat_files.ft_producto(
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
  storeday string);


CREATE TABLE IF NOT EXISTS cp_flat_files.ic_calendario_dim(
  fecha string, 
  aniosemanabimbo int, 
  aniomes int, 
  aniotrimestre int, 
  aniosemestre int, 
  anio int)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;

CREATE TABLE IF NOT EXISTS cp_flat_files.e_pago_empleado_lac(
  tiponomina_id string, 
  empleado_id string, 
  fechapago string, 
  cuentanatural_id string, 
  analisislocal_id string, 
  concepto_id string,
  region_id string,
  montopago string, 
  tipomoneda_id string,
  storeday string)
  PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;

CREATE TABLE IF NOT EXISTS cp_flat_files.e_pago_empleado(
  tiponomina_id string, 
  empleado_id string, 
  fechapago string, 
  cuentanatural_id string, 
  analisislocal_id string, 
  concepto_id string,
  region_id string,
  montopago string, 
  tipomoneda_id string,
  storeday string)
  PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;

CREATE TABLE IF NOT EXISTS cp_flat_files.e_empleado (
  entidadlegal_id STRING, 
  empleado_id string, 
  nombreempleado string, 
  apellidopaternoempleado string, 
  apellidomaternoempleado string, 
  sexo int, 
  fechanacimiento string, 
  numeroseguridadsocial string,
  pais_id int,
  storeday string)
  PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;

CREATE TABLE IF NOT EXISTS cp_flat_files.cp_puestos (  
  entidadlegal_id string, 
  codigo_planta int, 
  puesto_id string,
  perfil string,  
  subrubro_id int, 
  storeday string)
  PARTITIONED BY ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;

CREATE TABLE IF NOT EXISTS cp_flat_files.e_puesto(
  entidadlegal_id string
  ,puesto_id string
  ,descpuesto string
  ,storeday string)
  PARTITIONED BY ( 
  lgl_ent_id string);
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;

CREATE TABLE IF NOT EXISTS cp_flat_files.e_empleado_posicion(
  e_empleado_id string,
  e_organizacion_id string,
  e_numeroperiodo int,
  e_fechainicioperiodo string,
  e_fechafinperiodo string,
  e_fechainicioposicion string,
  e_fechafinposicion string,
  e_rol_id int,
  e_numerocolaboradorinterno string,
  e_tipocolaborador_id string,
  e_motivobaja_id string,
  e_fechaantiguedad string,
  p_posicion_id string,
  p_fechainicio string,
  p_fechafin string,
  p_nombreposicion string,
  p_puesto_id string,
  p_plan_id string,
  p_entidadlegal_id string,
  p_areanegocio_id string,
  p_unidadtrabajo_id string,
  p_categoria_id string,
  p_nivel_id string,
  p_centrotrabajo_id string,
  p_centrocostos_id string,
  p_convenio_id string,
  p_horasxsemana int,
  p_tiempocompleto string,
  storeday string)
  partitioned by ( 
  lgl_ent_id string)
row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile;

-- verificar si no requiere el delimitador
CREATE TABLE IF NOT EXISTS cp_flat_files.v_organizacion_inactiva_mf(
  entidadlegal_id string, 
  mf_organizacion_id int, 
  planta_id string, 
  gerencia_id int, 
  planta_ds string, 
  sistemafuente string, 
  usuarioetl string, 
  fechacarga string, 
  fechacambio string);

-- verificar si no requiere el delimitador
CREATE TABLE IF NOT EXISTS cp_flat_files.mf_grupo_lineas_produccion(
  linea_prod_id int, 
  linea_prod_ds string, 
  grupo_cat_linea_id int, 
  grupo_cat_linea_desc string, 
  storeday string, 
  entidadlegal_id string);

CREATE TABLE IF NOT EXISTS jedox.ext_big_data_12_importes(
  entidadlegal_id string, 
  planta_id string, 
  producto_id string, 
  monto decimal(38,10));


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_210_importes(
  entidadlegal_id string, 
  caso string, 
  valor double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_211_costostd(
  entidadlegal_id string, 
  planta_id string, 
  producto_id string, 
  costoestandarmaquilado double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_25_importes(
  entidadlegal_id string, 
  planta_id string, 
  centrocostos_id string, 
  producto string, 
  monto double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_26a29_importes(
  entidadlegal string, 
  plantas string, 
  ingredientes int, 
  importe double, 
  concepto int);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_29_importestotal(
  entidadlegal string, 
  plantas string, 
  ingredientes int, 
  importe double, 
  concepto int);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_31a24_cc_a_lineas(
  entidadlegal_id string, 
  planta_id string, 
  centrocostos_id string, 
  linea_prod_id int);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_31a34_fsg(
  entidadlegal_id string, 
  areanegocio_id string, 
  centrocostos_id string, 
  subrubro_id int, 
  monto double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_31a34_importes(
  subrubro_id string, 
  entidadlegal_id string, 
  centrocostos_id string, 
  areanegocio_id string, 
  monto double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_34_cc_generales(
  entidadlegal_id string, 
  planta_id string, 
  centrocostos_id string, 
  importe int);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_41y42_fsg_2(
  entidadlegal_id string, 
  planta_id string, 
  centrocostos_id string, 
  monto double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_41y42_importes_2(
  periodo string, 
  entidadlegal_id string, 
  planta_id string, 
  centrocostos_id string, 
  monto double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_43_cc_a_lineas(
  entidad_legal_id string, 
  planta_id string, 
  centrocostos_id string, 
  linea_prod_id float);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_510_importes(
  entidadlegal_id string, 
  areanegocio_id string, 
  centrocostos_id string, 
  monto double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_5111_importes(
  entidadlegal_id string, 
  planta_id string, 
  factor_id int, 
  monto double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_5112_importes(
  entidadlegal_id string, 
  areanegocio_id string, 
  planta_id string, 
  factor_id int, 
  monto double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_512_importes(
  entidadlegal_id string, 
  areanegocio_id string, 
  planta_id string, 
  monto double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_513_importes(
  entidadlegal_id string, 
  planta_id string, 
  monto double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_51a58_fsg(
  subrubro string, 
  entidadlegal_id string, 
  planta_id string, 
  centrocostos_id string, 
  monto double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_51a58_importes(
  entidadlegal_id string, 
  areanegocio_id string, 
  puesto_id string, 
  tipo string, 
  montopago double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_51a58_operarios(
  entidadlegal_id string, 
  mf_organizacion_id int, 
  planta_id string, 
  linea_prod_id int, 
  operarios_linea double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_59_importes(
  entidadlegal_id string, 
  areanegocio_id string, 
  centrocostos_id string, 
  monto double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_81_importes_caso1(
  periodo string, 
  entidadlegal_id string, 
  planta_id string, 
  producto_id string, 
  tipocasosubrubro_id int, 
  importe double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_81_importes_caso2(
  periodo string, 
  entidadlegal_id string, 
  planta_id string, 
  producto_id string, 
  tipocasosubrubro_id int, 
  importe double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_81_importes_caso3(
  periodo string, 
  entidadlegal_id string, 
  planta_id string, 
  producto_id string, 
  importe double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_81_importes_caso4(
  periodo string, 
  entidadlegal_id string, 
  mf_organizacion_id int, 
  planta_id string, 
  monto_producto double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_81_importes_caso5(
  entidadlegal_id string, 
  periodo string, 
  planta_id string, 
  tipocasosubrubro_id int, 
  importe double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_81_importes_caso6(
  periodo string, 
  entidadlegal_id string, 
  mf_organizacion_id int, 
  planta_id string, 
  monto_producto double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_82_importe_costo_capital(
  entidadlegal_id string, 
  costo_capital double);



CREATE TABLE IF NOT EXISTS jedox.ext_big_data_costo_estandar(
  entidadlegal_id string, 
  planta_id string, 
  producto_id string, 
  tipo_costo_id int, 
  costo float);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_drivers_linea(
  entidadlegal_id string, 
  planta_id string, 
  linea_prod_id int, 
  tipomedida_id int, 
  operarios double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_drivers_prorrateo(
  entidadlegal_id string, 
  planta_id string, 
  linea_prod_id int, 
  turno_id int, 
  producto_id int, 
  tipomedida_id int, 
  factor double, 
  medida_factor double, 
  medida_producto double, 
  turnos_linea_produccion int);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_drivers_prorrateo2(
  entidadlegal_id string, 
  planta_id string, 
  linea_prod_id int, 
  turno_id int, 
  tipomedida_id int, 
  turnoslinea int);



CREATE TABLE IF NOT EXISTS jedox.ext_big_data_formulas(
  essubensamble int, 
  entidadlegal_id string, 
  planta_id string, 
  producto_id string, 
  mf_producto_id string, 
  mf_organizacion_id string, 
  mf_ingrediente_id string, 
  ingrediente_id string, 
  tipo_producto int, 
  cantidad double, 
  costoreal double, 
  costoestandar double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_tipocambio(
  monedaorigen_id string, 
  monedadestino_id string, 
  tipocambio decimal(18,5));


CREATE TABLE IF NOT EXISTS jedox.ext_big_data_ultimos_precios(
  entidadlegal_id string, 
  planta_id string, 
  producto_id string, 
  costo_unitario double);


CREATE TABLE IF NOT EXISTS jedox.ext_big_dim_centros_costo(
  entidadlegal_id string, 
  centrocostos_id string);


CREATE TABLE IF NOT EXISTS jedox.ext_big_dim_entidad_legal(
  entidadlegal_id string);


CREATE TABLE IF NOT EXISTS jedox.ext_big_dim_ingredientes(
  entidadlegal_id string, 
  ingrediente_id string, 
  descripcion string);


CREATE TABLE IF NOT EXISTS jedox.ext_big_dim_lineas(
  entidadlegal_id string, 
  linea_id string);


CREATE TABLE IF NOT EXISTS jedox.ext_big_dim_plantas(
  entidadlegal_id string, 
  planta_id string, 
  planta_ds string);


CREATE TABLE IF NOT EXISTS jedox.ext_big_dim_productos(
  entidadlegal_id string, 
  producto_id string, 
  descripcion string);


CREATE TABLE IF NOT EXISTS jedox.ext_big_dim_tipo_costo_estandar(
  tipo_costo_id int, 
  indicador_costo string, 
  formula string, 
  tipo_costo_desc string);
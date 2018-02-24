CREATE TABLE IF NOT EXISTS jedox.cp_data_detalle(
  execution_date string, 
  organizacion_id string, 
  pais_id string, 
  planta_id string, 
  lineas_id string, 
  turno_id string, 
  producto_id string, 
  rubro_id string, 
  value string, 
  tmoneda_id string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string, 
  periodo string);

CREATE TABLE IF NOT EXISTS jedox.cp_data_piezas(
  execution_date string, 
  planta_id string, 
  lineas_id string, 
  turno_id string, 
  producto_id string, 
  concepto string, 
  cantidad string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string, 
  periodo string);


CREATE TABLE IF NOT EXISTS jedox.cp_data_sumario(
  execution_date string, 
  organizacion_id string, 
  pais_id string, 
  planta_id string, 
  lineas_id string, 
  turno_id string, 
  producto_id string, 
  rubro_id string, 
  tipo_costo string, 
  value string, 
  tmoneda_id string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string, 
  periodo string);

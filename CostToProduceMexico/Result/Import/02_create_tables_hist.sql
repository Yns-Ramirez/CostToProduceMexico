-- ==================  HQL ==================
-- Author / Autor             : Yenisei Ramirez
-- Date / Fecha               : Marzo 2018
-- Project /Proyecto          : Costo Producir Mexico
-- Objective / Objetivo       : Create tables historical
-- Subject Area / Area Sujeto : Cost to Produce

CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.cp_data_detalle(
  execution_date string, 
  organizacion_id string, 
  pais_id string, 
  planta_id string, 
  lineas_id string, 
  turno_id string, 
  producto_id string, 
  rubro_id string, 
  value double, 
  tmoneda_id string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string, 
  periodo string);

CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.cp_data_piezas(
  execution_date string, 
  planta_id string, 
  lineas_id string, 
  turno_id string, 
  producto_id string, 
  concepto string, 
  cantidad double, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string, 
  periodo string);


CREATE TABLE IF NOT EXISTS gb_smntc_mexico_costoproducir.cp_data_sumario(
  execution_date string, 
  organizacion_id string, 
  pais_id string, 
  planta_id string, 
  lineas_id string, 
  turno_id string, 
  producto_id string, 
  rubro_id string, 
  tipo_costo string, 
  value double, 
  tmoneda_id string, 
  storeday string)
PARTITIONED BY ( 
  entidadlegal_id string, 
  periodo string);


invalidate metadata;

insert overwrite table gb_smntc_mexico_costoproducir.cp_data_detalle partition(entidadlegal_id, periodo) 
select 
    execution_date,
    organizacion_id,
    pais_id,
    planta_id,
    lineas_id,
    turno_id,
    producto_id,
    rubro_id,
    value,
    tmoneda_id,
    from_unixtime(unix_timestamp()) as storeday,
    entidadlegal_id,
    regexp_replace(periodo, '\'', '')
from gb_smntc_mexico_costoproducir.cp_data_detalle_last_exec;


insert overwrite table gb_smntc_mexico_costoproducir.cp_data_piezas partition(entidadlegal_id, periodo) 
select 
    execution_date,
    planta_id,
    lineas_id,
    turno_id,
    producto_id,
    concepto,
    cantidad,
    from_unixtime(unix_timestamp()) as storeday,
    entidadlegal_id,
    regexp_replace(periodo, '\'', '')
from gb_smntc_mexico_costoproducir.cp_data_piezas_last_exec;

insert overwrite table gb_smntc_mexico_costoproducir.cp_data_sumario partition(entidadlegal_id, periodo) 
select 
    execution_date, 
    organizacion_id, 
    pais_id, 
    planta_id, 
    lineas_id, 
    turno_id, 
    producto_id, 
    rubro_id, 
    tipo_costo, 
    value, 
    tmoneda_id,
    from_unixtime(unix_timestamp()) as storeday,
    entidadlegal_id, 
    regexp_replace(periodo, '\'', '')
from gb_smntc_mexico_costoproducir.cp_data_sumario_last_exec;

-- ==================  HQL ==================
-- Author / Autor             : Yenisei Ramirez
-- Date / Fecha               : Marzo 2018
-- Project /Proyecto          : Costo Producir Mexico
-- Objective / Objetivo       : Create tables historical
-- Subject Area / Area Sujeto : Cost to Produce

invalidate metadata;

insert overwrite table jedox.cp_data_detalle partition(entidadlegal_id, periodo) 
select 
    execution_date,
    organizacion_id,
    pais_id,
    planta_id,
    lineas_id,
    turno_id,
    producto_id,
    rubro_id,
    cast(value as string),
    tmoneda_id,
    from_unixtime(unix_timestamp()) as storeday,
    entidadlegal_id,
    regexp_replace(periodo, '\'', '')
from gb_smntc_mexico_costoproducir.cp_data_detalle_last_exec;


insert overwrite table jedox.cp_data_piezas partition(entidadlegal_id, periodo) 
select 
    execution_date,
    planta_id,
    lineas_id,
    turno_id,
    producto_id,
    concepto,
    cast(cantidad as string),
    from_unixtime(unix_timestamp()) as storeday,
    entidadlegal_id,
    regexp_replace(periodo, '\'', '')
from gb_smntc_mexico_costoproducir.cp_data_piezas_last_exec;

insert overwrite table jedox.cp_data_sumario partition(entidadlegal_id, periodo) 
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
    cast(value as string), 
    tmoneda_id,
    from_unixtime(unix_timestamp()) as storeday,
    entidadlegal_id, 
    regexp_replace(periodo, '\'', '')
from gb_smntc_mexico_costoproducir.cp_data_sumario_last_exec;

-- ======================================================
--  cp_derivados_fin

-- poner fecha fin a los null
insert overwrite table gb_smntc_mexico_costoproducir.cp_derivados_fin partition(entidadlegal_id) 
     select
     periodo, 
     mf_organizacion_id, 
     planta_id, 
     ingrediente_id, 
     importe, 
     fecha_ini,
     case when fecha_fin is null then to_date(from_unixtime(unix_timestamp()) ) 
          else fecha_fin end as fecha_fin,
     storeday,
     entidadlegal_id
     from gb_smntc_mexico_costoproducir.cp_derivados_fin;

-- insertar los registros nuevos poniendo fecha_fin null
insert into table gb_smntc_mexico_costoproducir.cp_derivados_fin partition(entidadlegal_id)
     select 
     mes,
     -100,
     split(cta,"\\.")[1],
     item,
     total,
     date_sub(from_unixtime(unix_timestamp()) , 1) as fecha_ini,
     null as fecha_fin, 
     from_unixtime(unix_timestamp()),
     entidadlegal_id
     from cp_flat_files.cp_derivadosfinancieros;

-- compactar la tabla final
insert overwrite table gb_smntc_mexico_costoproducir.cp_derivados_fin partition(entidadlegal_id) select tmp.* from gb_smntc_mexico_costoproducir.cp_derivados_fin tmp join (select  entidadlegal_id, periodo, mf_organizacion_id,planta_id,ingrediente_id,importe, fecha_ini, max(storeday) as first_record from gb_smntc_mexico_costoproducir.cp_derivados_fin group by entidadlegal_id, periodo, mf_organizacion_id,planta_id,ingrediente_id,importe,fecha_ini) sec on tmp.entidadlegal_id = sec.entidadlegal_id and tmp.periodo = sec.periodo and tmp.mf_organizacion_id = sec.mf_organizacion_id and tmp.planta_id = sec.planta_id and tmp.ingrediente_id = sec.ingrediente_id and substr(tmp.fecha_ini,0,10) = substr(sec.fecha_ini,0,10) and tmp.importe = sec.importe and tmp.storeday = sec.first_record;
-- ======================================================
-- gb_smntc_mexico_costoproducir.cp_factores_prorrateo
-- poner fecha fin a los null
insert overwrite table gb_smntc_mexico_costoproducir.cp_factores_prorrateo partition(entidadlegal_id) 
     select
     tmp.cuentanatural_id,
     tmp.analisislocal_id,
     tmp.centrocostos_id,
     tmp.factor_id,
     tmp.dl,
     tmp.fecha_ini,
     case when tmp.fecha_fin is null then to_date(from_unixtime(unix_timestamp()) ) 
          else tmp.fecha_fin end as fecha_fin,
     tmp.storeday,
     tmp.entidadlegal_id
from gb_smntc_mexico_costoproducir.cp_factores_prorrateo tmp;

-- insertar los registros nuevos poniendo fecha_fin null
insert into table gb_smntc_mexico_costoproducir.cp_factores_prorrateo partition(entidadlegal_id) 
     select 
     tmp.cuentanatural_id,
     tmp.analisislocal_id,
     tmp.centrocostos_id,
     cf.factor_id as factor_id,
     tmp.directolinea as dl,
     date_sub(from_unixtime(unix_timestamp()) , 1) as fecha_ini,
     null as fecha_fin, 
     from_unixtime(unix_timestamp()) as storeday,
     tmp.entidadlegal_id
 from cp_flat_files.cp_d_fact_prorrateo tmp
 join cp_flat_files.cp_factores cf on lower(tmp.factor) = lower(cf.factor_ds);

-- compactar la tabla final cp_factores_prorrateo
insert overwrite table gb_smntc_mexico_costoproducir.cp_factores_prorrateo partition(entidadlegal_id) select tmp.* from gb_smntc_mexico_costoproducir.cp_factores_prorrateo tmp join (select entidadlegal_id, cuentanatural_id, analisislocal_id, centrocostos_id, factor_id, fecha_ini, max(storeday) as first_record from gb_smntc_mexico_costoproducir.cp_factores_prorrateo group by entidadlegal_id, cuentanatural_id, analisislocal_id, centrocostos_id, factor_id, fecha_ini) sec on tmp.entidadlegal_id = sec.entidadlegal_id and tmp.cuentanatural_id = sec.cuentanatural_id and tmp.analisislocal_id = sec.analisislocal_id and tmp.centrocostos_id = sec.centrocostos_id and tmp.factor_id = sec.factor_id and tmp.fecha_ini = sec.fecha_ini and tmp.storeday = sec.first_record;

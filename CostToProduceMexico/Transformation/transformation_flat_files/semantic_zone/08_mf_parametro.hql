-- ======================================================
-- mf_parametro
insert into table cp_dwh_mf.mf_parametro partition(entidadlegal_id) select tmp.tipo_parametro_id, tmp.fecha, tmp.parametro_desc, tmp.valor, from_unixtime(unix_timestamp()) as storeday, tmp.entidadlegal_id from cp_view.vdw_mf_cargasocial tmp where tmp.tipo_parametro_id in (3,8,12);
insert overwrite table  cp_dwh_mf.mf_parametro partition(entidadlegal_id) select tmp.* from  cp_dwh_mf.mf_parametro tmp join (select entidadlegal_id, tipo_parametro_id, fecha, max(storeday) as first_record from  cp_dwh_mf.mf_parametro group by entidadlegal_id, tipo_parametro_id, fecha) sec on tmp.entidadlegal_id = sec.entidadlegal_id and tmp.tipo_parametro_id = sec.tipo_parametro_id  and tmp.fecha = sec.fecha and tmp.storeday = sec.first_record;

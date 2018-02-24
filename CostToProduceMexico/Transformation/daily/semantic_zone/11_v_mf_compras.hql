-- ======================================================
--  cp_dwh_mf.mf_compras
insert into table cp_dwh_mf.mf_compras partition(entidadlegal_id) select tmp.* from cp_view.vdw_mf_compras tmp;

insert overwrite table cp_dwh_mf.mf_compras partition(entidadlegal_id) select tmp.* from cp_dwh_mf.mf_compras tmp join (select fecha, entidadlegal_id, mf_organizacion_id, mf_producto_id, max(storeday) as first_record from cp_dwh_mf.mf_compras group by fecha, entidadlegal_id, mf_organizacion_id, mf_producto_id) sec on tmp.fecha = sec.fecha and tmp.entidadlegal_id = sec.entidadlegal_id and tmp.mf_organizacion_id = sec.mf_organizacion_id and tmp.mf_producto_id = sec.mf_producto_id and tmp.storeday = sec.first_record;
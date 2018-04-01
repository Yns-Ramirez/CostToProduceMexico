-- ================== gb_mdl_mexico_manufactura.mf_unidad_medida ==================
-- Author / Autor             : Yenisei Ramirez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information, table gb_mdl_mexico_manufactura.mf_unidad_medida
-- Subject Area / Area Sujeto : Manufacture 

insert into table gb_mdl_mexico_manufactura.mf_unidad_medida select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_mf_unidad_medida tmp;
insert overwrite table  gb_mdl_mexico_manufactura.mf_unidad_medida select tmp.* from  gb_mdl_mexico_manufactura.mf_unidad_medida tmp join (select mf_unidadmedida_id, max(storeday) as first_record from  gb_mdl_mexico_manufactura.mf_unidad_medida group by mf_unidadmedida_id) sec on tmp.mf_unidadmedida_id = sec.mf_unidadmedida_id and tmp.storeday = sec.first_record;

-- ================== gb_mdl_mexico_manufactura.mf_transferencias ==================
-- Author / Autor             : Yenisei Ramirez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information, table gb_mdl_mexico_manufactura.mf_transferencias
-- Subject Area / Area Sujeto : Manufacture 

insert into table gb_mdl_mexico_manufactura.mf_transferencias partition(entidadlegal_id) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_mf_transferencias tmp;
insert overwrite table gb_mdl_mexico_manufactura.mf_transferencias partition(entidadlegal_id) select tmp.* from  gb_mdl_mexico_manufactura.mf_transferencias tmp join (select fecha, entidadlegal_id, mf_organizacion_id, recibe_mf_organizacion_id, mf_producto_id, max(storeday) as first_record from  gb_mdl_mexico_manufactura.mf_transferencias group by fecha, entidadlegal_id, mf_organizacion_id, recibe_mf_organizacion_id, mf_producto_id) sec on tmp.fecha = sec.fecha and tmp.entidadlegal_id = sec.entidadlegal_id and tmp.mf_organizacion_id = sec.mf_organizacion_id and tmp.recibe_mf_organizacion_id = sec.recibe_mf_organizacion_id and tmp.mf_producto_id = sec.mf_producto_id and tmp.storeday = sec.first_record;
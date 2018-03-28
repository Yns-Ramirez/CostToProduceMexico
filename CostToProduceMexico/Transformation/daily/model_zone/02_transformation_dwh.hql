-- mtl_transaction_accounts en dwh.
truncate table gb_mdl_mexico_costoproducir.mtl_transaction_accounts;
insert overwrite table gb_mdl_mexico_costoproducir.mtl_transaction_accounts partition (storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_mtl_transaction_accounts tmp join (select  transaction_id ,  max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_mtl_transaction_accounts group by transaction_id) sec on tmp.transaction_id = sec.transaction_id and tmp.storeday = sec.first_record;
-- mtl_transaccion_materiales en dwh.
truncate table gb_mdl_mexico_costoproducir.mtl_transaccion_materiales;
insert overwrite table gb_mdl_mexico_costoproducir.mtl_transaccion_materiales partition (storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_mtl_transaccion_materiales tmp join (select  transaction_id ,  max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_mtl_transaccion_materiales group by transaction_id) sec on tmp.transaction_id = sec.transaction_id and tmp.storeday = sec.first_record;
-- mtl_categorias en dwh.
insert overwrite table gb_mdl_mexico_costoproducir.mtl_categorias select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_mtl_categorias tmp;
-- gb_mdl_mexico_costoproducir.mtl_flexfields_materiales
insert overwrite table gb_mdl_mexico_costoproducir.mtl_flexfields_materiales select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_mtl_flexfields_materiales tmp join (select  inventory_item_id, organization_id ,  max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_mtl_flexfields_materiales group by inventory_item_id, organization_id) sec on tmp.inventory_item_id = sec.inventory_item_id and tmp.organization_id = sec.organization_id and tmp.storeday = sec.first_record;
-- vdw_mtl_catalogo_materiales en dwh.
truncate table gb_mdl_mexico_costoproducir.mtl_catalogo_materiales;
insert overwrite table gb_mdl_mexico_costoproducir.mtl_catalogo_materiales partition (storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_mtl_catalogo_materiales tmp join (select  inventory_item_id ,organization_id ,  max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_mtl_catalogo_materiales group by inventory_item_id ,organization_id) sec on tmp.inventory_item_id = sec.inventory_item_id and tmp.organization_id = sec.organization_id and tmp.storeday = sec.first_record;
-- mtl_categoria_materiales en dwh.
truncate table gb_mdl_mexico_costoproducir.mtl_categoria_materiales;
insert overwrite table gb_mdl_mexico_costoproducir.mtl_categoria_materiales partition (storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_mtl_categoria_materiales tmp join (select  inventory_item_id ,organization_id ,category_set_id ,category_id ,last_update_date ,last_updated_by ,creation_date ,created_by ,  max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_mtl_categoria_materiales group by inventory_item_id ,organization_id ,category_set_id ,category_id ,last_update_date ,last_updated_by ,creation_date ,created_by) sec on tmp.inventory_item_id = sec.inventory_item_id and tmp.organization_id = sec.organization_id and tmp.category_set_id = sec.category_set_id and tmp.category_id = sec.category_id and tmp.last_update_date = sec.last_update_date and tmp.last_updated_by = sec.last_updated_by and tmp.creation_date = sec.creation_date and tmp.created_by = sec.created_by and tmp.storeday = sec.first_record;
-- mtl_unidad_medida en dwh.
truncate table gb_mdl_mexico_costoproducir.mtl_unidad_medida;
insert overwrite table gb_mdl_mexico_costoproducir.mtl_unidad_medida partition (storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_mtl_unidad_medida tmp join (select  unit_of_measure ,  max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_mtl_unidad_medida group by unit_of_measure) sec on tmp.unit_of_measure = sec.unit_of_measure and tmp.storeday = sec.first_record;
-- mtl_conversiones_udm en dwh.
insert overwrite table gb_mdl_mexico_costoproducir.mtl_conversiones_udm select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_mtl_conversiones_udm tmp join (select  unit_of_measure ,  max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_mtl_conversiones_udm group by unit_of_measure) sec on tmp.unit_of_measure = sec.unit_of_measure and tmp.storeday = sec.first_record;
-- hr_location en dwh.
truncate table gb_mdl_mexico_costoproducir.hr_location;
insert overwrite table gb_mdl_mexico_costoproducir.hr_location partition (storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_hr_location tmp join (select  location_id ,  max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_hr_location group by location_id) sec on tmp.location_id = sec.location_id  and tmp.storeday = sec.first_record;
-- bom_resources en dwh.
truncate table gb_mdl_mexico_costoproducir.bom_resources;
insert overwrite table gb_mdl_mexico_costoproducir.bom_resources partition (storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_bom_resources tmp join (select  resource_id,resource_code ,organization_id,cost_code_type ,  max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_bom_resources group by resource_id,resource_code ,organization_id,cost_code_type) sec on tmp.resource_id = sec.resource_id and tmp.resource_code = sec.resource_code and tmp.organization_id = sec.organization_id and tmp.cost_code_type = sec.cost_code_type  and tmp.storeday = sec.first_record;
-- hr_organizacion en dwh.
truncate table gb_mdl_mexico_costoproducir.hr_organizacion;
 insert overwrite table gb_mdl_mexico_costoproducir.hr_organizacion partition (storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_hr_organizacion tmp join (select  organization_id ,  max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_hr_organizacion group by organization_id) sec on tmp.organization_id = sec.organization_id  and tmp.storeday = sec.first_record;
-- gb_mdl_mexico_costoproducir.mtl_referencia_cruzada_mat en dwh.
insert into table gb_mdl_mexico_costoproducir.mtl_referencia_cruzada_mat select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_mtl_referencia_cruzada_mat tmp;
insert overwrite table gb_mdl_mexico_costoproducir.mtl_referencia_cruzada_mat select tmp.* from gb_mdl_mexico_costoproducir.mtl_referencia_cruzada_mat tmp join (select inventory_item_id, organization_id, cross_reference_type, cross_reference, last_update_date, last_update_date_h, max(storeday) as first_record from gb_mdl_mexico_costoproducir.mtl_referencia_cruzada_mat group by inventory_item_id, organization_id, cross_reference_type, cross_reference, last_update_date, last_update_date_h) sec on coalesce(tmp.inventory_item_id,0) = coalesce(sec.inventory_item_id,0) and coalesce(tmp.organization_id,0) = coalesce(sec.organization_id,0) and coalesce(tmp.cross_reference_type,0) = coalesce(sec.cross_reference_type,0) and coalesce(tmp.cross_reference,0) = coalesce(sec.cross_reference,0) and coalesce(tmp.last_update_date,0) = coalesce(sec.last_update_date,0) and coalesce(tmp.last_update_date_h,0) = coalesce(sec.last_update_date_h,0) and coalesce(tmp.storeday,0) = coalesce(sec.first_record,0);
-- gb_mdl_mexico_costoproducir.wip_lineas
truncate table gb_mdl_mexico_costoproducir.wip_lineas;
insert overwrite table gb_mdl_mexico_costoproducir.wip_lineas partition (storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_wip_lineas tmp join (select  line_id, line_code, organization_id, max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_wip_lineas group by line_id, line_code, organization_id) sec on tmp.line_id = sec.line_id and tmp.line_code = sec.line_code and tmp.organization_id = sec.organization_id and tmp.storeday = sec.first_record;
-- wip_flow_schedules en dwh.
truncate table gb_mdl_mexico_costoproducir.wip_flow_schedules;
insert overwrite table gb_mdl_mexico_costoproducir.wip_flow_schedules partition (storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_wip_flow_schedules tmp join (select  wip_entity_id, schedule_number, organization_id, max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_wip_flow_schedules group by wip_entity_id, schedule_number, organization_id) sec on tmp.wip_entity_id = sec.wip_entity_id and tmp.schedule_number = sec.schedule_number and tmp.organization_id = sec.organization_id and tmp.storeday = sec.first_record;
-- gb_mdl_mexico_costoproducir.wip_repetitive_items_hist
truncate table gb_mdl_mexico_costoproducir.wip_repetitive_items_hist;
insert into table gb_mdl_mexico_costoproducir.wip_repetitive_items_hist partition(storeday) select date_sub(add_months(concat(vfe.fechaini),1), 1) as fecha, wri.wip_entity_id , wri.line_id , wri.organization_id , wri.last_update_date , wri.last_update_date_h , wri.last_updated_by , wri.creation_date , wri.creation_date_h , wri.created_by , wri.last_update_login , wri.request_id , wri.program_application_id , wri.program_id , wri.program_update_date , wri.program_update_date_h , wri.primary_item_id , wri.alternate_bom_designator , wri.alternate_routing_designator , wri.class_code , wri.wip_supply_type , wri.completion_subinventory , wri.completion_locator_id , wri.load_distribution_priority , wri.primary_line_flag , wri.production_line_rate , wri.overcompletion_tolerance_type , wri.overcompletion_tolerance_value , wri.attribute1 , wri.attribute6 , wri.storeday from erp_mexico_sz.wip_repetitive_items wri, gb_mdl_mexico_costoproducir_views.v_fechas_extraccion_hist vfe; --where wri.storeday between to_date(date_add(vfe.fechaini,1)) and vfe.fechafin;
insert overwrite table gb_mdl_mexico_costoproducir.wip_repetitive_items_hist partition(storeday) select tmp.* from gb_mdl_mexico_costoproducir.wip_repetitive_items_hist tmp join (select fecha_actualizacion, wip_entity_id, line_id, organization_id, primary_item_id, max(storeday) as first_record from gb_mdl_mexico_costoproducir.wip_repetitive_items_hist group by fecha_actualizacion, wip_entity_id, line_id, organization_id, primary_item_id) sec on tmp.fecha_actualizacion = sec.fecha_actualizacion and tmp.wip_entity_id = sec.wip_entity_id and tmp.line_id = sec.line_id and tmp.organization_id = sec.organization_id and tmp.primary_item_id = sec.primary_item_id and tmp.storeday = sec.first_record;
-- WIP_repetitive_items en dwh.
insert into table gb_mdl_mexico_costoproducir.wip_repetitive_items select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_wip_repetitive_items tmp;
insert overwrite table gb_mdl_mexico_costoproducir.wip_repetitive_items select tmp.* from gb_mdl_mexico_costoproducir.wip_repetitive_items tmp join (select  wip_entity_id, line_id, organization_id, primary_item_id, max(storeday) as first_record from gb_mdl_mexico_costoproducir.wip_repetitive_items group by wip_entity_id, line_id, organization_id, primary_item_id) sec on tmp.wip_entity_id = sec.wip_entity_id and tmp.line_id = sec.line_id and tmp.organization_id = sec.organization_id and tmp.primary_item_id = sec.primary_item_id and tmp.storeday = sec.first_record;
-- mtl_onhand_diario en dwh.
-- correr al inicio de mes por que la vista apunta a un dia anterior
insert overwrite table gb_mdl_mexico_costoproducir.mtl_onhand_diario partition (fecha) select 
    tmp.organization_id,
    tmp.inventory_item_id,
    tmp.subinventory_code,
    tmp.max_last_update_date,
    tmp.prim_transaction_quantity,
    tmp.storeday,
    date_sub(add_months(concat(vfe.fechaini),1), 1) as fecha
    from gb_mdl_mexico_costoproducir_views.vdw_mtl_onhand_diario tmp, gb_mdl_mexico_costoproducir_views.v_fechas_extraccion_hist vfe join (select  fecha, organization_id, inventory_item_id, subinventory_code ,  max(storeday) as first_record from gb_mdl_mexico_costoproducir_views.vdw_mtl_onhand_diario group by fecha, organization_id, inventory_item_id, subinventory_code) sec on tmp.fecha = sec.fecha and tmp.organization_id = sec.organization_id and tmp.inventory_item_id = sec.inventory_item_id and tmp.subinventory_code = sec.subinventory_code  and tmp.storeday = sec.first_record
    where tmp.storeday between to_date(date_add(vfe.fechaini,1)) and vfe.fechafin;
-- ======================================================
--  gb_mdl_mexico_costoproducir.a_centro_costos
truncate table gb_mdl_mexico_costoproducir.a_centro_costos;
insert into table gb_mdl_mexico_costoproducir.a_centro_costos select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_centro_costos tmp;
insert overwrite table  gb_mdl_mexico_costoproducir.a_centro_costos select tmp.* from  gb_mdl_mexico_costoproducir.a_centro_costos tmp join (select centrocostos_id, max(storeday) as first_record from  gb_mdl_mexico_costoproducir.a_centro_costos group by centrocostos_id) sec on tmp.centrocostos_id = sec.centrocostos_id and tmp.storeday = sec.first_record;
-- ======================================================
-- a_reporte_financiero_dtl
insert overwrite table gb_mdl_mexico_costoproducir.a_reporte_financiero_dtl select * from gb_mdl_mexico_costoproducir_views.vdw_a_reporte_financiero_dtl;
-- a_reporte_financiero
insert overwrite table gb_mdl_mexico_costoproducir.a_reporte_financiero select * from gb_mdl_mexico_costoproducir_views.vdw_a_reporte_financiero;

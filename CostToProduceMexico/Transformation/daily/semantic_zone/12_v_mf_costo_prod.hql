-- gb_mdl_mexico_costoproducir.CST_ITEM_COST_DETAILS_HIST compactation
-- esta la procesa paco
-- insert into gb_mdl_mexico_costoproducir.cst_item_cost_details_hist partition(storeday) select current_date ,inventory_item_id ,organization_id ,cost_type_id ,last_update_date ,last_updated_by ,creation_date ,created_by ,last_update_login ,operation_sequence_id ,operation_seq_num ,department_id ,level_type ,activity_id ,resource_seq_num ,resource_id ,resource_rate ,item_units ,activity_units ,case when usage_rate_or_amount like '%e1' then cast(substr(usage_rate_or_amount,1,5) as string) when usage_rate_or_amount like '%e2%' then cast(substr(usage_rate_or_amount,1,5) as string) * 10 when usage_rate_or_amount like '%e3%' then cast(substr(usage_rate_or_amount,1,5) as string) * 100 when usage_rate_or_amount like '%e4%' then cast(substr(usage_rate_or_amount,1,5) as string) * 1000 when usage_rate_or_amount like '%e5%' then cast(substr(usage_rate_or_amount,1,5) as string) * 10000 when usage_rate_or_amount like '%e6%' then cast(substr(usage_rate_or_amount,1,5) as string) * 100000 when usage_rate_or_amount like '%e7%' then cast(substr(usage_rate_or_amount,1,5) as string) * 1000000 when usage_rate_or_amount like '%e8%' then cast(substr(usage_rate_or_amount,1,5) as string) * 10000000 when usage_rate_or_amount like '%e9%' then cast(substr(usage_rate_or_amount,1,5) as string) * 100000000 when usage_rate_or_amount like '%e-1' then substr(trim(cast(usage_rate_or_amount as string)),1,5) * 0.1 when usage_rate_or_amount like '%e-2%' then cast(substr(usage_rate_or_amount,1,5) as string)  * 0.01 when usage_rate_or_amount like '%e-3%' then cast(substr(usage_rate_or_amount,1,5) as string)  * 0.001 when usage_rate_or_amount like '%e-4%' then cast(substr(usage_rate_or_amount,1,5) as string)  * 0.0001 when substr(usage_rate_or_amount,2,3)  like '%e-5%' then cast(substr(usage_rate_or_amount,1,1) as string)  * 0.00001 when substr(usage_rate_or_amount,2,3)  like '%e-6%' then cast(substr(usage_rate_or_amount,1,1) as string)  * 0.000001 when usage_rate_or_amount like '%e-5%' then cast(substr(usage_rate_or_amount,1,3) as string)  * 0.00001 when usage_rate_or_amount like '%e-6%' then cast(substr(usage_rate_or_amount,1,3) as string)  * 0.000001 when substr(usage_rate_or_amount,2,3)  like '%e-7%' then cast(substr(usage_rate_or_amount,1,1) as string)  * 0.0000001 when substr(usage_rate_or_amount,2,3)  like '%e-8%' then cast(substr(usage_rate_or_amount,1,1) as string)  * 0.00000001 when usage_rate_or_amount like '%e-7%' then cast(substr(usage_rate_or_amount,1,3) as string)  * 0.0000001 when usage_rate_or_amount like '%e-8%' then cast(substr(usage_rate_or_amount,1,3) as string)  * 0.00000001 when usage_rate_or_amount like '%e-9%' then cast(substr(usage_rate_or_amount,1,5) as string)  * 0.000000001 when usage_rate_or_amount like '%e-1%' then 0 when usage_rate_or_amount like '%e-0%' then 0 else cast(usage_rate_or_amount as string ) end ,basis_type ,basis_resource_id ,case when basis_factor like '%e1%' then cast(substr(basis_factor,1,5) as string ) when basis_factor like '%e2%' then cast(substr(basis_factor,1,5) as string) * 10 when basis_factor like '%e-1%' then cast(substr(basis_factor,1,5) as string)  * 0.1 when basis_factor like '%e-2%' then cast(substr(basis_factor,1,5) as string)  * 0.01 when substr(basis_factor,2,3)  like '%e-5%' then cast(substr(basis_factor,1,1) as string)  * 0.00001 when substr(basis_factor,2,3)  like '%e-6%' then cast(substr(basis_factor,1,1) as string)  * 0.000001 when basis_factor like '%e-5%' then cast(substr(basis_factor,1,3) as string)  * 0.00001 when basis_factor like '%e-6%' then cast(substr(basis_factor,1,3) as string)  * 0.000001 when basis_factor like '%e-%' then 0 else cast(basis_factor as string ) end ,net_yield_or_shrinkage_factor ,case when item_cost like '%e1%' then cast(substr(item_cost,1,5) as string ) when item_cost like '%e2%' then cast(substr(item_cost,1,5) as string) * 10 when item_cost like '%e-1%' then cast(substr(item_cost,1,5) as string)  * 0.1 when item_cost like '%e-2%' then cast(substr(item_cost,1,5) as string)  * 0.01 when substr(item_cost,2,3)  like '%e-5%' then cast(substr(item_cost,1,1) as string)  * 0.00001 when substr(item_cost,2,3)  like '%e-6%' then cast(substr(item_cost,1,1) as string)  * 0.000001 when item_cost like '%e-5%' then cast(substr(item_cost,1,3) as string)  * 0.00001 when item_cost like '%e-6%' then cast(substr(item_cost,1,3) as string)  * 0.000001 when item_cost like '%e-%' then 0 else cast(item_cost as string ) end ,cost_element_id ,rollup_source_type ,activity_context ,request_id ,program_application_id ,program_id ,program_update_date ,attribute_category ,attribute1 ,attribute2 ,attribute3 ,attribute4 ,attribute5 ,attribute6 ,attribute7 ,attribute8 ,attribute9 ,attribute10 ,attribute11 ,attribute12 ,attribute13 ,attribute14 ,attribute15 ,yielded_cost ,source_organization_id ,vendor_id ,allocation_percent ,vendor_site_id ,ship_method ,storeday from erp_mexico_sz.cst_item_cost_details;
--insert overwrite table gb_mdl_mexico_costoproducir.cst_item_cost_details_hist partition(storeday) select fecha_actualizacion, inventory_item_id, organization_id, cost_type_id, last_update_date, last_updated_by, creation_date, created_by, last_update_login, operation_sequence_id, operation_seq_num, department_id, level_type, activity_id, resource_seq_num, resource_id, resource_rate, item_units, activity_units, usage_rate_or_amount, basis_type, basis_resource_id, basis_factor, net_yield_or_shrinkage_factor, item_cost, cost_element_id, rollup_source_type, activity_context, request_id, program_application_id, program_id, program_update_date, attribute_category, attribute1, attribute2, attribute3, attribute4, attribute5, attribute6, attribute7, attribute8, attribute9, attribute10, attribute11, attribute12, attribute13, attribute14, attribute15, yielded_cost, source_organization_id, vendor_id, allocation_percent, vendor_site_id, ship_method, max(storeday) from gb_mdl_mexico_costoproducir.cst_item_cost_details_hist group by fecha_actualizacion, inventory_item_id, organization_id, cost_type_id, last_update_date, last_updated_by, creation_date, created_by, last_update_login, operation_sequence_id, operation_seq_num, department_id, level_type, activity_id, resource_seq_num, resource_id, resource_rate, item_units, activity_units, usage_rate_or_amount, basis_type, basis_resource_id, basis_factor, net_yield_or_shrinkage_factor, item_cost, cost_element_id, rollup_source_type, activity_context, request_id, program_application_id, program_id, program_update_date, attribute_category, attribute1, attribute2, attribute3, attribute4, attribute5, attribute6, attribute7, attribute8, attribute9, attribute10, attribute11, attribute12, attribute13, attribute14, attribute15, yielded_cost, source_organization_id, vendor_id, allocation_percent, vendor_site_id, ship_method;


-- ======================================================
--  gb_mdl_mexico_manufactura.mf_costo_prod
-- falta contemplar 2 union para tablas que no se tienen aun
insert overwrite table gb_mdl_mexico_manufactura.mf_costo_prod partition(entidadlegal_id) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_mf_costo_prod tmp;

insert overwrite table gb_mdl_mexico_manufactura.mf_costo_prod partition(entidadlegal_id) select cp.mf_organizacion_id , cp.planta_id , cp.mf_producto_id , cp.periodo , coalesce(tm.tipomoneda_id,-1) , cp.tipo_costo_id , sum(cp.costo) , cp.fecha_alta , cp.fecha_mod , cp.storeday , cp.entidadlegal_id from gb_mdl_mexico_manufactura.mf_costo_prod cp left outer join ( select o.entidadlegal_id , coalesce(m.tipomoneda_id,-1) as tipomoneda_id , o.organizacion_id , coalesce(m.pais_id,-1) as pais_id from gb_mdl_mexico_costoproducir.o_entidadlegal_organizacion o left outer join gb_mdl_mexico_costoproducir.o_entidad_legal el on o.entidadlegal_id = el.entidadlegal_id left outer join gb_mdl_mexico_costoproducir.v_tipo_moneda m on m.pais_id = o.pais_id left outer join gb_mdl_mexico_costoproducir.g_pais g on o.pais_id = g.pais_id left outer join gb_mdl_mexico_costoproducir.e_organizacion e on e.organizacion_id = o.organizacion_id ) tm on cp.entidadlegal_id = tm.entidadlegal_id group by cp.mf_organizacion_id , cp.planta_id , cp.mf_producto_id , cp.periodo , coalesce(tm.tipomoneda_id,-1) , cp.tipo_costo_id , cp.fecha_alta , cp.fecha_mod , cp.storeday , cp.entidadlegal_id having sum(cp.costo) <> 0;

insert overwrite table  gb_mdl_mexico_manufactura.mf_costo_prod partition(entidadlegal_id) select tmp.* from  gb_mdl_mexico_manufactura.mf_costo_prod tmp join (select mf_organizacion_id, mf_producto_id, tipo_costo_id, periodo, max(storeday) as first_record from  gb_mdl_mexico_manufactura.mf_costo_prod group by mf_organizacion_id, mf_producto_id, tipo_costo_id, periodo) sec on tmp.mf_organizacion_id = sec.mf_organizacion_id and  tmp.mf_producto_id = sec.mf_producto_id and  tmp.tipo_costo_id = sec.tipo_costo_id and  tmp.periodo = sec.periodo and tmp.storeday = sec.first_record;

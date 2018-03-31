-- ================== GB MODEL MEXICO ERP TRANSFORMATIONS ==================
-- Author / Autor             : Yenisei Ramirez
-- Date / Fecha               : October 2017
-- Project / Proyecto         : Transformations ERP
-- Parameters / Parametros    : 

INSERT overwrite TABLE gb_mdl_mexico_erp.wip_repetitive_items_hist partition(fecha_actualizacion)
SELECT 
       wri.wip_entity_id ,
       wri.line_id ,
       wri.organization_id ,
       wri.last_update_date ,
       wri.last_update_date_h ,
       wri.last_updated_by ,
       wri.creation_date ,
       wri.creation_date_h ,
       wri.created_by ,
       wri.last_update_login ,
       wri.request_id ,
       wri.program_application_id ,
       wri.program_id ,
       wri.program_update_date ,
       wri.program_update_date_h ,
       wri.primary_item_id ,
       wri.alternate_bom_designator ,
       wri.alternate_routing_designator ,
       wri.class_code ,
       wri.wip_supply_type ,
       wri.completion_subinventory ,
       wri.completion_locator_id ,
       wri.load_distribution_priority ,
       wri.primary_line_flag ,
       wri.production_line_rate ,
       wri.overcompletion_tolerance_type ,
       wri.overcompletion_tolerance_value ,
       wri.attribute1 ,
       wri.attribute6 ,
       wri.storeday,
       to_date(date_sub(add_months(concat(substr(exec.new_date,1,7),'-01'),1), 1)) as fecha_actualizacion
FROM erp_mexico_sz.wip_repetitive_items wri,
     (select 
    exec.exec_date as exec_date
    ,case 
        when day(exec.exec_date) = 1 then to_date(date_sub(exec.exec_date,1))
        else exec.exec_date
    end as new_date
    from(
        select to_date(from_unixtime(unix_timestamp())) as exec_date
    )exec
)exec;
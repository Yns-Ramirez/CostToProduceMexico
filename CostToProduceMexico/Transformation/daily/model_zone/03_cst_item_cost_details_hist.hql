-- ================== gb_mdl_mexico_erp.cst_item_cost_details_hist monthly ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information monthly, table gb_mdl_mexico_erp.cst_item_cost_details_hist
-- Subject Area / Area Sujeto :

-- delete data with the field FECHA_ACTUALIZACION related with the month is closing

-- 
-- INSERT OVERWRITE table gb_mdl_mexico_erp.cst_item_cost_details_hist partition(fecha_actualizacion)
-- SELECT tmp.* FROM gb_mdl_mexico_erp.cst_item_cost_details_hist tmp 
-- left outer join (SELECT a.FECHA_ACTUALIZACION,a.INVENTORY_ITEM_ID,a.ORGANIZATION_ID,a.COST_TYPE_ID 
-- FROM gb_mdl_mexico_erp.cst_item_cost_details_hist a
-- WHERE a.FECHA_ACTUALIZACION between (add_months(FROM_UNIXTIME(UNIX_TIMESTAMP()),1) -1) and FROM_UNIXTIME(UNIX_TIMESTAMP())-1) sec 
-- on tmp.FECHA_ACTUALIZACION=sec.FECHA_ACTUALIZACION
-- and tmp.INVENTORY_ITEM_ID=sec.INVENTORY_ITEM_ID
-- and tmp.ORGANIZATION_ID=sec.ORGANIZATION_ID
-- and tmp.COST_TYPE_ID=sec.COST_TYPE_ID
-- where sec.FECHA_ACTUALIZACION is null
-- and sec.INVENTORY_ITEM_ID is null
-- and sec.ORGANIZATION_ID is null
-- and sec.COST_TYPE_ID is null;

-- insert data with the field FECHA_ACTUALIZACION with the last date of the month is closing
insert overwrite table gb_mdl_mexico_erp.CST_ITEM_COST_DETAILS_HIST partition(fecha_actualizacion)
SELECT
  cd.INVENTORY_ITEM_ID
  ,cd.ORGANIZATION_ID
  ,cd.COST_TYPE_ID
  ,cd.LAST_UPDATE_DATE
  ,cd.LAST_UPDATED_BY
  ,cd.CREATION_DATE
  ,cd.CREATED_BY
  ,cd.LAST_UPDATE_LOGIN     
  ,cd.OPERATION_SEQUENCE_ID 
  ,cd.OPERATION_SEQ_NUM     
  ,cd.DEPARTMENT_ID
  ,cd.LEVEL_TYPE
  ,cd.ACTIVITY_ID
  ,cd.RESOURCE_SEQ_NUM
  ,cd.RESOURCE_ID
  ,cd.RESOURCE_RATE         
  ,cd.ITEM_UNITS
  ,cd.ACTIVITY_UNITS
    ,CASE
     when cd.USAGE_RATE_OR_AMOUNT like '%e1' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,5) as STRING )     
     when cd.USAGE_RATE_OR_AMOUNT like '%e2%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,5) as STRING) * 10     
     when cd.USAGE_RATE_OR_AMOUNT like '%e3%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,5) as STRING) * 100    
     when cd.USAGE_RATE_OR_AMOUNT like '%e4%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,5) as STRING) * 1000       
     when cd.USAGE_RATE_OR_AMOUNT like '%e5%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,5) as STRING) * 10000 
     when cd.USAGE_RATE_OR_AMOUNT like '%e6%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,5) as STRING) * 100000
     when cd.USAGE_RATE_OR_AMOUNT like '%e7%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,5) as STRING) * 1000000
     when cd.USAGE_RATE_OR_AMOUNT like '%e8%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,5) as STRING) * 10000000
     when cd.USAGE_RATE_OR_AMOUNT like '%e9%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,5) as STRING) * 100000000
     when cd.USAGE_RATE_OR_AMOUNT like '%e-1' then cast(substr(trim(cast(cd.USAGE_RATE_OR_AMOUNT as string)),1,5) as STRING) * 0.1  
     when cd.USAGE_RATE_OR_AMOUNT like '%e-2%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,5) as STRING) * 0.01 
     when cd.USAGE_RATE_OR_AMOUNT like '%e-3%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,5) as STRING) * 0.001  
     when cd.USAGE_RATE_OR_AMOUNT like '%e-4%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,5) as STRING) * 0.0001 
   when substr(cd.USAGE_RATE_OR_AMOUNT,2,3)  like '%e-5%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,1) as STRING) * 0.00001 
   when substr(cd.USAGE_RATE_OR_AMOUNT,2,3)  like '%e-6%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,1) as STRING) * 0.000001
     when cd.USAGE_RATE_OR_AMOUNT like '%e-5%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,3) as STRING) * 0.00001             
     when cd.USAGE_RATE_OR_AMOUNT like '%e-6%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,3) as STRING) * 0.000001    
   when substr(cd.USAGE_RATE_OR_AMOUNT,2,3)  like '%e-7%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,1) as STRING) * 0.0000001 
   when substr(cd.USAGE_RATE_OR_AMOUNT,2,3)  like '%e-8%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,1) as STRING) * 0.00000001
     when cd.USAGE_RATE_OR_AMOUNT like '%e-7%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,3) as STRING) * 0.0000001
     when cd.USAGE_RATE_OR_AMOUNT like '%e-8%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,3) as STRING) * 0.00000001
     when cd.USAGE_RATE_OR_AMOUNT like '%e-9%' then cast(substr(cd.USAGE_RATE_OR_AMOUNT,1,5) as STRING) * 0.000000001
  when cd.USAGE_RATE_OR_AMOUNT like '%e-1%' then 0
    when cd.USAGE_RATE_OR_AMOUNT like '%e-0%' then 0
     else cast(cd.USAGE_RATE_OR_AMOUNT as STRING)           
    end       
  ,cd.BASIS_TYPE            
  ,cd.BASIS_RESOURCE_ID          
  ,case 
     when cd.BASIS_FACTOR like '%e1%' then cast(substr(cd.BASIS_FACTOR,1,5) as STRING)         
     when cd.BASIS_FACTOR like '%e2%' then cast(substr(cd.BASIS_FACTOR,1,5) as STRING) * 10     
     when cd.BASIS_FACTOR like '%e-1%' then cast(substr(cd.BASIS_FACTOR,1,5) as STRING)  * 0.1  
     when cd.BASIS_FACTOR like '%e-2%' then cast(substr(cd.BASIS_FACTOR,1,5) as STRING)  * 0.01 
     when substr(cd.BASIS_FACTOR,2,3) like '%e-5%' then cast(substr(cd.BASIS_FACTOR,1,1) as STRING)  * 0.00001 
     when substr(cd.BASIS_FACTOR,2,3) like '%e-6%' then cast(substr(cd.BASIS_FACTOR,1,1) as STRING)  * 0.000001
     when cd.BASIS_FACTOR like '%e-5%' then cast(substr(cd.BASIS_FACTOR,1,3) as STRING)  * 0.00001              
     when cd.BASIS_FACTOR like '%e-6%' then cast(substr(cd.BASIS_FACTOR,1,3) as STRING)  * 0.000001             
    when cd.BASIS_FACTOR like '%e-%' then 0 
     else cast(cd.BASIS_FACTOR as STRING)   
    end  
  ,cd.NET_YIELD_OR_SHRINKAGE_FACTOR         
  ,case  
     when cd.item_cost like '%e1%' then cast(substr(cd.item_cost,1,5) as STRING )               
     when cd.item_cost like '%e2%' then cast(substr(cd.item_cost,1,5) as STRING) * 10           
     when cd.item_cost like '%e-1%' then cast(substr(cd.item_cost,1,5) as STRING)  * 0.1        
     when cd.item_cost like '%e-2%' then cast(substr(cd.item_cost,1,5) as STRING)  * 0.01       
     when substr(cd.item_cost,2,3)  like '%e-5%' then cast(substr(cd.item_cost,1,1) as STRING)  * 0.00001       
     when substr(cd.item_cost,2,3)  like '%e-6%' then cast(substr(cd.item_cost,1,1) as STRING)  * 0.000001      
     when cd.item_cost like '%e-5%' then cast(substr(cd.item_cost,1,3) as STRING)  * 0.00001    
     when cd.item_cost like '%e-6%' then cast(substr(cd.item_cost,1,3) as STRING)  * 0.000001   
    when cd.item_cost like '%e-%' then 0    
     else cast(cd.item_cost as STRING )      
    end  
  ,cd.COST_ELEMENT_ID       
  ,cd.ROLLUP_SOURCE_TYPE    
  ,cd.ACTIVITY_CONTEXT      
  ,cd.REQUEST_ID            
  ,cd.PROGRAM_APPLICATION_ID
  ,cd.PROGRAM_ID            
  ,cd.PROGRAM_UPDATE_DATE   
  ,cd.ATTRIBUTE_CATEGORY    
  ,cd.ATTRIBUTE1            
  ,cd.ATTRIBUTE2            
  ,cd.ATTRIBUTE3            
  ,cd.ATTRIBUTE4            
  ,cd.ATTRIBUTE5            
  ,cd.ATTRIBUTE6            
  ,cd.ATTRIBUTE7            
  ,cd.ATTRIBUTE8            
  ,cd.ATTRIBUTE9            
  ,cd.ATTRIBUTE10           
  ,cd.ATTRIBUTE11           
  ,cd.ATTRIBUTE12           
  ,cd.ATTRIBUTE13           
  ,cd.ATTRIBUTE14           
  ,cd.ATTRIBUTE15           
  ,cd.YIELDED_COST          
  ,cd.SOURCE_ORGANIZATION_ID
  ,cd.VENDOR_ID             
  ,cd.ALLOCATION_PERCENT    
  ,cd.VENDOR_SITE_ID        
  ,cd.SHIP_METHOD
  ,cd.storeday
  ,to_date(date_sub(add_months(concat(substr(exec.new_date,1,7),'-01'),1), 1)) as fecha_actualizacion
FROM erp_mexico_sz.CST_ITEM_COST_DETAILS cd, 
(select 
    exec.exec_date as exec_date
    ,case 
        when day(exec.exec_date) = 1 then to_date(date_sub(exec.exec_date,1))
        else exec.exec_date
    end as new_date
    from(
        select to_date(from_unixtime(unix_timestamp())) as exec_date
    )exec
)exec
WHERE cd.USAGE_RATE_OR_AMOUNT NOT LIKE '-2e-5';

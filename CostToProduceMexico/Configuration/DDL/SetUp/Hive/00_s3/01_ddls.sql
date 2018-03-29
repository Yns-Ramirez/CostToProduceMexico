create database if not exists gb_mdl_mexico_erp LOCATION '${hiveconf:paths3}/gb_mdl_mexico_erp.db';


CREATE TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir.cst_item_cost_details_hist(
  inventory_item_id decimal(18,0), 
  organization_id decimal(18,0), 
  cost_type_id int, 
  last_update_date string, 
  last_updated_by decimal(18,0), 
  creation_date string, 
  created_by decimal(18,0), 
  last_update_login decimal(18,0), 
  operation_sequence_id decimal(18,0), 
  operation_seq_num decimal(18,0), 
  department_id decimal(18,0), 
  level_type int, 
  activity_id decimal(18,0), 
  resource_seq_num decimal(18,0), 
  resource_id decimal(18,0), 
  resource_rate decimal(18,6), 
  item_units decimal(18,0), 
  activity_units decimal(18,0), 
  usage_rate_or_amount decimal(18,6), 
  basis_type int, 
  basis_resource_id decimal(18,0), 
  basis_factor decimal(18,6), 
  net_yield_or_shrinkage_factor decimal(18,6), 
  item_cost decimal(18,10), 
  cost_element_id int, 
  rollup_source_type int, 
  activity_context string, 
  request_id decimal(18,0), 
  program_application_id decimal(18,0), 
  program_id decimal(18,0), 
  program_update_date string, 
  attribute_category string, 
  attribute1 string, 
  attribute2 string, 
  attribute3 string, 
  attribute4 string, 
  attribute5 string, 
  attribute6 string, 
  attribute7 string, 
  attribute8 string, 
  attribute9 string, 
  attribute10 string, 
  attribute11 string, 
  attribute12 string, 
  attribute13 string, 
  attribute14 string, 
  attribute15 string, 
  yielded_cost decimal(18,10), 
  source_organization_id decimal(15,0), 
  vendor_id decimal(15,0), 
  allocation_percent decimal(18,6), 
  vendor_site_id decimal(15,0), 
  ship_method string, 
  storeday string)
PARTITIONED BY ( 
  fecha_actualizacion string)
LOCATION '${hiveconf:paths3}/gb_mdl_mexico_erp.db/cst_item_cost_details_hist';
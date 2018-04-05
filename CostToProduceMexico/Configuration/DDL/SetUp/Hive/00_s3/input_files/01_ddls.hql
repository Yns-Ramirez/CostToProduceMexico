create database if not exists gb_mdl_mexico_erp;


CREATE EXTERNAL TABLE IF NOT EXISTS gb_mdl_mexico_erp.cst_item_cost_details_hist(
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
LOCATION '${hiveconf:bucket_mexico_costoproducir_mdl}/cst_item_cost_details_hist';




CREATE EXTERNAL TABLE IF NOT EXISTS gb_mdl_mexico_erp.wip_repetitive_items_hist(
  
  wip_entity_id int, 
  line_id int, 
  organization_id int, 
  last_update_date string, 
  last_update_date_h string, 
  last_updated_by float, 
  creation_date string, 
  creation_date_h string, 
  created_by float, 
  last_update_login float, 
  request_id int, 
  program_application_id int, 
  program_id int, 
  program_update_date string, 
  program_update_date_h string, 
  primary_item_id int, 
  alternate_bom_designator string, 
  alternate_routing_designator string, 
  class_code string, 
  wip_supply_type float, 
  completion_subinventory string, 
  completion_locator_id int, 
  load_distribution_priority float, 
  primary_line_flag float, 
  production_line_rate float, 
  overcompletion_tolerance_type float, 
  overcompletion_tolerance_value float, 
  attribute1 string, 
  attribute6 string,
  storeday string)
PARTITIONED BY ( 
  fecha_actualizacion string)
LOCATION '${hiveconf:bucket_mexico_costoproducir_mdl}/wip_repetitive_items_hist';



CREATE EXTERNAL TABLE IF NOT EXISTS gb_mdl_mexico_erp.a_pago_empleado(
  tiponomina_id string, 
  empleado_id string, 
  cuentanatural_id string, 
  analisislocal_id string, 
  concepto_id int, 
  region_id string, 
  montopago float, 
  tipomoneda_id int, 
  sistemafuente string, 
  usuarioetl string, 
  fechacarga string, 
  fechacambio string, 
  storeday string)
PARTITIONED BY(fechapago string)
LOCATION '${hiveconf:bucket_mexico_costoproducir_mdl}/a_pago_empleado';



CREATE EXTERNAL TABLE IF NOT EXISTS gb_mdl_mexico_costoproducir.mtl_onhand_diario(
  organization_id int, 
  inventory_item_id int, 
  subinventory_code string, 
  max_last_update_date string, 
  primary_transaction_quantity float,
  storeday string)
PARTITIONED BY (fecha string)
LOCATION '${hiveconf:bucket_mexico_costoproducir_mdl}/mtl_onhand_diario';
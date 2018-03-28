CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_transaction_accounts AS select
    mtl_transaction_accounts.transaction_id            
    ,mtl_transaction_accounts.reference_account  
    ,regexp_replace(mtl_transaction_accounts.last_update_date, '-', '/') as last_update_date        
    ,mtl_transaction_accounts.last_update_date_h        
    ,mtl_transaction_accounts.last_updated_by           
    ,regexp_replace(mtl_transaction_accounts.creation_date, '-', '/') as creation_date
    ,mtl_transaction_accounts.creation_date_h           
    ,mtl_transaction_accounts.created_by                
    ,mtl_transaction_accounts.last_update_login         
    ,mtl_transaction_accounts.inventory_item_id         
    ,mtl_transaction_accounts.organization_id  
    ,regexp_replace(mtl_transaction_accounts.transaction_date, '-', '/') as transaction_date
    ,mtl_transaction_accounts.transaction_date_h        
    ,mtl_transaction_accounts.transaction_source_id     
    ,mtl_transaction_accounts.transaction_source_type_id
    ,mtl_transaction_accounts.transaction_value         
    ,mtl_transaction_accounts.primary_quantity          
    ,mtl_transaction_accounts.gl_batch_id               
    ,mtl_transaction_accounts.accounting_line_type      
    ,mtl_transaction_accounts.base_transaction_value    
    ,mtl_transaction_accounts.contra_set_id             
    ,mtl_transaction_accounts.rate_or_amount            
    ,mtl_transaction_accounts.basis_type                
    ,mtl_transaction_accounts.resource_id               
    ,mtl_transaction_accounts.cost_element_id           
    ,mtl_transaction_accounts.activity_id               
    ,mtl_transaction_accounts.currency_code
    ,regexp_replace(mtl_transaction_accounts.currency_conversion_date, '-', '/') as currency_conversion_date
    ,mtl_transaction_accounts.currency_conversion_date_h
    ,mtl_transaction_accounts.currency_conversion_type  
    ,mtl_transaction_accounts.currency_conversion_rate  
    ,mtl_transaction_accounts.request_id                
    ,mtl_transaction_accounts.program_application_id    
    ,mtl_transaction_accounts.program_id                
    ,regexp_replace(mtl_transaction_accounts.program_update_date, '-', '/') as program_update_date
    ,mtl_transaction_accounts.program_update_date_h     
    ,mtl_transaction_accounts.encumbrance_type_id       
    ,mtl_transaction_accounts.repetitive_schedule_id    
    ,mtl_transaction_accounts.gl_sl_link_id
    ,mtl_transaction_accounts.storeday 
from erp_mexico_sz.mtl_transaction_accounts;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_transaccion_materiales AS select mtl_material_transactions.transaction_id,  
    mtl_material_transactions.last_update_date,
    mtl_material_transactions.last_update_date_h,  
    mtl_material_transactions.creation_date,
    mtl_material_transactions.creation_date_h,     
    mtl_material_transactions.inventory_item_id,   
    mtl_material_transactions.organization_id,     
    mtl_material_transactions.subinventory_code,   
    mtl_material_transactions.transaction_type_id,
    mtl_material_transactions.transaction_source_type_id,
    mtl_material_transactions.transaction_source_id,
    mtl_material_transactions.transaction_action_id,
    mtl_material_transactions.transaction_quantity,
    mtl_material_transactions.transaction_uom,     
    mtl_material_transactions.primary_quantity,
    mtl_material_transactions.transaction_date,
    substr(trim(mtl_material_transactions.transaction_date_h),1,8),
    mtl_material_transactions.transaction_reference,
    mtl_material_transactions.actual_cost,         
    mtl_material_transactions.transaction_cost,    
    mtl_material_transactions.prior_cost,          
    mtl_material_transactions.new_cost,            
    mtl_material_transactions.quantity_adjusted,   
    mtl_material_transactions.department_id,       
    mtl_material_transactions.transfer_organization_id,
    mtl_material_transactions.ship_to_location_id, 
    mtl_material_transactions.fuente_mak,          
    mtl_material_transactions.completion_transaction_id,
    mtl_material_transactions.transaction_set_id,  
    mtl_material_transactions.reason_id,           
    mtl_material_transactions.source_code,         
    mtl_material_transactions.acct_period_id,      
    mtl_material_transactions.last_updated_by,     
    mtl_material_transactions.created_by,          
    mtl_material_transactions.request_id,          
    mtl_material_transactions.revision,            
    mtl_material_transactions.transaction_source_name,
    mtl_material_transactions.physical_adjustment_id,
    mtl_material_transactions.source_line_id,      
    mtl_material_transactions.transfer_subinventory,
    mtl_material_transactions.rma_line_id,         
    mtl_material_transactions.attribute_category,  
    mtl_material_transactions.attribute7,          
    mtl_material_transactions.attribute8,          
    mtl_material_transactions.storeday                             
from erp_mexico_sz.mtl_material_transactions;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_categorias AS select
mtl_categories_b.category_id 
,mtl_categories_b.structure_id 
,mtl_categories_b.description 
,mtl_categories_b.disable_date 
,mtl_categories_b.disable_date_h 
,mtl_categories_b.segment1 
,mtl_categories_b.segment2 
,mtl_categories_b.segment3 
,mtl_categories_b.segment4 
,mtl_categories_b.segment5 
,mtl_categories_b.segment6 
,mtl_categories_b.segment7 
,mtl_categories_b.segment8 
,mtl_categories_b.segment9 
,mtl_categories_b.segment10 
,mtl_categories_b.segment11 
,mtl_categories_b.segment12 
,mtl_categories_b.segment13 
,mtl_categories_b.segment14 
,mtl_categories_b.segment15 
,mtl_categories_b.segment16 
,mtl_categories_b.segment17 
,mtl_categories_b.segment18 
,mtl_categories_b.segment19 
,mtl_categories_b.segment20 
,mtl_categories_b.summary_flag 
,mtl_categories_b.enabled_flag 
,mtl_categories_b.start_date_active 
,mtl_categories_b.start_date_active_h 
,mtl_categories_b.end_date_active 
,mtl_categories_b.end_date_active_h 
,mtl_categories_b.last_update_date 
,mtl_categories_b.last_update_date_h 
,mtl_categories_b.last_updated_by 
,mtl_categories_b.creation_date 
,mtl_categories_b.creation_date_h 
,mtl_categories_b.created_by 
,mtl_categories_b.last_update_login 
,mtl_categories_b.request_id 
,mtl_categories_b.program_application_id 
,mtl_categories_b.program_id 
,mtl_categories_b.program_update_date 
,mtl_categories_b.program_update_date_h 
,mtl_categories_b.web_status 
,mtl_categories_b.supplier_enabled_flag 
,mtl_categories_b.storeday
from erp_mexico_sz.MTL_CATEGORIES_b;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_flexfields_materiales AS SELECT   mtl_item_flexfields.item_id                       
, mtl_item_flexfields.inventory_item_id             
, mtl_item_flexfields.organization_id         
,regexp_replace(substr(mtl_item_flexfields.last_update_date, 3, 10), '-', '/')       
, mtl_item_flexfields.last_update_date_h            
, mtl_item_flexfields.last_updated_by               
,regexp_replace(substr(mtl_item_flexfields.creation_date, 3, 10), '-', '/') 
, mtl_item_flexfields.creation_date_h               
, mtl_item_flexfields.created_by                    
, mtl_item_flexfields.last_update_login             
, mtl_item_flexfields.summary_flag                  
, mtl_item_flexfields.enabled_flag                  
,regexp_replace(substr(mtl_item_flexfields.start_date_active, 3, 10), '-', '/') 
, mtl_item_flexfields.start_date_active_h           
,regexp_replace(substr(mtl_item_flexfields.end_date_active, 3, 10), '-', '/') 
, mtl_item_flexfields.end_date_active_h             
, mtl_item_flexfields.description                   
, mtl_item_flexfields.buyer_id                      
, mtl_item_flexfields.accounting_rule_id            
, mtl_item_flexfields.invoicing_rule_id             
, mtl_item_flexfields.segment1                      
, mtl_item_flexfields.segment2                      
, mtl_item_flexfields.segment3                      
, mtl_item_flexfields.segment4                      
, mtl_item_flexfields.segment5                      
, mtl_item_flexfields.segment6                      
, mtl_item_flexfields.segment7                      
, mtl_item_flexfields.segment8                      
, mtl_item_flexfields.segment9                      
, mtl_item_flexfields.segment10                     
, mtl_item_flexfields.segment11                     
, mtl_item_flexfields.segment12                     
, mtl_item_flexfields.segment13                     
, mtl_item_flexfields.segment14                     
, mtl_item_flexfields.segment15                     
, mtl_item_flexfields.segment16                     
, mtl_item_flexfields.segment17                     
, mtl_item_flexfields.segment18                     
, mtl_item_flexfields.segment19                     
, mtl_item_flexfields.segment20                     
, mtl_item_flexfields.attribute_category            
, mtl_item_flexfields.attribute1                    
, mtl_item_flexfields.attribute2                    
, mtl_item_flexfields.attribute3                    
, mtl_item_flexfields.attribute4                    
, mtl_item_flexfields.attribute5                    
, mtl_item_flexfields.attribute6                    
, mtl_item_flexfields.attribute7                    
, mtl_item_flexfields.attribute8                    
, mtl_item_flexfields.attribute9                    
, mtl_item_flexfields.attribute10                   
, mtl_item_flexfields.attribute11                   
, mtl_item_flexfields.attribute12                   
, mtl_item_flexfields.attribute13                   
, mtl_item_flexfields.attribute14                   
, mtl_item_flexfields.attribute15                   
, mtl_item_flexfields.purchasing_item_flag          
, mtl_item_flexfields.shippable_item_flag           
, mtl_item_flexfields.customer_order_flag           
, mtl_item_flexfields.internal_order_flag           
, mtl_item_flexfields.service_item_flag             
, mtl_item_flexfields.inventory_item_flag           
, mtl_item_flexfields.eng_item_flag                 
, mtl_item_flexfields.inventory_asset_flag          
, mtl_item_flexfields.purchasing_enabled_flag       
, mtl_item_flexfields.customer_order_enabled_flag   
, mtl_item_flexfields.internal_order_enabled_flag   
, mtl_item_flexfields.so_transactions_flag          
, mtl_item_flexfields.mtl_transactions_enabled_flag 
, mtl_item_flexfields.stock_enabled_flag            
, mtl_item_flexfields.bom_enabled_flag              
, mtl_item_flexfields.build_in_wip_flag             
, mtl_item_flexfields.revision_qty_control_code     
, mtl_item_flexfields.item_catalog_group_id         
, mtl_item_flexfields.catalog_status_flag           
, mtl_item_flexfields.returnable_flag               
, mtl_item_flexfields.default_shipping_org          
, mtl_item_flexfields.collateral_flag               
, mtl_item_flexfields.taxable_flag                  
, mtl_item_flexfields.purchasing_tax_code           
, mtl_item_flexfields.qty_rcv_exception_code        
, mtl_item_flexfields.allow_item_desc_update_flag   
, mtl_item_flexfields.inspection_required_flag      
, mtl_item_flexfields.receipt_required_flag         
, mtl_item_flexfields.market_price                  
, mtl_item_flexfields.hazard_class_id               
, mtl_item_flexfields.rfq_required_flag             
, mtl_item_flexfields.qty_rcv_tolerance             
, mtl_item_flexfields.list_price_per_unit           
, mtl_item_flexfields.un_number_id                  
, mtl_item_flexfields.price_tolerance_percent       
, mtl_item_flexfields.asset_category_id             
, mtl_item_flexfields.rounding_factor               
, mtl_item_flexfields.unit_of_issue                 
, mtl_item_flexfields.enforce_ship_to_location_code 
, mtl_item_flexfields.allow_substitute_receipts_flag
, mtl_item_flexfields.allow_unordered_receipts_flag 
, mtl_item_flexfields.allow_express_delivery_flag   
, mtl_item_flexfields.days_early_receipt_allowed    
, mtl_item_flexfields.days_late_receipt_allowed     
, mtl_item_flexfields.receipt_days_exception_code   
, mtl_item_flexfields.receiving_routing_id          
, mtl_item_flexfields.invoice_close_tolerance       
, mtl_item_flexfields.receive_close_tolerance       
, mtl_item_flexfields.auto_lot_alpha_prefix         
, mtl_item_flexfields.start_auto_lot_number         
, mtl_item_flexfields.lot_control_code              
, mtl_item_flexfields.shelf_life_code               
, mtl_item_flexfields.shelf_life_days               
, mtl_item_flexfields.serial_number_control_code    
, mtl_item_flexfields.start_auto_serial_number      
, mtl_item_flexfields.auto_serial_alpha_prefix      
, mtl_item_flexfields.source_type                   
, mtl_item_flexfields.source_organization_id        
, mtl_item_flexfields.source_subinventory           
, mtl_item_flexfields.expense_account               
, mtl_item_flexfields.encumbrance_account           
, mtl_item_flexfields.restrict_subinventories_code  
, mtl_item_flexfields.unit_weight                   
, mtl_item_flexfields.weight_uom_code               
, mtl_item_flexfields.volume_uom_code               
, mtl_item_flexfields.unit_volume                   
, mtl_item_flexfields.restrict_locators_code        
, mtl_item_flexfields.location_control_code         
, mtl_item_flexfields.shrinkage_rate                
, mtl_item_flexfields.acceptable_early_days         
, mtl_item_flexfields.planning_time_fence_code      
, mtl_item_flexfields.demand_time_fence_code        
, mtl_item_flexfields.lead_time_lot_size            
, mtl_item_flexfields.std_lot_size                  
, mtl_item_flexfields.cum_manufacturing_lead_time   
, mtl_item_flexfields.overrun_percentage            
, mtl_item_flexfields.mrp_calculate_atp_flag        
, mtl_item_flexfields.acceptable_rate_increase      
, mtl_item_flexfields.acceptable_rate_decrease      
, mtl_item_flexfields.cumulative_total_lead_time    
, mtl_item_flexfields.planning_time_fence_days      
, mtl_item_flexfields.demand_time_fence_days        
, mtl_item_flexfields.end_assembly_pegging_flag     
, mtl_item_flexfields.repetitive_planning_flag      
, mtl_item_flexfields.planning_exception_set        
, mtl_item_flexfields.bom_item_type                 
, mtl_item_flexfields.pick_components_flag          
, mtl_item_flexfields.replenish_to_order_flag       
, mtl_item_flexfields.base_item_id                  
, mtl_item_flexfields.atp_components_flag           
, mtl_item_flexfields.atp_flag                      
, mtl_item_flexfields.fixed_lead_time               
, mtl_item_flexfields.variable_lead_time            
, mtl_item_flexfields.wip_supply_locator_id         
, mtl_item_flexfields.wip_supply_type               
, mtl_item_flexfields.wip_supply_subinventory       
, mtl_item_flexfields.overcompletion_tolerance_type 
, mtl_item_flexfields.overcompletion_tolerance_value
, mtl_item_flexfields.primary_uom_code              
, mtl_item_flexfields.primary_unit_of_measure       
, mtl_item_flexfields.allowed_units_lookup_code     
, mtl_item_flexfields.cost_of_sales_account         
, mtl_item_flexfields.sales_account                 
, mtl_item_flexfields.default_include_in_rollup_flag
, mtl_item_flexfields.inventory_item_status_code    
, mtl_item_flexfields.inventory_planning_code       
, mtl_item_flexfields.planner_code                  
, mtl_item_flexfields.planning_make_buy_code        
, mtl_item_flexfields.fixed_lot_multiplier          
, mtl_item_flexfields.rounding_control_type         
, mtl_item_flexfields.carrying_cost                 
, mtl_item_flexfields.postprocessing_lead_time      
, mtl_item_flexfields.preprocessing_lead_time       
, mtl_item_flexfields.full_lead_time                
, mtl_item_flexfields.order_cost                    
, mtl_item_flexfields.mrp_safety_stock_percent      
, mtl_item_flexfields.mrp_safety_stock_code         
, mtl_item_flexfields.min_minmax_quantity           
, mtl_item_flexfields.max_minmax_quantity           
, mtl_item_flexfields.minimum_order_quantity        
, mtl_item_flexfields.fixed_order_quantity          
, mtl_item_flexfields.fixed_days_supply             
, mtl_item_flexfields.maximum_order_quantity        
, mtl_item_flexfields.atp_rule_id                   
, mtl_item_flexfields.picking_rule_id               
, mtl_item_flexfields.reservable_type               
, mtl_item_flexfields.positive_measurement_error    
, mtl_item_flexfields.negative_measurement_error    
, mtl_item_flexfields.engineering_ecn_code          
, mtl_item_flexfields.engineering_item_id           
,regexp_replace(substr(mtl_item_flexfields.engineering_date, 3, 10), '-', '/') 
, mtl_item_flexfields.engineering_date_h            
, mtl_item_flexfields.service_starting_delay        
, mtl_item_flexfields.vendor_warranty_flag          
, mtl_item_flexfields.serviceable_component_flag    
, mtl_item_flexfields.serviceable_product_flag      
, mtl_item_flexfields.base_warranty_service_id      
, mtl_item_flexfields.payment_terms_id              
, mtl_item_flexfields.preventive_maintenance_flag   
, mtl_item_flexfields.primary_specialist_id         
, mtl_item_flexfields.secondary_specialist_id       
, mtl_item_flexfields.serviceable_item_class_id     
, mtl_item_flexfields.time_billable_flag            
, mtl_item_flexfields.material_billable_flag        
, mtl_item_flexfields.expense_billable_flag         
, mtl_item_flexfields.prorate_service_flag          
, mtl_item_flexfields.coverage_schedule_id          
, mtl_item_flexfields.service_duration_period_code  
, mtl_item_flexfields.service_duration              
, mtl_item_flexfields.warranty_vendor_id            
, mtl_item_flexfields.max_warranty_amount           
, mtl_item_flexfields.response_time_period_code     
, mtl_item_flexfields.response_time_value           
, mtl_item_flexfields.new_revision_code             
, mtl_item_flexfields.invoiceable_item_flag         
, mtl_item_flexfields.tax_code                      
, mtl_item_flexfields.invoice_enabled_flag          
, mtl_item_flexfields.must_use_approved_vendor_flag 
, mtl_item_flexfields.request_id                    
, mtl_item_flexfields.program_application_id        
, mtl_item_flexfields.program_id                    
,regexp_replace(substr(mtl_item_flexfields.program_update_date, 3, 10), '-', '/')     
, mtl_item_flexfields.program_update_date_h         
, mtl_item_flexfields.outside_operation_flag        
, mtl_item_flexfields.outside_operation_uom_type    
, mtl_item_flexfields.safety_stock_bucket_days      
, mtl_item_flexfields.auto_reduce_mps               
, mtl_item_flexfields.costing_enabled_flag          
, mtl_item_flexfields.auto_created_config_flag      
, mtl_item_flexfields.cycle_count_enabled_flag      
, mtl_item_flexfields.item_type                     
, mtl_item_flexfields.model_config_clause_name      
, mtl_item_flexfields.ship_model_complete_flag      
, mtl_item_flexfields.mrp_planning_code             
, mtl_item_flexfields.return_inspection_requirement 
, mtl_item_flexfields.ato_forecast_control          
, mtl_item_flexfields.release_time_fence_code       
, mtl_item_flexfields.release_time_fence_days       
, mtl_item_flexfields.container_item_flag           
, mtl_item_flexfields.container_type_code           
, mtl_item_flexfields.internal_volume               
, mtl_item_flexfields.maximum_load_weight           
, mtl_item_flexfields.vehicle_item_flag             
, mtl_item_flexfields.minimum_fill_percent          
, mtl_item_flexfields.product_family_item_id        
, mtl_item_flexfields.effectivity_control           
, mtl_item_flexfields.check_shortages_flag          
, mtl_item_flexfields.over_shipment_tolerance       
, mtl_item_flexfields.under_shipment_tolerance      
, mtl_item_flexfields.over_return_tolerance         
, mtl_item_flexfields.under_return_tolerance        
, mtl_item_flexfields.equipment_type                
, mtl_item_flexfields.recovered_part_disp_code      
, mtl_item_flexfields.defect_tracking_on_flag       
, mtl_item_flexfields.usage_item_flag               
, mtl_item_flexfields.event_flag                    
, mtl_item_flexfields.electronic_flag               
, mtl_item_flexfields.downloadable_flag             
, mtl_item_flexfields.vol_discount_exempt_flag      
, mtl_item_flexfields.coupon_exempt_flag            
, mtl_item_flexfields.comms_nl_trackable_flag       
, mtl_item_flexfields.asset_creation_code           
, mtl_item_flexfields.comms_activation_reqd_flag    
, mtl_item_flexfields.orderable_on_web_flag         
, mtl_item_flexfields.back_orderable_flag           
, mtl_item_flexfields.web_status                    
, mtl_item_flexfields.indivisible_flag              
, mtl_item_flexfields.dimension_uom_code            
, mtl_item_flexfields.unit_length                   
, mtl_item_flexfields.unit_width                    
, mtl_item_flexfields.unit_height                   
, mtl_item_flexfields.bulk_picked_flag              
, mtl_item_flexfields.lot_status_enabled            
, mtl_item_flexfields.default_lot_status_id         
, mtl_item_flexfields.serial_status_enabled         
, mtl_item_flexfields.default_serial_status_id      
, mtl_item_flexfields.lot_split_enabled             
, mtl_item_flexfields.lot_merge_enabled             
, mtl_item_flexfields.inventory_carry_penalty       
, mtl_item_flexfields.operation_slack_penalty       
, mtl_item_flexfields.financing_allowed_flag        
, mtl_item_flexfields.eam_item_type                 
, mtl_item_flexfields.eam_activity_type_code        
, mtl_item_flexfields.eam_activity_cause_code       
, mtl_item_flexfields.eam_act_notification_flag     
, mtl_item_flexfields.eam_act_shutdown_status       
, mtl_item_flexfields.dual_uom_control              
, mtl_item_flexfields.secondary_uom_code            
, mtl_item_flexfields.dual_uom_deviation_high       
, mtl_item_flexfields.dual_uom_deviation_low        
, mtl_item_flexfields.contract_item_type_code       
, mtl_item_flexfields.subscription_depend_flag      
, mtl_item_flexfields.serv_req_enabled_code         
, mtl_item_flexfields.serv_billing_enabled_flag     
, mtl_item_flexfields.serv_importance_level         
, mtl_item_flexfields.planned_inv_point_flag        
, mtl_item_flexfields.lot_translate_enabled         
, mtl_item_flexfields.default_so_source_type        
, mtl_item_flexfields.create_supply_flag            
, mtl_item_flexfields.substitution_window_code      
, mtl_item_flexfields.substitution_window_days      
, mtl_item_flexfields.ib_item_instance_class        
, mtl_item_flexfields.config_model_type             
, mtl_item_flexfields.lot_substitution_enabled      
, mtl_item_flexfields.minimum_license_quantity      
, mtl_item_flexfields.eam_activity_source_code      
, mtl_item_flexfields.item_number                   
, mtl_item_flexfields.padded_item_number, mtl_item_flexfields.storeday
FROM erp_mexico_sz.MTL_ITEM_FLEXFIELDS;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_catalogo_materiales AS select mtl_system_items_b.inventory_item_id   
, mtl_system_items_b.organization_id
,regexp_replace(mtl_system_items_b.last_update_date, '-', '/')      
, mtl_system_items_b.last_updated_by
,regexp_replace(mtl_system_items_b.creation_date, '-', '/')      
, mtl_system_items_b.created_by          
, mtl_system_items_b.last_update_login   
, mtl_system_items_b.summary_flag        
, mtl_system_items_b.enabled_flag
,regexp_replace(mtl_system_items_b.start_date_active, '-', '/')         
,regexp_replace(mtl_system_items_b.end_date_active, '-', '/')    
, mtl_system_items_b.description         
, mtl_system_items_b.buyer_id            
, mtl_system_items_b.accounting_rule_id  
, mtl_system_items_b.invoicing_rule_id   
, mtl_system_items_b.segment1            
, mtl_system_items_b.segment2            
, mtl_system_items_b.segment3            
, mtl_system_items_b.segment4            
, mtl_system_items_b.segment5            
, mtl_system_items_b.segment6            
, mtl_system_items_b.segment7            
, mtl_system_items_b.segment8            
, mtl_system_items_b.segment9            
, mtl_system_items_b.segment10           
, mtl_system_items_b.segment11           
, mtl_system_items_b.segment12           
, mtl_system_items_b.segment13           
, mtl_system_items_b.segment14           
, mtl_system_items_b.segment15           
, mtl_system_items_b.segment16           
, mtl_system_items_b.segment17           
, mtl_system_items_b.segment18           
, mtl_system_items_b.segment19           
, mtl_system_items_b.segment20           
, mtl_system_items_b.attribute_category  
, mtl_system_items_b.attribute1          
, mtl_system_items_b.attribute2          
, mtl_system_items_b.attribute3          
, mtl_system_items_b.attribute4          
, mtl_system_items_b.attribute5          
, mtl_system_items_b.attribute6          
, mtl_system_items_b.attribute7          
, mtl_system_items_b.attribute8          
, mtl_system_items_b.attribute9          
, mtl_system_items_b.attribute10         
, mtl_system_items_b.attribute11         
, mtl_system_items_b.attribute12         
, mtl_system_items_b.attribute13         
, mtl_system_items_b.attribute14         
, mtl_system_items_b.attribute15         
, mtl_system_items_b.purchasing_item_flag
, mtl_system_items_b.shippable_item_flag 
, mtl_system_items_b.customer_order_flag 
, mtl_system_items_b.internal_order_flag 
, mtl_system_items_b.service_item_flag   
, mtl_system_items_b.inventory_item_flag 
, mtl_system_items_b.eng_item_flag       
, mtl_system_items_b.inventory_asset_flag
, mtl_system_items_b.purchasing_enabled_flag
, mtl_system_items_b.customer_order_enabled_flag
, mtl_system_items_b.internal_order_enabled_flag
, mtl_system_items_b.so_transactions_flag
, mtl_system_items_b.mtl_transactions_enabled_flag
, mtl_system_items_b.stock_enabled_flag  
, mtl_system_items_b.bom_enabled_flag    
, mtl_system_items_b.build_in_wip_flag   
, mtl_system_items_b.revision_qty_control_code
, mtl_system_items_b.item_catalog_group_id
, mtl_system_items_b.catalog_status_flag 
, mtl_system_items_b.returnable_flag     
, mtl_system_items_b.default_shipping_org
, mtl_system_items_b.collateral_flag     
, mtl_system_items_b.taxable_flag        
, mtl_system_items_b.qty_rcv_exception_code
, mtl_system_items_b.allow_item_desc_update_flag
, mtl_system_items_b.inspection_required_flag
, mtl_system_items_b.receipt_required_flag
, mtl_system_items_b.market_price        
, mtl_system_items_b.hazard_class_id     
, mtl_system_items_b.rfq_required_flag   
, mtl_system_items_b.qty_rcv_tolerance   
, mtl_system_items_b.list_price_per_unit 
, mtl_system_items_b.un_number_id        
, mtl_system_items_b.price_tolerance_percent
, mtl_system_items_b.asset_category_id   
, mtl_system_items_b.rounding_factor     
, mtl_system_items_b.unit_of_issue       
, mtl_system_items_b.enforce_ship_to_location_code
, mtl_system_items_b.allow_substitute_receipts_flag
, mtl_system_items_b.allow_unordered_receipts_flag
, mtl_system_items_b.allow_express_delivery_flag
, mtl_system_items_b.days_early_receipt_allowed
, mtl_system_items_b.days_late_receipt_allowed
, mtl_system_items_b.receipt_days_exception_code
, mtl_system_items_b.receiving_routing_id
, mtl_system_items_b.invoice_close_tolerance
, mtl_system_items_b.receive_close_tolerance
, mtl_system_items_b.auto_lot_alpha_prefix
, mtl_system_items_b.start_auto_lot_number
, mtl_system_items_b.lot_control_code    
, mtl_system_items_b.shelf_life_code     
, mtl_system_items_b.shelf_life_days     
, mtl_system_items_b.serial_number_control_code
, mtl_system_items_b.start_auto_serial_number
, mtl_system_items_b.auto_serial_alpha_prefix
, mtl_system_items_b.source_type         
, mtl_system_items_b.source_organization_id
, mtl_system_items_b.source_subinventory 
, mtl_system_items_b.expense_account     
, mtl_system_items_b.encumbrance_account 
, mtl_system_items_b.restrict_subinventories_code
, mtl_system_items_b.unit_weight         
, mtl_system_items_b.weight_uom_code     
, mtl_system_items_b.volume_uom_code     
, mtl_system_items_b.unit_volume         
, mtl_system_items_b.restrict_locators_code
, mtl_system_items_b.location_control_code
, mtl_system_items_b.shrinkage_rate      
, mtl_system_items_b.acceptable_early_days
, mtl_system_items_b.planning_time_fence_code
, mtl_system_items_b.demand_time_fence_code
, mtl_system_items_b.lead_time_lot_size  
, mtl_system_items_b.std_lot_size        
, mtl_system_items_b.cum_manufacturing_lead_time
, mtl_system_items_b.overrun_percentage  
, mtl_system_items_b.mrp_calculate_atp_flag
, mtl_system_items_b.acceptable_rate_increase
, mtl_system_items_b.acceptable_rate_decrease
, mtl_system_items_b.cumulative_total_lead_time
, mtl_system_items_b.planning_time_fence_days
, mtl_system_items_b.demand_time_fence_days
, mtl_system_items_b.end_assembly_pegging_flag
, mtl_system_items_b.repetitive_planning_flag
, mtl_system_items_b.planning_exception_set
, mtl_system_items_b.bom_item_type       
, mtl_system_items_b.pick_components_flag
, mtl_system_items_b.replenish_to_order_flag
, mtl_system_items_b.base_item_id        
, mtl_system_items_b.atp_components_flag 
, mtl_system_items_b.atp_flag            
, mtl_system_items_b.fixed_lead_time     
, mtl_system_items_b.variable_lead_time  
, mtl_system_items_b.wip_supply_locator_id
, mtl_system_items_b.wip_supply_type     
, mtl_system_items_b.wip_supply_subinventory
, mtl_system_items_b.primary_uom_code    
, mtl_system_items_b.primary_unit_of_measure
, mtl_system_items_b.allowed_units_lookup_code
, mtl_system_items_b.cost_of_sales_account
, mtl_system_items_b.sales_account       
, mtl_system_items_b.default_include_in_rollup_flag
, mtl_system_items_b.inventory_item_status_code
, mtl_system_items_b.inventory_planning_code
, mtl_system_items_b.planner_code        
, mtl_system_items_b.planning_make_buy_code
, mtl_system_items_b.fixed_lot_multiplier
, mtl_system_items_b.rounding_control_type
, mtl_system_items_b.carrying_cost       
, mtl_system_items_b.postprocessing_lead_time
, mtl_system_items_b.preprocessing_lead_time
, mtl_system_items_b.full_lead_time      
, mtl_system_items_b.order_cost          
, mtl_system_items_b.mrp_safety_stock_percent
, mtl_system_items_b.mrp_safety_stock_code
, mtl_system_items_b.min_minmax_quantity 
, mtl_system_items_b.max_minmax_quantity 
, mtl_system_items_b.minimum_order_quantity
, mtl_system_items_b.fixed_order_quantity
, mtl_system_items_b.fixed_days_supply   
, mtl_system_items_b.maximum_order_quantity
, mtl_system_items_b.atp_rule_id         
, mtl_system_items_b.picking_rule_id     
, mtl_system_items_b.reservable_type     
, mtl_system_items_b.positive_measurement_error
, mtl_system_items_b.negative_measurement_error
, mtl_system_items_b.engineering_ecn_code
, mtl_system_items_b.engineering_item_id 
,regexp_replace(mtl_system_items_b.engineering_date, '-', '/') 
, mtl_system_items_b.service_starting_delay
, mtl_system_items_b.vendor_warranty_flag
, mtl_system_items_b.serviceable_component_flag
, mtl_system_items_b.serviceable_product_flag
, mtl_system_items_b.base_warranty_service_id
, mtl_system_items_b.payment_terms_id    
, mtl_system_items_b.preventive_maintenance_flag
, mtl_system_items_b.primary_specialist_id
, mtl_system_items_b.secondary_specialist_id
, mtl_system_items_b.serviceable_item_class_id
, mtl_system_items_b.time_billable_flag  
, mtl_system_items_b.material_billable_flag
, mtl_system_items_b.expense_billable_flag
, mtl_system_items_b.prorate_service_flag
, mtl_system_items_b.coverage_schedule_id
, mtl_system_items_b.service_duration_period_code
, mtl_system_items_b.service_duration    
, mtl_system_items_b.warranty_vendor_id  
, mtl_system_items_b.max_warranty_amount 
, mtl_system_items_b.response_time_period_code
, mtl_system_items_b.response_time_value 
, mtl_system_items_b.new_revision_code   
, mtl_system_items_b.invoiceable_item_flag
, mtl_system_items_b.tax_code            
, mtl_system_items_b.invoice_enabled_flag
, mtl_system_items_b.must_use_approved_vendor_flag
, mtl_system_items_b.request_id          
, mtl_system_items_b.program_application_id
, mtl_system_items_b.program_id   
,regexp_replace(mtl_system_items_b.program_update_date, '-', '/')        
, mtl_system_items_b.outside_operation_flag
, mtl_system_items_b.outside_operation_uom_type
, mtl_system_items_b.safety_stock_bucket_days
, mtl_system_items_b.auto_reduce_mps     
, mtl_system_items_b.costing_enabled_flag
, mtl_system_items_b.auto_created_config_flag
, mtl_system_items_b.cycle_count_enabled_flag
, mtl_system_items_b.item_type           
, mtl_system_items_b.model_config_clause_name
, mtl_system_items_b.ship_model_complete_flag
, mtl_system_items_b.mrp_planning_code   
, mtl_system_items_b.return_inspection_requirement
, mtl_system_items_b.ato_forecast_control
, mtl_system_items_b.release_time_fence_code
, mtl_system_items_b.release_time_fence_days
, mtl_system_items_b.container_item_flag 
, mtl_system_items_b.vehicle_item_flag   
, mtl_system_items_b.maximum_load_weight 
, mtl_system_items_b.minimum_fill_percent
, mtl_system_items_b.container_type_code 
, mtl_system_items_b.internal_volume    
,regexp_replace(mtl_system_items_b.wh_update_date, '-', '/')  
, mtl_system_items_b.product_family_item_id
, mtl_system_items_b.purchasing_tax_code 
, mtl_system_items_b.overcompletion_tolerance_type
, mtl_system_items_b.overcompletion_tolerance_value
, mtl_system_items_b.effectivity_control 
, mtl_system_items_b.check_shortages_flag
, mtl_system_items_b.over_shipment_tolerance
, mtl_system_items_b.under_shipment_tolerance
, mtl_system_items_b.over_return_tolerance
, mtl_system_items_b.under_return_tolerance
, mtl_system_items_b.equipment_type      
, mtl_system_items_b.recovered_part_disp_code
, mtl_system_items_b.defect_tracking_on_flag
, mtl_system_items_b.usage_item_flag     
, mtl_system_items_b.event_flag          
, mtl_system_items_b.electronic_flag     
, mtl_system_items_b.downloadable_flag   
, mtl_system_items_b.vol_discount_exempt_flag
, mtl_system_items_b.coupon_exempt_flag  
, mtl_system_items_b.comms_nl_trackable_flag
, mtl_system_items_b.asset_creation_code 
, mtl_system_items_b.comms_activation_reqd_flag
, mtl_system_items_b.orderable_on_web_flag
, mtl_system_items_b.back_orderable_flag 
, mtl_system_items_b.web_status          
, mtl_system_items_b.indivisible_flag    
, mtl_system_items_b.dimension_uom_code  
, mtl_system_items_b.unit_length         
, mtl_system_items_b.unit_width          
, mtl_system_items_b.unit_height         
, mtl_system_items_b.bulk_picked_flag    
, mtl_system_items_b.lot_status_enabled  
, mtl_system_items_b.default_lot_status_id
, mtl_system_items_b.serial_status_enabled
, mtl_system_items_b.default_serial_status_id
, mtl_system_items_b.lot_split_enabled   
, mtl_system_items_b.lot_merge_enabled   
, mtl_system_items_b.inventory_carry_penalty
, mtl_system_items_b.operation_slack_penalty
, mtl_system_items_b.financing_allowed_flag
, mtl_system_items_b.eam_item_type       
, mtl_system_items_b.eam_activity_type_code
, mtl_system_items_b.eam_activity_cause_code
, mtl_system_items_b.eam_act_notification_flag
, mtl_system_items_b.eam_act_shutdown_status
, mtl_system_items_b.dual_uom_control    
, mtl_system_items_b.secondary_uom_code  
, mtl_system_items_b.dual_uom_deviation_high
, mtl_system_items_b.dual_uom_deviation_low
, mtl_system_items_b.contract_item_type_code
, mtl_system_items_b.subscription_depend_flag
, mtl_system_items_b.serv_req_enabled_code
, mtl_system_items_b.serv_billing_enabled_flag
, mtl_system_items_b.serv_importance_level
, mtl_system_items_b.planned_inv_point_flag
, mtl_system_items_b.lot_translate_enabled
, mtl_system_items_b.default_so_source_type
, mtl_system_items_b.create_supply_flag  
, mtl_system_items_b.substitution_window_code
, mtl_system_items_b.substitution_window_days
, mtl_system_items_b.ib_item_instance_class
, mtl_system_items_b.config_model_type   
, mtl_system_items_b.lot_substitution_enabled
, mtl_system_items_b.minimum_license_quantity
, mtl_system_items_b.eam_activity_source_code
, mtl_system_items_b.lifecycle_id        
, mtl_system_items_b.current_phase_id    
, mtl_system_items_b.object_version_number
, mtl_system_items_b.last_update_date_h  
, mtl_system_items_b.creation_date_h     
, mtl_system_items_b.start_date_active_h 
, mtl_system_items_b.end_date_active_h   
, mtl_system_items_b.engineering_date_h  
, mtl_system_items_b.program_update_date_h
, mtl_system_items_b.wh_update_date_h    
, mtl_system_items_b.storeday            
from erp_mexico_sz.MTL_SYSTEM_ITEMS_B;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_categoria_materiales AS select
mtl_item_categories.inventory_item_id 
,mtl_item_categories.organization_id 
,mtl_item_categories.category_set_id 
,mtl_item_categories.category_id 
,mtl_item_categories.last_update_date 
,mtl_item_categories.last_update_date_h 
,mtl_item_categories.last_updated_by 
,mtl_item_categories.creation_date 
,mtl_item_categories.creation_date_h 
,mtl_item_categories.created_by 
,mtl_item_categories.last_update_login 
,mtl_item_categories.request_id 
,mtl_item_categories.program_application_id 
,mtl_item_categories.program_id 
,mtl_item_categories.program_update_date 
,mtl_item_categories.program_update_date_h 
,mtl_item_categories.wh_update_date 
,mtl_item_categories.wh_update_date_h
,mtl_item_categories.storeday 
from erp_mexico_sz.MTL_ITEM_CATEGORIES;



CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_unidad_medida AS SELECT   mtl_units_of_measure_tl.unit_of_measure       
, mtl_units_of_measure_tl.uom_code              
, mtl_units_of_measure_tl.uom_class             
, mtl_units_of_measure_tl.base_uom_flag         
, mtl_units_of_measure_tl.unit_of_measure_tl  
,regexp_replace(substr(mtl_units_of_measure_tl.last_update_date, 3, 10), '-', '/')
, mtl_units_of_measure_tl.last_update_date_h    
, mtl_units_of_measure_tl.last_updated_by       
, mtl_units_of_measure_tl.created_by       
,regexp_replace(substr(mtl_units_of_measure_tl.creation_date, 3, 10), '-', '/')        
, mtl_units_of_measure_tl.creation_date_h       
, mtl_units_of_measure_tl.last_update_login  
,regexp_replace(substr(mtl_units_of_measure_tl.disable_date, 3, 10), '-', '/')      
, mtl_units_of_measure_tl.disable_date_h        
, mtl_units_of_measure_tl.description           
, mtl_units_of_measure_tl.languages             
, mtl_units_of_measure_tl.source_lang           
, mtl_units_of_measure_tl.attribute_category    
, mtl_units_of_measure_tl.attribute1            
, mtl_units_of_measure_tl.attribute2            
, mtl_units_of_measure_tl.attribute3            
, mtl_units_of_measure_tl.attribute4            
, mtl_units_of_measure_tl.attribute5            
, mtl_units_of_measure_tl.attribute6            
, mtl_units_of_measure_tl.attribute7            
, mtl_units_of_measure_tl.attribute8            
, mtl_units_of_measure_tl.attribute9            
, mtl_units_of_measure_tl.attribute10           
, mtl_units_of_measure_tl.attribute11           
, mtl_units_of_measure_tl.attribute12           
, mtl_units_of_measure_tl.attribute13           
, mtl_units_of_measure_tl.attribute14           
, mtl_units_of_measure_tl.attribute15           
, mtl_units_of_measure_tl.request_id            
, mtl_units_of_measure_tl.program_application_id
, mtl_units_of_measure_tl.program_id    
,regexp_replace(substr(mtl_units_of_measure_tl.program_update_date, 3, 10), '-', '/')           
, mtl_units_of_measure_tl.program_update_date_h, mtl_units_of_measure_tl.storeday
FROM erp_mexico_sz.MTL_UNITS_OF_MEASURE_TL;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_conversiones_udm AS SELECT   mtl_uom_conversions.unit_of_measure        
, mtl_uom_conversions.uom_code               
, mtl_uom_conversions.uom_class              
, mtl_uom_conversions.inventory_item_id      
, mtl_uom_conversions.conversion_rate        
, mtl_uom_conversions.default_conversion_flag
,regexp_replace(substr(mtl_uom_conversions.last_update_date, 3, 10), '-', '/') 
, mtl_uom_conversions.last_update_date_h     
, mtl_uom_conversions.last_updated_by                   
,regexp_replace(substr(mtl_uom_conversions.creation_date, 3, 10), '-', '/') 
, mtl_uom_conversions.creation_date_h        
, mtl_uom_conversions.created_by             
, mtl_uom_conversions.last_update_login      
,regexp_replace(substr(mtl_uom_conversions.disable_date, 3, 10), '-', '/') 
, mtl_uom_conversions.disable_date_h         
, mtl_uom_conversions.request_id             
, mtl_uom_conversions.program_application_id 
, mtl_uom_conversions.program_id             
,regexp_replace(substr(mtl_uom_conversions.program_update_date, 3, 10), '-', '/') 
, mtl_uom_conversions.program_update_date_h  
, mtl_uom_conversions.length                 
, mtl_uom_conversions.width                  
, mtl_uom_conversions.height                 
, mtl_uom_conversions.dimension_uom, mtl_uom_conversions.storeday          
FROM erp_mexico_sz.MTL_UOM_CONVERSIONS;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_hr_location AS select
hr_locations_all.location_id 
,hr_locations_all.location_code 
,hr_locations_all.business_group_id 
,hr_locations_all.description 
,hr_locations_all.ship_to_location_id 
,hr_locations_all.ship_to_site_flag 
,hr_locations_all.receiving_site_flag 
,hr_locations_all.bill_to_site_flag 
,hr_locations_all.in_organization_flag 
,hr_locations_all.office_site_flag 
,hr_locations_all.designated_receiver_id 
,hr_locations_all.inventory_organization_id 
,hr_locations_all.tax_name
,regexp_replace(hr_locations_all.inactive_date, '-', '/')
,hr_locations_all.inactive_date_h 
,hr_locations_all.style 
,hr_locations_all.address_line_1 
,hr_locations_all.address_line_2 
,hr_locations_all.address_line_3 
,hr_locations_all.town_or_city 
,hr_locations_all.country 
,hr_locations_all.postal_code 
,hr_locations_all.region_1 
,hr_locations_all.region_2 
,hr_locations_all.region_3 
,hr_locations_all.telephone_number_1 
,hr_locations_all.telephone_number_2 
,hr_locations_all.telephone_number_3 
,hr_locations_all.loc_information13 
,hr_locations_all.loc_information14 
,hr_locations_all.loc_information15 
,hr_locations_all.loc_information16 
,hr_locations_all.loc_information17 
,hr_locations_all.attribute_category 
,hr_locations_all.attribute1 
,regexp_replace(hr_locations_all.last_update_date, '-', '/')
,hr_locations_all.last_update_date_h 
,hr_locations_all.last_updated_by 
,hr_locations_all.last_update_login 
,hr_locations_all.created_by
,regexp_replace(hr_locations_all.creation_date, '-', '/')
,hr_locations_all.creation_date_h 
,hr_locations_all.entered_by 
,hr_locations_all.tp_header_id 
,hr_locations_all.ece_tp_location_code 
,hr_locations_all.derived_locale 
,hr_locations_all.legal_address_flag 
,hr_locations_all.timezone_code
,hr_locations_all.storeday
from  erp_mexico_sz.HR_LOCATIONS_ALL;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_bom_resources AS SELECT 
bom_resources.resource_id                   
,bom_resources.resource_code                 
,bom_resources.organization_id
,regexp_replace(bom_resources.last_update_date, '-', '/')                         
,bom_resources.last_update_date_h            
,bom_resources.last_updated_by               
,regexp_replace(bom_resources.creation_date, '-', '/')              
,bom_resources.creation_date_h               
,bom_resources.created_by                    
,bom_resources.last_update_login             
,bom_resources.description
,regexp_replace(bom_resources.disable_date, '-', '/')                   
,bom_resources.disable_date_h                
,bom_resources.cost_element_id               
,bom_resources.purchase_item_id              
,bom_resources.cost_code_type                
,bom_resources.functional_currency_flag      
,bom_resources.unit_of_measure               
,bom_resources.default_activity_id           
,bom_resources.resource_type                 
,bom_resources.autocharge_type               
,bom_resources.standard_rate_flag            
,bom_resources.default_basis_type            
,bom_resources.absorption_account            
,bom_resources.allow_costs_flag              
,bom_resources.rate_variance_account         
,bom_resources.expenditure_type              
,bom_resources.attribute_category            
,bom_resources.attribute1                    
,bom_resources.attribute2                    
,bom_resources.attribute3                    
,bom_resources.attribute4                    
,bom_resources.attribute5                    
,bom_resources.attribute6                    
,bom_resources.attribute7                    
,bom_resources.attribute8                    
,bom_resources.attribute9                    
,bom_resources.attribute10                   
,bom_resources.attribute11                   
,bom_resources.attribute12                   
,bom_resources.attribute13                   
,bom_resources.attribute14                   
,bom_resources.attribute15                   
,bom_resources.request_id                    
,bom_resources.program_application_id        
,bom_resources.program_id
,regexp_replace(bom_resources.program_update_date, '-', '/')                          
,bom_resources.program_update_date_h         
,bom_resources.batchable                     
,bom_resources.max_batch_capacity            
,bom_resources.min_batch_capacity            
,bom_resources.batch_capacity_uom            
,bom_resources.batch_window                  
,bom_resources.batch_window_uom              
,bom_resources.competence_id                 
,bom_resources.rating_level_id               
,bom_resources.qualification_type_id         
,bom_resources.billable_item_id
,bom_resources.storeday              
FROM erp_mexico_sz.BOM_RESOURCES;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_hr_organizacion AS select 
hr_all_organization_units.organization_id 
,regexp_replace(hr_all_organization_units.last_update_date, '-', '/')
,hr_all_organization_units.last_update_date_h 
,hr_all_organization_units.last_updated_by 
,hr_all_organization_units.created_by
,regexp_replace(hr_all_organization_units.creation_date, '-', '/')
,hr_all_organization_units.creation_date_h 
,hr_all_organization_units.business_group_id 
,hr_all_organization_units.cost_allocation_keyflex_id 
,hr_all_organization_units.location_id 
,hr_all_organization_units.soft_coding_keyflex_id 
,regexp_replace(hr_all_organization_units.date_from, '-', '/') 
,hr_all_organization_units.date_from_h 
,hr_all_organization_units.name 
,hr_all_organization_units.comments
,regexp_replace(hr_all_organization_units.date_to, '-', '/') 
,hr_all_organization_units.date_to_h 
,hr_all_organization_units.internal_external_flag 
,hr_all_organization_units.internal_address_line 
,hr_all_organization_units.type_x 
,hr_all_organization_units.request_id 
,hr_all_organization_units.program_application_id 
,hr_all_organization_units.program_id
,regexp_replace(hr_all_organization_units.program_update_date, '-', '/')
,hr_all_organization_units.program_update_date_h 
,hr_all_organization_units.attribute_category 
,hr_all_organization_units.attribute1 
,hr_all_organization_units.attribute2 
,hr_all_organization_units.attribute3 
,hr_all_organization_units.attribute4 
,hr_all_organization_units.party_id
,hr_all_organization_units.storeday
from erp_mexico_sz.hr_all_organization_units;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_referencia_cruzada_mat AS select
mtl_cross_references.inventory_item_id
,mtl_cross_references.organization_id
,mtl_cross_references.cross_reference_type
,case when substr(mtl_cross_references.cross_reference,length(mtl_cross_references.cross_reference),1) = '.'
then 
substr(mtl_cross_references.cross_reference,1,length(mtl_cross_references.cross_reference)-1)
else 
mtl_cross_references.cross_reference
end as cross_reference
,mtl_cross_references.last_update_date
,mtl_cross_references.last_update_date_h
,mtl_cross_references.last_updated_by
,mtl_cross_references.creation_date
,mtl_cross_references.creation_date_h
,mtl_cross_references.created_by
,mtl_cross_references.last_update_login
,mtl_cross_references.description
,mtl_cross_references.org_independent_flag
,mtl_cross_references.request_id
,mtl_cross_references.program_application_id
,mtl_cross_references.program_id
,mtl_cross_references.program_update_date
,mtl_cross_references.program_update_date_h
,mtl_cross_references.attribute1
,mtl_cross_references.attribute2
,mtl_cross_references.attribute3
,mtl_cross_references.attribute4
,mtl_cross_references.attribute5
,mtl_cross_references.attribute6
,mtl_cross_references.attribute7
,mtl_cross_references.attribute8
,mtl_cross_references.attribute9
,mtl_cross_references.attribute10
,mtl_cross_references.attribute11
,mtl_cross_references.attribute12
,mtl_cross_references.attribute13
,mtl_cross_references.attribute14
,mtl_cross_references.attribute15
,mtl_cross_references.attribute_category
,mtl_cross_references.storeday
from erp_mexico_sz.mtl_cross_references;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_wip_lineas AS select
wip_lines.line_id
,wip_lines.organization_id
,substr(wip_lines.last_update_date, 3, 10) as LAST_UPDATE_DATE
,wip_lines.last_update_date_h
,wip_lines.last_updated_by
,substr(wip_lines.creation_date, 3, 10) as CREATION_DATE
,wip_lines.creation_date_h
,wip_lines.created_by
,wip_lines.last_update_login
,wip_lines.line_code
,wip_lines.description
,substr(wip_lines.disable_date, 3, 10) as DISABLE_DATE
,wip_lines.disable_date_h
,wip_lines.minimum_rate
,wip_lines.maximum_rate
,wip_lines.fixed_throughput
,wip_lines.line_schedule_type
,wip_lines.scheduling_method_id
,wip_lines.start_time
,wip_lines.stop_time
,wip_lines.attribute_category
,wip_lines.atp_rule_id
,wip_lines.exception_set_name
,wip_lines.seq_direction
,wip_lines.seq_connect_flag
,wip_lines.seq_fix_sequence_type
,wip_lines.seq_fix_sequence_amount
,wip_lines.seq_default_rule_id
,wip_lines.seq_combine_schedule_flag
,wip_lines.storeday
from erp_mexico_sz.WIP_LINES;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_wip_flow_schedules AS select
wip_flow_schedules.scheduled_flag
, wip_flow_schedules.wip_entity_id
, wip_flow_schedules.organization_id
, wip_flow_schedules.last_update_date
, wip_flow_schedules.last_update_date_h
, wip_flow_schedules.last_updated_by
, wip_flow_schedules.creation_date
, wip_flow_schedules.creation_date_h
, wip_flow_schedules.created_by
, wip_flow_schedules.last_update_login
, wip_flow_schedules.request_id
, wip_flow_schedules.program_application_id
, wip_flow_schedules.program_id
, wip_flow_schedules.program_update_date
, wip_flow_schedules.program_update_date_h
, wip_flow_schedules.primary_item_id
, wip_flow_schedules.class_code
, wip_flow_schedules.scheduled_start_date
, wip_flow_schedules.scheduled_start_date_h
, wip_flow_schedules.date_closed
, wip_flow_schedules.date_closed_h
, wip_flow_schedules.planned_quantity
, wip_flow_schedules.quantity_completed
, wip_flow_schedules.mps_scheduled_completion_date
, wip_flow_schedules.mps_scheduled_completio_date_h
, wip_flow_schedules.mps_net_quantity
, wip_flow_schedules.bom_revision
, wip_flow_schedules.routing_revision
, wip_flow_schedules.bom_revision_date
, wip_flow_schedules.bom_revision_date_h
, wip_flow_schedules.routing_revision_date
, wip_flow_schedules.routing_revision_date_h
, wip_flow_schedules.alternate_bom_designator
, wip_flow_schedules.alternate_routing_designator
, wip_flow_schedules.completion_subinventory
, wip_flow_schedules.completion_locator_id
, wip_flow_schedules.material_account
, wip_flow_schedules.material_overhead_account
, wip_flow_schedules.resource_account
, wip_flow_schedules.outside_processing_account
, wip_flow_schedules.material_variance_account
, wip_flow_schedules.resource_variance_account
, wip_flow_schedules.outside_proc_variance_account
, wip_flow_schedules.std_cost_adjustment_account
, wip_flow_schedules.overhead_account
, wip_flow_schedules.overhead_variance_account
, wip_flow_schedules.demand_class
, wip_flow_schedules.scheduled_completion_date
, wip_flow_schedules.scheduled_completion_date_h
, wip_flow_schedules.schedule_group_id
, wip_flow_schedules.build_sequence
, wip_flow_schedules.line_id
, wip_flow_schedules.project_id
, wip_flow_schedules.task_id
, wip_flow_schedules.status
, wip_flow_schedules.schedule_number
, wip_flow_schedules.demand_source_header_id
, wip_flow_schedules.demand_source_line
, wip_flow_schedules.demand_source_delivery
, wip_flow_schedules.demand_source_type
, wip_flow_schedules.kanban_card_id
, wip_flow_schedules.end_item_unit_number
, wip_flow_schedules.quantity_scrapped
, wip_flow_schedules.current_line_operation
, wip_flow_schedules.synch_schedule_num
, wip_flow_schedules.synch_operation_seq_num
, wip_flow_schedules.roll_forwarded_flag
, wip_flow_schedules.allocated_flag
, wip_flow_schedules.storeday 
from erp_mexico_sz.WIP_FLOW_SCHEDULES;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_wip_repetitive_items AS select
wip_repetitive_items.wip_entity_id
, wip_repetitive_items.line_id
, wip_repetitive_items.organization_id
, wip_repetitive_items.last_update_date
, wip_repetitive_items.last_update_date_h
, wip_repetitive_items.last_updated_by
, wip_repetitive_items.creation_date
, wip_repetitive_items.creation_date_h
, wip_repetitive_items.created_by
, wip_repetitive_items.last_update_login
, wip_repetitive_items.request_id
, wip_repetitive_items.program_application_id
, wip_repetitive_items.program_id
, wip_repetitive_items.program_update_date
, wip_repetitive_items.program_update_date_h
, wip_repetitive_items.primary_item_id
, wip_repetitive_items.alternate_bom_designator
, wip_repetitive_items.alternate_routing_designator
, wip_repetitive_items.class_code
, wip_repetitive_items.wip_supply_type
, wip_repetitive_items.completion_subinventory
, wip_repetitive_items.completion_locator_id
, wip_repetitive_items.load_distribution_priority
, wip_repetitive_items.primary_line_flag
, wip_repetitive_items.production_line_rate
, wip_repetitive_items.overcompletion_tolerance_type
, wip_repetitive_items.overcompletion_tolerance_value
, wip_repetitive_items.attribute1
, wip_repetitive_items.attribute6
, wip_repetitive_items.storeday 
from erp_mexico_sz.wip_repetitive_items;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_onhand_diario AS select
date_sub(to_date(from_unixtime(unix_timestamp())),1) as fecha,
mtl_onhand_quantities_detail.organization_id,
mtl_onhand_quantities_detail.inventory_item_id,
mtl_onhand_quantities_detail.subinventory_code,
max(concat(mtl_onhand_quantities_detail.last_update_date,mtl_onhand_quantities_detail.last_update_date_h)) as max_last_update_date,
sum(mtl_onhand_quantities_detail.primary_transaction_quantity) as prim_transaction_quantity,
max(mtl_onhand_quantities_detail.storeday) as storeday
from  erp_mexico_sz.mtl_onhand_quantities_detail
group by 
date_sub(to_date(from_unixtime(unix_timestamp())),1),
mtl_onhand_quantities_detail.organization_id,
mtl_onhand_quantities_detail.inventory_item_id,
mtl_onhand_quantities_detail.subinventory_code;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_centro_costos AS select
vl.flex_value         as centrocostos_id
,vl.description        as nombrecentrocostos
,'oracle-mx-r11'       as sistemafuente
,'user'                as usuarioetl
,from_unixtime(unix_timestamp())        as fechacarga
,from_unixtime(unix_timestamp())        as fechacambio
,from_unixtime(unix_timestamp())        as storeday
from erp_mexico_sz.fnd_flex_values_vl vl
inner join erp_mexico_sz.fnd_id_flex_segments seg on cast(vl.flex_value_set_id as double) = seg.flex_value_set_id
inner join erp_mexico_sz.gl_sets_of_books bk on seg.id_flex_num = bk.chart_of_accounts_id
where lower(seg.id_flex_code) = 'gl#'  
and lower(seg.enabled_flag) = 'y' 
and seg.application_id = 101  
and lower(seg.application_column_name) = 'segment6' 
and seg.flex_value_set_id = 1004593
and lower(vl.flex_value) != 't'
group by vl.flex_value, vl.description, 'oracle-mx-r11', 'user', from_unixtime(unix_timestamp()), from_unixtime(unix_timestamp()), from_unixtime(unix_timestamp());


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.gl_lookups AS SELECT
fnd_lookup_values_vl.lookup_type,fnd_lookup_values_vl.lookup_code,fnd_lookup_values_vl.view_application_id,fnd_lookup_values_vl.meaning,fnd_lookup_values_vl.description,fnd_lookup_values_vl.enabled_flag,fnd_lookup_values_vl.start_date_active,fnd_lookup_values_vl.end_date_active,fnd_lookup_values_vl.created_by
,fnd_lookup_values_vl.creation_date,fnd_lookup_values_vl.last_updated_by,fnd_lookup_values_vl.last_update_login,fnd_lookup_values_vl.last_update_date,fnd_lookup_values_vl.territory_code,fnd_lookup_values_vl.attribute_category,fnd_lookup_values_vl.attribute1
,fnd_lookup_values_vl.attribute2,fnd_lookup_values_vl.attribute3,fnd_lookup_values_vl.attribute4,fnd_lookup_values_vl.attribute5,fnd_lookup_values_vl.attribute6,fnd_lookup_values_vl.attribute7,fnd_lookup_values_vl.attribute8,fnd_lookup_values_vl.attribute9,fnd_lookup_values_vl.attribute10,fnd_lookup_values_vl.attribute11
,fnd_lookup_values_vl.attribute12,fnd_lookup_values_vl.attribute13,fnd_lookup_values_vl.attribute14,fnd_lookup_values_vl.attribute15,fnd_lookup_values_vl.tag 
FROM erp_mexico_sz.fnd_lookup_values_vl 
WHERE fnd_lookup_values_vl.view_application_id = 101;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_a_reporte_financiero_dtl AS SELECT 
CAST(rx.axis_set_id AS INT) AS Reporte_ID        
,CAST(rx.axis_seq AS INT) AS Linea_ID          
,rx.sign AS Signo             
,rx.segment1_low AS I_EntidadLegal_ID 
,rx.segment2_low AS I_AreaNegocio_ID  
,rx.segment3_low AS I_CuentaNatural_ID
,rx.segment4_low AS I_AnalisisLocal_ID
,rx.segment5_low AS I_Marca_ID        
,rx.segment6_low AS I_CentroCostos_ID 
,rx.segment7_low AS I_Intercompania_ID
,rx.segment8_low AS I_Segment8_ID     
,rx.segment9_low AS I_Segment9_ID     
,rx.segment10_low AS I_Segment10_ID    
,rx.segment1_high AS F_EntidadLegal_ID 
,rx.segment2_high AS F_AreaNegocio_ID  
,rx.segment3_high AS F_CuentaNatural_ID
,rx.segment4_high AS F_AnalisisLocal_ID
,rx.segment5_high AS F_Marca_ID        
,rx.segment6_high AS F_CentroCostos_ID 
,rx.segment7_high AS F_Intercompania_ID
,rx.segment8_high AS F_Segment8_ID     
,rx.segment9_high AS F_Segment9_ID     
,rx.segment10_high AS F_Segment10_ID
FROM erp_mexico_sz.RG_REPORT_AXIS_CONTENTS RX 
LEFT OUTER JOIN erp_mexico_sz.FND_LOOKUPS LK1 
ON lk1.lookup_type='YES_NO' AND lk1.lookup_code=rx.range_mode  
LEFT OUTER JOIN gb_mdl_mexico_costoproducir_views.GL_LOOKUPS GLLK 
ON gllk.lookup_type='DR_CR_NET_CODE' AND gllk.lookup_code=rx.dr_cr_net_code    
LEFT OUTER JOIN erp_mexico_sz.GL_SETS_OF_BOOKS GLSOB 
ON glsob.set_of_books_id = rx.set_of_books_id;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_a_reporte_financiero AS SELECT 
CAST(rx.axis_set_id AS INT) AS Reporte_ID
,CAST(rx.axis_seq AS INT) AS Linea_ID
,rx.axis_name AS CodigoConcepto
,rx.description AS NombreConcepto
,rx.display_flag AS DisplayFlag
FROM erp_mexico_sz.RG_REPORT_AXES RX
LEFT OUTER JOIN erp_mexico_sz.RG_REPORT_STANDARD_AXES STD  ON std.standard_axis_id = rx.standard_axis_id 
LEFT OUTER JOIN erp_mexico_sz.fnd_lookups LK1 ON lk1.lookup_code = rx.calculation_precedence_flag AND lk1.lookup_type = 'YES_NO' 
LEFT OUTER JOIN erp_mexico_sz.fnd_lookups LK2 ON lk2.lookup_code = rx.display_flag AND lk2.lookup_type = 'YES_NO' 
LEFT OUTER JOIN erp_mexico_sz.rg_lookups  RGLK1 ON rglk1.lookup_code = CAST(rx.display_units AS STRING) AND rglk1.lookup_type = 'DISPLAY_UNITS' 
LEFT OUTER JOIN erp_mexico_sz.fnd_lookups LK3 ON lk3.lookup_code = rx.display_zero_amount_flag AND lk3.lookup_type = 'YES_NO' 
LEFT OUTER JOIN erp_mexico_sz.fnd_lookups LK4 ON lk4.lookup_code = rx.change_sign_flag AND lk4.lookup_type = 'YES_NO' 
LEFT OUTER JOIN erp_mexico_sz.fnd_lookups LK5 ON lk5.lookup_code = rx.change_variance_sign_flag AND lk5.lookup_type = 'YES_NO' 
LEFT OUTER JOIN erp_mexico_sz.rg_lookups RGLK2 ON rglk2.lookup_code = CAST( rx.display_level AS STRING) AND rglk2.lookup_type = 'GL_DISPLAY_LEVEL' 
LEFT OUTER JOIN erp_mexico_sz.fnd_lookups LK6 ON lk6.lookup_code = rx.page_break_before_flag AND lk6.lookup_type = 'YES_NO' 
LEFT OUTER JOIN erp_mexico_sz.fnd_lookups LK7 ON lk7.lookup_code = rx.page_break_after_flag AND lk7.lookup_type = 'YES_NO';


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_gl_je_headers AS select
gl_je_headers.je_header_id                  
,gl_je_headers.last_update_date              
,gl_je_headers.last_update_date_h            
,gl_je_headers.last_updated_by               
,gl_je_headers.set_of_books_id               
,gl_je_headers.je_category                   
,gl_je_headers.je_source                     
,gl_je_headers.period_name                   
,gl_je_headers.name                          
,gl_je_headers.currency_code                 
,gl_je_headers.status                        
,gl_je_headers.date_created                  
,gl_je_headers.date_created_h                
,gl_je_headers.accrual_rev_flag              
,gl_je_headers.multi_bal_seg_flag            
,gl_je_headers.actual_flag                   
,gl_je_headers.default_effective_date        
,gl_je_headers.default_effective_date_h      
,gl_je_headers.tax_status_code               
,gl_je_headers.conversion_flag               
,gl_je_headers.creation_date                 
,gl_je_headers.creation_date_h               
,gl_je_headers.created_by                    
,gl_je_headers.last_update_login             
,gl_je_headers.encumbrance_type_id           
,gl_je_headers.budget_version_id             
,gl_je_headers.balanced_je_flag              
,gl_je_headers.balancing_segment_value       
,gl_je_headers.je_batch_id                   
,gl_je_headers.from_recurring_header_id      
,gl_je_headers.unique_date                   
,gl_je_headers.earliest_postable_date        
,gl_je_headers.earliest_postable_date_h      
,gl_je_headers.posted_date                   
,gl_je_headers.posted_date_h                 
,gl_je_headers.accrual_rev_effective_date    
,gl_je_headers.accrual_rev_effective_date_h  
,gl_je_headers.accrual_rev_period_name       
,gl_je_headers.accrual_rev_status            
,gl_je_headers.accrual_rev_je_header_id      
,gl_je_headers.accrual_rev_change_sign_flag  
,gl_je_headers.description                   
,gl_je_headers.control_total                 
,gl_je_headers.running_total_dr              
,gl_je_headers.running_total_cr              
,gl_je_headers.running_total_accounted_dr    
,gl_je_headers.running_total_accounted_cr    
,gl_je_headers.currency_conversion_rate      
,gl_je_headers.currency_conversion_type      
,gl_je_headers.currency_conversion_date      
,gl_je_headers.currency_conversion_date_h    
,gl_je_headers.external_reference            
,gl_je_headers.parent_je_header_id           
,gl_je_headers.reversed_je_header_id         
,gl_je_headers.originating_bal_seg_value     
,gl_je_headers.intercompany_mode             
,gl_je_headers.dr_bal_seg_value              
,gl_je_headers.cr_bal_seg_value              
,gl_je_headers.attribute1                    
,gl_je_headers.attribute2                    
,gl_je_headers.attribute3                    
,gl_je_headers.attribute4                    
,gl_je_headers.attribute5                    
,gl_je_headers.attribute6                    
,gl_je_headers.attribute7                    
,gl_je_headers.attribute8                    
,gl_je_headers.attribute9                    
,gl_je_headers.attribute10                   
,gl_je_headers.context                       
,gl_je_headers.global_attribute_category     
,gl_je_headers.global_attribute1             
,gl_je_headers.global_attribute2             
,gl_je_headers.global_attribute3             
,gl_je_headers.global_attribute4             
,gl_je_headers.global_attribute5             
,gl_je_headers.global_attribute6             
,gl_je_headers.global_attribute7             
,gl_je_headers.global_attribute8             
,gl_je_headers.global_attribute9             
,gl_je_headers.global_attribute10            
,gl_je_headers.ussgl_transaction_code        
,gl_je_headers.context2                      
,gl_je_headers.doc_sequence_id               
,gl_je_headers.doc_sequence_value            
,gl_je_headers.jgzz_recon_context            
,gl_je_headers.jgzz_recon_ref
,gl_je_headers.storeday                
from erp_mexico_sz.gl_je_headers;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_gl_je_lines AS select 
      gl_je_lines.je_header_id                  
     ,gl_je_lines.je_line_num                   
     ,gl_je_lines.last_update_date              
     ,gl_je_lines.last_update_date_h            
     ,gl_je_lines.last_updated_by               
     ,gl_je_lines.set_of_books_id               
     ,gl_je_lines.code_combination_id           
     ,gl_je_lines.period_name                   
     ,gl_je_lines.effective_date                
     ,gl_je_lines.effective_date_h              
     ,gl_je_lines.status                        
     ,gl_je_lines.creation_date                 
     ,gl_je_lines.creation_date_h               
     ,gl_je_lines.created_by                    
     ,gl_je_lines.last_update_login             
     ,gl_je_lines.entered_dr                    
     ,gl_je_lines.entered_cr                    
     ,gl_je_lines.accounted_dr                  
     ,gl_je_lines.accounted_cr                  
     ,gl_je_lines.description                   
     ,gl_je_lines.line_type_code                
     ,gl_je_lines.reference_1                   
     ,gl_je_lines.reference_2                   
     ,gl_je_lines.reference_3                   
     ,gl_je_lines.reference_4                   
     ,gl_je_lines.reference_5                   
     ,gl_je_lines.attribute1                    
     ,gl_je_lines.attribute2                    
     ,gl_je_lines.attribute3                    
     ,gl_je_lines.attribute4                    
     ,gl_je_lines.attribute5                    
     ,gl_je_lines.attribute6                    
     ,gl_je_lines.attribute7                    
     ,gl_je_lines.attribute8                    
     ,gl_je_lines.attribute9                    
     ,gl_je_lines.attribute10                   
     ,gl_je_lines.attribute11                   
     ,gl_je_lines.attribute12                   
     ,gl_je_lines.attribute13                   
     ,gl_je_lines.attribute14                   
     ,gl_je_lines.attribute15                   
     ,gl_je_lines.attribute16                   
     ,gl_je_lines.attribute17                   
     ,gl_je_lines.attribute18                   
     ,gl_je_lines.attribute19                   
     ,gl_je_lines.attribute20                   
     ,gl_je_lines.context                       
     ,gl_je_lines.context2                      
     ,gl_je_lines.invoice_date                  
     ,gl_je_lines.invoice_date_h                
     ,gl_je_lines.tax_code                      
     ,gl_je_lines.invoice_identifier            
     ,gl_je_lines.invoice_amount                
     ,gl_je_lines.no1                           
     ,gl_je_lines.stat_amount                   
     ,gl_je_lines.ignore_rate_flag              
     ,gl_je_lines.context3                      
     ,gl_je_lines.ussgl_transaction_code        
     ,gl_je_lines.subledger_doc_sequence_id     
     ,gl_je_lines.context4                      
     ,gl_je_lines.subledger_doc_sequence_value  
     ,gl_je_lines.reference_6                   
     ,gl_je_lines.reference_7                   
     ,gl_je_lines.gl_sl_link_id                 
     ,gl_je_lines.gl_sl_link_table              
     ,gl_je_lines.reference_8                   
     ,gl_je_lines.reference_9                   
     ,gl_je_lines.reference_10                  
     ,gl_je_lines.global_attribute_category     
     ,gl_je_lines.global_attribute1             
     ,gl_je_lines.global_attribute2             
     ,gl_je_lines.global_attribute3             
     ,gl_je_lines.global_attribute4             
     ,gl_je_lines.global_attribute5             
     ,gl_je_lines.global_attribute6             
     ,gl_je_lines.global_attribute7             
     ,gl_je_lines.global_attribute8             
     ,gl_je_lines.global_attribute9             
     ,gl_je_lines.global_attribute10            
     ,gl_je_lines.jgzz_recon_status             
     ,gl_je_lines.jgzz_recon_date               
     ,gl_je_lines.jgzz_recon_date_h             
     ,gl_je_lines.jgzz_recon_id                 
     ,gl_je_lines.jgzz_recon_ref                
     ,gl_je_lines.jgzz_recon_context            
     ,gl_je_lines.taxable_line_flag             
     ,gl_je_lines.tax_type_code                 
     ,gl_je_lines.tax_code_id                   
     ,gl_je_lines.tax_rounding_rule_code        
     ,gl_je_lines.amount_includes_tax_flag      
     ,gl_je_lines.tax_document_identifier       
     ,gl_je_lines.tax_document_date             
     ,gl_je_lines.tax_document_date_h           
     ,gl_je_lines.tax_customer_name             
     ,gl_je_lines.tax_customer_reference        
     ,gl_je_lines.tax_registration_number       
     ,gl_je_lines.tax_line_flag                 
     ,gl_je_lines.tax_group_id
     ,gl_je_lines.storeday                  
from erp_mexico_sz.gl_je_lines;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.gl_balances AS SELECT glb.set_of_books_id, glb.code_combination_id
, glb.currency_code, glb.period_name
, glb.actual_flag, glb.last_update_date
, glb.last_update_date_h, glb.last_updated_by
, glb.budget_version_id, glb.encumbrance_type_id, glb.translated_flag
, glb.revaluation_status, glb.period_type
, glb.period_year, glb.period_num
, glb.period_net_dr, glb.period_net_cr
, glb.period_to_date_adb, glb.quarter_to_date_dr
, glb.quarter_to_date_cr, glb.quarter_to_date_adb
, glb.year_to_date_adb, glb.project_to_date_dr
, glb.project_to_date_cr, glb.project_to_date_adb
, glb.begin_balance_dr, glb.begin_balance_cr
, glb.period_net_dr_beq, glb.period_net_cr_beq
, glb.begin_balance_dr_beq, glb.begin_balance_cr_beq
, glb.template_id, glb.encumbrance_doc_id
, glb.encumbrance_line_num, glb.quarter_to_date_dr_beq
, glb.quarter_to_date_cr_beq, glb.project_to_date_dr_beq
, glb.project_to_date_cr_beq, glb.storeday 
FROM erp_mexico_sz.GL_BALANCES glb;


create view IF NOT EXISTS gb_mdl_mexico_costoproducir_views.gl_code_combinations_fa_brio_tmp
as
select 
max(account_type) as account_type
,max(allocation_create_flag) as allocation_create_flag
,max(attribute1) as attribute1
,max(attribute10) as attribute10
,max(attribute2) as attribute2
,max(attribute3) as attribute3
,max(attribute4) as attribute4
,max(attribute5) as attribute5
,max(attribute6) as attribute6
,max(attribute7) as attribute7
,max(attribute8) as attribute8
,max(attribute9) as attribute9
,chart_of_accounts_id as chart_of_accounts_id
,code_combination_id as code_combination_id
,max(company_cost_center_org_id) as company_cost_center_org_id
,max(context) as context
,max(description) as description
,max(detail_budgeting_allowed_flag) as detail_budgeting_allowed_flag
,max(detail_posting_allowed_flag) as detail_posting_allowed_flag
,max(enabled_flag) as enabled_flag
,max(end_date_active) as end_date_active
,max(igi_balanced_budget_flag) as igi_balanced_budget_flag
,max(jgzz_recon_context) as jgzz_recon_context
,max(jgzz_recon_flag) as jgzz_recon_flag
,max(last_update_date) as last_update_date
,max(last_updated_by) as last_updated_by
,max(preserve_flag) as preserve_flag
,max(reference1) as reference1
,max(reference2) as reference2
,max(reference3) as reference3
,max(reference4) as reference4
,max(reference5) as reference5
,max(refresh_flag) as refresh_flag
,max(revaluation_id) as revaluation_id
,max(segment_attribute1) as segment_attribute1
,max(segment_attribute10) as segment_attribute10
,max(segment_attribute11) as segment_attribute11
,max(segment_attribute12) as segment_attribute12
,max(segment_attribute13) as segment_attribute13
,max(segment_attribute14) as segment_attribute14
,max(segment_attribute15) as segment_attribute15
,max(segment_attribute16) as segment_attribute16
,max(segment_attribute17) as segment_attribute17
,max(segment_attribute18) as segment_attribute18
,max(segment_attribute19) as segment_attribute19
,max(segment_attribute2) as segment_attribute2
,max(segment_attribute20) as segment_attribute20
,max(segment_attribute21) as segment_attribute21
,max(segment_attribute22) as segment_attribute22
,max(segment_attribute23) as segment_attribute23
,max(segment_attribute24) as segment_attribute24
,max(segment_attribute25) as segment_attribute25
,max(segment_attribute26) as segment_attribute26
,max(segment_attribute27) as segment_attribute27
,max(segment_attribute28) as segment_attribute28
,max(segment_attribute29) as segment_attribute29
,max(segment_attribute3) as segment_attribute3
,max(segment_attribute30) as segment_attribute30
,max(segment_attribute31) as segment_attribute31
,max(segment_attribute32) as segment_attribute32
,max(segment_attribute33) as segment_attribute33
,max(segment_attribute34) as segment_attribute34
,max(segment_attribute35) as segment_attribute35
,max(segment_attribute36) as segment_attribute36
,max(segment_attribute37) as segment_attribute37
,max(segment_attribute38) as segment_attribute38
,max(segment_attribute39) as segment_attribute39
,max(segment_attribute4) as segment_attribute4
,max(segment_attribute40) as segment_attribute40
,max(segment_attribute41) as segment_attribute41
,max(segment_attribute42) as segment_attribute42
,max(segment_attribute5) as segment_attribute5
,max(segment_attribute6) as segment_attribute6
,max(segment_attribute7) as segment_attribute7
,max(segment_attribute8) as segment_attribute8
,max(segment_attribute9) as segment_attribute9
,max(segment1) as segment1
,max(segment10) as segment10
,max(segment11) as segment11
,max(segment12) as segment12
,max(segment13) as segment13
,max(segment14) as segment14
,max(segment15) as segment15
,max(segment16) as segment16
,max(segment17) as segment17
,max(segment18) as segment18
,max(segment19) as segment19
,max(segment2) as segment2
,max(segment20) as segment20
,max(segment21) as segment21
,max(segment22) as segment22
,max(segment23) as segment23
,max(segment24) as segment24
,max(segment25) as segment25
,max(segment26) as segment26
,max(segment27) as segment27
,max(segment28) as segment28
,max(segment29) as segment29
,max(segment3) as segment3
,max(segment30) as segment30
,max(segment4) as segment4
,max(segment5) as segment5
,max(segment6) as segment6
,max(segment7) as segment7
,max(segment8) as segment8
,max(segment9) as segment9
,max(start_date_active) as start_date_active
,max(summary_flag) as summary_flag
,max(template_id) as template_id
,max(storeday) as storeday
from erp_mexico_sz.gl_code_combinations_fa_brio
group by chart_of_accounts_id,code_combination_id;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_cst_tipo_costo_material AS select
cst_item_cost_type_v.inventory_item_id
,cst_item_cost_type_v.item_number                                   
,cst_item_cost_type_v.padded_item_number                            
,cst_item_cost_type_v.description                                   
,cst_item_cost_type_v.primary_uom_code                              
,cst_item_cost_type_v.organization_id                               
,cst_item_cost_type_v.cost_type_id                                  
,cst_item_cost_type_v.cost_type                                     
,cst_item_cost_type_v.cost_type_description                         
,cst_item_cost_type_v.allow_updates_flag                            
,cst_item_cost_type_v.frozen_standard_flag                          
,cst_item_cost_type_v.default_cost_type_id                          
,cst_item_cost_type_v.default_cost_type 
,regexp_replace(cst_item_cost_type_v.last_update_date, '-', '/')                                              
,cst_item_cost_type_v.last_update_date_h                            
,cst_item_cost_type_v.last_updated_by  
,regexp_replace(cst_item_cost_type_v.creation_date, '-', '/')                               
,cst_item_cost_type_v.creation_date_h                               
,cst_item_cost_type_v.created_by                                    
,cst_item_cost_type_v.last_update_login                             
,cst_item_cost_type_v.inventory_asset                               
,cst_item_cost_type_v.lot_size                                      
,cst_item_cost_type_v.based_on_rollup                               
,cst_item_cost_type_v.shrinkage_rate                                
,cst_item_cost_type_v.defaulted_flag                                
,cst_item_cost_type_v.item_cost                                     
,cst_item_cost_type_v.material_cost                                 
,cst_item_cost_type_v.material_overhead_cost                        
,cst_item_cost_type_v.resource_cost                                 
,cst_item_cost_type_v.outside_processing_cost                       
,cst_item_cost_type_v.overhead_cost                                 
,cst_item_cost_type_v.planning_make_buy_code                        
,cst_item_cost_type_v.default_include_in_rollup_flag                
,cst_item_cost_type_v.cost_of_sales_account                         
,cst_item_cost_type_v.sales_account                                 
,cst_item_cost_type_v.category_id                                   
,cst_item_cost_type_v.attribute_category                            
,cst_item_cost_type_v.request_id 
,cst_item_cost_type_v.program_application_id 
,cst_item_cost_type_v.program_id 
,regexp_replace(cst_item_cost_type_v.program_update_date, '-', '/') 
,cst_item_cost_type_v.program_update_date_h
,cst_item_cost_type_v.storeday
from erp_mexico_sz.CST_ITEM_COST_TYPE_V;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_fnd_lookup_values AS SELECT 
fnd_lookup_values.lookup_type
,fnd_lookup_values.language
,fnd_lookup_values.lookup_code
,fnd_lookup_values.meaning
,fnd_lookup_values.description
,fnd_lookup_values.enabled_flag
,fnd_lookup_values.start_date_active
,fnd_lookup_values.start_date_active_h
,fnd_lookup_values.end_date_active
,fnd_lookup_values.end_date_active_h
,fnd_lookup_values.created_by
,fnd_lookup_values.creation_date
,fnd_lookup_values.creation_date_h
,fnd_lookup_values.last_updated_by
,fnd_lookup_values.last_update_login
,fnd_lookup_values.last_update_date
,fnd_lookup_values.last_update_date_h
,fnd_lookup_values.source_lang
,fnd_lookup_values.security_group_id
,fnd_lookup_values.view_application_id
,fnd_lookup_values.territory_code
,fnd_lookup_values.attribute_category
,fnd_lookup_values.attribute1
,fnd_lookup_values.attribute2
,fnd_lookup_values.attribute3
,fnd_lookup_values.attribute4
,fnd_lookup_values.attribute5
,fnd_lookup_values.attribute6
,fnd_lookup_values.attribute7
,fnd_lookup_values.attribute8
,fnd_lookup_values.attribute9
,fnd_lookup_values.attribute10
,fnd_lookup_values.attribute11
,fnd_lookup_values.attribute12
,fnd_lookup_values.attribute13
,fnd_lookup_values.attribute14
,fnd_lookup_values.attribute15
,fnd_lookup_values.tag
,fnd_lookup_values.storeday
FROM erp_mexico_sz.FND_LOOKUP_VALUES;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_conjunto_categoria AS SELECT  mtl_category_sets_tl.category_set_id   
,mtl_category_sets_tl.language       
,mtl_category_sets_tl.source_lang       
,mtl_category_sets_tl.category_set_name 
,mtl_category_sets_tl.description      
,regexp_replace(substr(mtl_category_sets_tl.last_update_date, 3, 10), '-', '/')   
,mtl_category_sets_tl.last_update_date_h
,mtl_category_sets_tl.last_updated_by   
,regexp_replace(substr(mtl_category_sets_tl.creation_date, 3, 10), '-', '/')  
,mtl_category_sets_tl.creation_date_h   
,mtl_category_sets_tl.created_by        
,mtl_category_sets_tl.last_update_login, mtl_category_sets_tl.storeday
FROM erp_mexico_sz.MTL_CATEGORY_SETS_TL;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_parameters AS SELECT
mtl_parameters.organization_id               
,mtl_parameters.last_update_date              
,mtl_parameters.last_update_date_h            
,mtl_parameters.last_updated_by               
,mtl_parameters.creation_date                 
,mtl_parameters.creation_date_h               
,mtl_parameters.created_by                    
,mtl_parameters.last_update_login             
,mtl_parameters.organization_code             
,mtl_parameters.master_organization_id        
,mtl_parameters.primary_cost_method           
,mtl_parameters.cost_organization_id          
,mtl_parameters.default_material_cost_id      
,mtl_parameters.calendar_exception_set_id     
,mtl_parameters.calendar_code                 
,mtl_parameters.general_ledger_update_code    
,mtl_parameters.default_atp_rule_id           
,mtl_parameters.default_picking_rule_id       
,mtl_parameters.default_locator_order_value   
,mtl_parameters.default_subinv_order_value    
,mtl_parameters.negative_inv_receipt_code     
,mtl_parameters.stock_locator_control_code    
,mtl_parameters.material_account              
,mtl_parameters.material_overhead_account     
,mtl_parameters.matl_ovhd_absorption_acct     
,mtl_parameters.resource_account              
,mtl_parameters.purchase_price_var_account    
,mtl_parameters.ap_accrual_account            
,mtl_parameters.overhead_account              
,mtl_parameters.outside_processing_account    
,mtl_parameters.intransit_inv_account         
,mtl_parameters.interorg_receivables_account  
,mtl_parameters.interorg_price_var_account    
,mtl_parameters.interorg_payables_account     
,mtl_parameters.cost_of_sales_account         
,mtl_parameters.encumbrance_account           
,mtl_parameters.project_cost_account          
,mtl_parameters.interorg_transfer_cr_account  
,mtl_parameters.matl_interorg_transfer_code   
,mtl_parameters.interorg_trnsfr_charge_percent
,mtl_parameters.source_organization_id        
,mtl_parameters.source_subinventory           
,mtl_parameters.source_type                   
,mtl_parameters.org_max_weight                
,mtl_parameters.org_max_weight_uom_code       
,mtl_parameters.org_max_volume                
,mtl_parameters.org_max_volume_uom_code       
,mtl_parameters.serial_number_type            
,mtl_parameters.auto_serial_alpha_prefix      
,mtl_parameters.start_auto_serial_number      
,mtl_parameters.auto_lot_alpha_prefix         
,mtl_parameters.lot_number_uniqueness         
,mtl_parameters.lot_number_generation         
,mtl_parameters.lot_number_zero_padding       
,mtl_parameters.lot_number_length             
,mtl_parameters.starting_revision             
,mtl_parameters.attribute_category            
,mtl_parameters.attribute1                    
,mtl_parameters.attribute2                    
,mtl_parameters.attribute3                    
,mtl_parameters.attribute4                    
,mtl_parameters.attribute5                    
,mtl_parameters.attribute6                    
,mtl_parameters.attribute7                    
,mtl_parameters.attribute8                    
,mtl_parameters.attribute9                    
,mtl_parameters.attribute10                   
,mtl_parameters.attribute11                   
,mtl_parameters.attribute12                   
,mtl_parameters.attribute13                   
,mtl_parameters.attribute14                   
,mtl_parameters.attribute15                   
,mtl_parameters.default_demand_class          
,mtl_parameters.encumbrance_reversal_flag     
,mtl_parameters.maintain_fifo_qty_stack_type  
,mtl_parameters.invoice_price_var_account     
,mtl_parameters.average_cost_var_account      
,mtl_parameters.sales_account                 
,mtl_parameters.expense_account               
,mtl_parameters.serial_number_generation      
,mtl_parameters.request_id                    
,mtl_parameters.program_application_id        
,mtl_parameters.program_id                    
,mtl_parameters.program_update_date           
,mtl_parameters.program_update_date_h         
,mtl_parameters.global_attribute_category     
,mtl_parameters.global_attribute1             
,mtl_parameters.global_attribute2             
,mtl_parameters.global_attribute3             
,mtl_parameters.global_attribute4             
,mtl_parameters.global_attribute5             
,mtl_parameters.global_attribute6             
,mtl_parameters.global_attribute7             
,mtl_parameters.global_attribute8             
,mtl_parameters.global_attribute9             
,mtl_parameters.global_attribute10            
,mtl_parameters.global_attribute11            
,mtl_parameters.global_attribute12            
,mtl_parameters.global_attribute13            
,mtl_parameters.global_attribute14            
,mtl_parameters.global_attribute15            
,mtl_parameters.global_attribute16            
,mtl_parameters.global_attribute17            
,mtl_parameters.global_attribute18            
,mtl_parameters.global_attribute19            
,mtl_parameters.global_attribute20            
,mtl_parameters.mat_ovhd_cost_type_id         
,mtl_parameters.project_reference_enabled     
,mtl_parameters.pm_cost_collection_enabled    
,mtl_parameters.project_control_level         
,mtl_parameters.avg_rates_cost_type_id        
,mtl_parameters.txn_approval_timeout_period   
,mtl_parameters.mo_source_required            
,mtl_parameters.mo_pick_confirm_required      
,mtl_parameters.mo_approval_timeout_action    
,mtl_parameters.borrpay_matl_var_account      
,mtl_parameters.borrpay_moh_var_account       
,mtl_parameters.borrpay_res_var_account       
,mtl_parameters.borrpay_osp_var_account       
,mtl_parameters.borrpay_ovh_var_account       
,mtl_parameters.process_enabled_flag          
,mtl_parameters.process_orgn_code             
,mtl_parameters.wsm_enabled_flag              
,mtl_parameters.default_cost_group_id         
,mtl_parameters.lpn_prefix                    
,mtl_parameters.lpn_suffix                    
,mtl_parameters.lpn_starting_number           
,mtl_parameters.wms_enabled_flag              
,mtl_parameters.pregen_putaway_tasks_flag     
,mtl_parameters.regeneration_interval         
,mtl_parameters.timezone_id                   
,mtl_parameters.max_picks_batch               
,mtl_parameters.default_wms_picking_rule_id   
,mtl_parameters.default_put_away_rule_id      
,mtl_parameters.default_task_assign_rule_id   
,mtl_parameters.default_label_comp_rule_id    
,mtl_parameters.default_carton_rule_id        
,mtl_parameters.default_cyc_count_header_id   
,mtl_parameters.crossdock_flag                
,mtl_parameters.cartonization_flag            
,mtl_parameters.cost_cutoff_date              
,mtl_parameters.cost_cutoff_date_h            
,mtl_parameters.enable_costing_by_category    
,mtl_parameters.cost_group_accounting         
,mtl_parameters.allocate_serial_flag          
,mtl_parameters.default_pick_task_type_id     
,mtl_parameters.default_cc_task_type_id       
,mtl_parameters.default_putaway_task_type_id  
,mtl_parameters.default_repl_task_type_id     
,mtl_parameters.eam_enabled_flag              
,mtl_parameters.maint_organization_id         
,mtl_parameters.prioritize_wip_jobs           
,mtl_parameters.default_crossdock_subinventory
,mtl_parameters.skip_task_waiting_minutes     
,mtl_parameters.qa_skipping_insp_flag         
,mtl_parameters.default_crossdock_locator_id  
,mtl_parameters.default_moxfer_task_type_id   
,mtl_parameters.default_moissue_task_type_id  
,mtl_parameters.default_matl_ovhd_cost_id     
,mtl_parameters.distributed_organization_flag 
,mtl_parameters.carrier_manifesting_flag      
,mtl_parameters.distribution_account_id       
,mtl_parameters.direct_shipping_allowed       
,mtl_parameters.default_pick_op_plan_id       
,mtl_parameters.max_clusters_allowed          
,mtl_parameters.consigned_flag                
,mtl_parameters.cartonize_sales_orders        
,mtl_parameters.cartonize_manufacturing       
,mtl_parameters.defer_logical_transactions    
,mtl_parameters.wip_overpick_enabled          
,mtl_parameters.ovpk_transfer_orders_enabled  
,mtl_parameters.total_lpn_length              
,mtl_parameters.ucc_128_suffix_flag           
,mtl_parameters.wcs_enabled                   
,mtl_parameters.auto_del_alloc_flag           
,mtl_parameters.rfid_verif_pcnt_threshold, mtl_parameters.storeday    
FROM erp_mexico_sz.MTL_PARAMETERS;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_tipo_fuente_trans AS select
mtl_txn_source_types.transaction_source_type_id
,regexp_replace(mtl_txn_source_types.last_update_date, '-', '/')
,mtl_txn_source_types.last_update_date_h 
,mtl_txn_source_types.last_updated_by 
,regexp_replace(mtl_txn_source_types.creation_date, '-', '/')
,mtl_txn_source_types.creation_date_h 
,mtl_txn_source_types.created_by 
,mtl_txn_source_types.transaction_source_type_name 
,mtl_txn_source_types.description 
,regexp_replace(mtl_txn_source_types.disable_date, '-', '/')
,mtl_txn_source_types.disable_date_h 
,mtl_txn_source_types.user_defined_flag 
,mtl_txn_source_types.validated_flag 
,mtl_txn_source_types.transaction_source_category 
,mtl_txn_source_types.transaction_source, mtl_txn_source_types.storeday
FROM erp_mexico_sz.MTL_TXN_SOURCE_TYPES;



CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_tipo_transacciones AS select
mtl_transaction_types.transaction_type_id
,regexp_replace(mtl_transaction_types.last_update_date, '-', '/') 
,mtl_transaction_types.last_update_date_h 
,mtl_transaction_types.last_updated_by 
,regexp_replace(mtl_transaction_types.creation_date, '-', '/')
,mtl_transaction_types.creation_date_h 
,mtl_transaction_types.created_by 
,mtl_transaction_types.transaction_type_name 
,mtl_transaction_types.description 
,mtl_transaction_types.transaction_action_id 
,mtl_transaction_types.transaction_source_type_id 
,mtl_transaction_types.shortage_msg_background_flag 
,mtl_transaction_types.shortage_msg_online_flag 
,regexp_replace(mtl_transaction_types.disable_date, '-', '/')
,mtl_transaction_types.disable_date_h 
,mtl_transaction_types.user_defined_flag 
,mtl_transaction_types.attribute1 
,mtl_transaction_types.attribute2 
,mtl_transaction_types.attribute_category 
,mtl_transaction_types.type_class 
,mtl_transaction_types.status_control_flag,mtl_transaction_types.storeday
from erp_mexico_sz.MTL_TRANSACTION_TYPES;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_mtl_item_subinventarios AS select
mtl_item_sub_inventories.inventory_item_id 
,mtl_item_sub_inventories.organization_id 
,mtl_item_sub_inventories.secondary_inventory
,regexp_replace(mtl_item_sub_inventories.last_update_date, '-', '/')  
,mtl_item_sub_inventories.last_update_date_h 
,mtl_item_sub_inventories.last_updated_by 
,regexp_replace(mtl_item_sub_inventories.creation_date, '-', '/')
,mtl_item_sub_inventories.creation_date_h 
,mtl_item_sub_inventories.created_by 
,mtl_item_sub_inventories.last_update_login 
,mtl_item_sub_inventories.primary_subinventory_flag 
,mtl_item_sub_inventories.picking_order 
,mtl_item_sub_inventories.min_minmax_quantity 
,mtl_item_sub_inventories.max_minmax_quantity 
,mtl_item_sub_inventories.inventory_planning_code 
,mtl_item_sub_inventories.fixed_lot_multiple 
,mtl_item_sub_inventories.minimum_order_quantity 
,mtl_item_sub_inventories.maximum_order_quantity 
,mtl_item_sub_inventories.source_type 
,mtl_item_sub_inventories.source_organization_id 
,mtl_item_sub_inventories.source_subinventory, mtl_item_sub_inventories.storeday 
from erp_mexico_sz.MTL_ITEM_SUB_INVENTORIES;





CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_m4sar_h_convenio_temp AS SELECT m4sar_h_convenio.id_organization,m4sar_h_convenio.std_id_hr,m4sar_h_convenio.std_or_hr_period,MAX(m4sar_h_convenio.dt_end) AS DT_END
FROM erp_mexico_sz.M4SAR_H_CONVENIO
WHERE m4sar_h_convenio.id_organization <> '072' 
GROUP BY m4sar_h_convenio.id_organization,m4sar_h_convenio.std_id_hr ,m4sar_h_convenio.std_or_hr_period;




CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_m4sar_h_hr_c_costo_temp AS SELECT m4sar_h_hr_c_costo.id_organization ,m4sar_h_hr_c_costo.sco_id_hr ,m4sar_h_hr_c_costo.sco_or_hr_role, MAX(m4sar_h_hr_c_costo.dt_end) AS DT_END
FROM erp_mexico_sz.M4SAR_H_HR_C_COSTO
WHERE m4sar_h_hr_c_costo.id_organization <> '072' 
GROUP BY m4sar_h_hr_c_costo.id_organization ,m4sar_h_hr_c_costo.sco_id_hr ,m4sar_h_hr_c_costo.sco_or_hr_role;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_m4scb_h_hr_conveni_temp_col AS SELECT m4scb_h_hr_conveni.id_organization,m4scb_h_hr_conveni.std_id_hr,m4scb_h_hr_conveni.std_or_hr_period,MAX(m4scb_h_hr_conveni.scb_dt_end) AS DT_END
FROM erp_mexico_sz.M4SCB_H_HR_CONVENI
WHERE m4scb_h_hr_conveni.id_organization = '072' 
GROUP BY m4scb_h_hr_conveni.id_organization,m4scb_h_hr_conveni.std_id_hr,m4scb_h_hr_conveni.std_or_hr_period;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_m4scb_h_hr_rol_cc_temp_col AS SELECT m4scb_h_hr_rol_cc.id_organization ,m4scb_h_hr_rol_cc.sco_id_hr ,m4scb_h_hr_rol_cc.sco_or_hr_role, MAX(m4scb_h_hr_rol_cc.scb_dt_end) AS DT_END
FROM erp_mexico_sz.M4SCB_H_HR_ROL_CC
WHERE m4scb_h_hr_rol_cc.id_organization = '072' 
GROUP BY m4scb_h_hr_rol_cc.id_organization ,m4scb_h_hr_rol_cc.sco_id_hr ,m4scb_h_hr_rol_cc.sco_or_hr_role;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_m4ssp_h_cent_cos_iber AS SELECT m4ssp_h_cent_cos.id_organization ,m4ssp_h_cent_cos.sco_id_hr ,m4ssp_h_cent_cos.sco_or_hr_role, MAX(m4ssp_h_cent_cos.ssp_fec_fin) AS DT_END
FROM erp_mexico_sz.M4SSP_H_CENT_COS
WHERE m4ssp_h_cent_cos.id_organization IN ('118','170','171','172','173','175','176','177','183','190')
GROUP BY m4ssp_h_cent_cos.id_organization ,m4ssp_h_cent_cos.sco_id_hr ,m4ssp_h_cent_cos.sco_or_hr_role;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.v_m4ssp_h_convenios_iber AS SELECT m4ssp_h_convenios.id_organization ,m4ssp_h_convenios.ssp_id_hr ,m4ssp_h_convenios.std_or_hr_period,MAX(m4ssp_h_convenios.fec_fin) AS DT_END
FROM erp_mexico_sz.M4SSP_H_CONVENIOS
WHERE m4ssp_h_convenios.id_organization IN ('118','170','171','172','173','175','176','177','183','190')
GROUP BY m4ssp_h_convenios.id_organization,m4ssp_h_convenios.ssp_id_hr,m4ssp_h_convenios.std_or_hr_period;


CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_a_pago_empleado AS select
      m4t_contabilidad_dwh.id_tipo_nomina as tiponomina_id
     ,m4t_contabilidad_dwh.id_empleado as empleado_id
     ,m4t_contabilidad_dwh.fecha_pago as fechapago
     ,m4t_contabilidad_dwh.id_cuenta_natural as cuentanatural_id
     ,m4t_contabilidad_dwh.id_analisis_local as analisislocal_id
     ,m4t_contabilidad_dwh.id_concepto as concepto_id
     ,substr(m4t_contabilidad_dwh.id_unidad,1,6) as region_id
     ,m4t_contabilidad_dwh.monto_pago as montopago
     ,1 as tipomoneda_id
     ,'emind_mexico' as sistemafuente
     ,'user' as usuarioetl
     ,m4t_contabilidad_dwh.fecha_registro as fechacarga
     ,m4t_contabilidad_dwh.fec_ult_actualizacion as fechacambio
     ,m4t_contabilidad_dwh.storeday as storeday
from erp_mexico_sz.m4t_contabilidad_dwh;

CREATE VIEW IF NOT EXISTS gb_mdl_mexico_costoproducir_views.vdw_gl_estructura_contable AS select
gl_code_combinations.code_combination_id 
,regexp_replace(gl_code_combinations.last_update_date, '-', '/')
,gl_code_combinations.last_update_date_h 
,gl_code_combinations.last_updated_by 
,gl_code_combinations.chart_of_accounts_id 
,gl_code_combinations.detail_posting_allowed_flag 
,gl_code_combinations.detail_budgeting_allowed_flag 
,gl_code_combinations.account_type 
,gl_code_combinations.enabled_flag 
,gl_code_combinations.summary_flag 
,gl_code_combinations.segment1 
,gl_code_combinations.segment2 
,gl_code_combinations.segment3 
,gl_code_combinations.segment4 
,gl_code_combinations.segment5 
,gl_code_combinations.segment6 
,gl_code_combinations.segment7 
,gl_code_combinations.segment8 
,gl_code_combinations.segment9 
,gl_code_combinations.segment10 
,gl_code_combinations.segment11 
,gl_code_combinations.description 
,gl_code_combinations.template_id 
,gl_code_combinations.allocation_create_flag 
,regexp_replace(gl_code_combinations.start_date_active, '-', '/') 
,gl_code_combinations.start_date_active_h 
,regexp_replace(gl_code_combinations.end_date_active, '-', '/') 
,gl_code_combinations.end_date_active_h
,gl_code_combinations.storeday 
from erp_mexico_sz.gl_code_combinations;
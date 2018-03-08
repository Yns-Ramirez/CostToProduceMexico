-- ======================================================
-- gb_smntc_mexico_costoproducir.v_rubro25_diferencia_inv.
insert into gb_smntc_mexico_costoproducir.v_rubro25_diferencia_inv partition(entidadlegal_id) select regexp_replace(substr(a.transaction_date, 0, 7),'/','-') as periodo, trim(b.segment6) as centrocostos_id, a.organization_id as mf_organizacion_id, a.inventory_item_id as mf_producto_id, sum(c.primary_quantity * c.actual_cost) as monto, from_unixtime(unix_timestamp()), trim(b.segment1) as entidadlegal_id from gb_mdl_mexico_costoproducir.mtl_transaction_accounts a, gb_mdl_mexico_costoproducir.gl_estructura_contable b, gb_mdl_mexico_costoproducir.mtl_transaccion_materiales c where (a.transaction_id = c.transaction_id and a.organization_id = c.organization_id and a.inventory_item_id = c.inventory_item_id and a.reference_account = b.code_combination_id) and (a.transaction_source_type_id = 10 and trim(b.segment3) = '6717'and trim(b.segment4) = '0023'and trim(b.segment6) != '0405') group by substr(a.transaction_date, 0, 7), trim(b.segment1), trim(b.segment6), a.organization_id, a.inventory_item_id;
insert overwrite table gb_smntc_mexico_costoproducir.v_rubro25_diferencia_inv partition(entidadlegal_id) select tmp.* from gb_smntc_mexico_costoproducir.v_rubro25_diferencia_inv tmp join (select periodo , centrocostos_id, mf_organizacion_id, mf_producto_id , max(storeday) as first_record from gb_smntc_mexico_costoproducir.v_rubro25_diferencia_inv group by periodo , centrocostos_id, mf_organizacion_id, mf_producto_id ) sec on tmp.periodo = sec.periodo and tmp.centrocostos_id = sec.centrocostos_id and tmp.mf_organizacion_id = sec.mf_organizacion_id and tmp.mf_producto_id = sec.mf_producto_id and tmp.storeday = sec.first_record;
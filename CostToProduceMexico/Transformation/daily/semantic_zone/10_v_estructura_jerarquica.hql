-- ======================================================
--  gb_mdl_mexico_manufactura.mf_organizacion
insert into table gb_mdl_mexico_manufactura.mf_organizacion partition(entidadlegal_id) select fte.mf_organizacion_id as mf_organizacion_id , case when coalesce(fte.host_id,'-1') <> '-1' then fte.host_id else coalesce(tbl.host_id,'-1') end as host_id , case when coalesce(fte.organizacion_desc,'s/i') <> 's/i' then fte.organizacion_desc else coalesce(tbl.organizacion_desc,'s/i') end as organizacion_desc , case when coalesce(fte.organizacion_tipo,'s/i') <> 's/i' then fte.organizacion_tipo else coalesce(tbl.organizacion_tipo,'s/i') end as organizacion_tipo, fte.storeday as storeday, case when coalesce(fte.entidadlegal_id,'-1') <> '-1' then fte.entidadlegal_id else coalesce(tbl.entidadlegal_id,'-1') end as entidadlegal_id from gb_mdl_mexico_costoproducir_views.vdw_mf_organizacion fte left join gb_mdl_mexico_manufactura.mf_organizacion tbl on fte.mf_organizacion_id = tbl.mf_organizacion_id and fte.entidadlegal_id = tbl.entidadlegal_id where ( (tbl.mf_organizacion_id is null or tbl.entidadlegal_id is null) or ( trim(fte.host_id) <> trim(tbl.host_id) or trim(fte.organizacion_desc) <> trim(tbl.organizacion_desc) or trim(fte.organizacion_tipo) <> trim(tbl.organizacion_tipo) ) );

insert into gb_mdl_mexico_manufactura.mf_organizacion partition(entidadlegal_id) select trn.mf_organizacion_id ,'-1','s/i','s/i',trn.storeday ,'-1'from (select mt.transfer_organization_id   as mf_organizacion_id, mt.storeday as storeday from erp_mexico_sz.mtl_material_transactions mt group by mt.transfer_organization_id, mt.storeday union all select mt2.organization_id as mf_organizacion_id, mt2.storeday as storeday from erp_mexico_sz.mtl_material_transactions mt2 group by mt2.organization_id, mt2.storeday union all select -1 as mf_organizacion_id, from_unixtime(unix_timestamp()) as storeday from gb_mdl_mexico_manufactura.mf_plantas group by -1 )trn where trn.mf_organizacion_id not in (select mf2.mf_organizacion_id from gb_mdl_mexico_manufactura.mf_organizacion mf2 where mf2.entidadlegal_id in ('100','101','114') );

insert overwrite table gb_mdl_mexico_manufactura.mf_organizacion partition(entidadlegal_id) select tmp.* from gb_mdl_mexico_manufactura.mf_organizacion tmp join (select entidadlegal_id, mf_organizacion_id, max(storeday) as first_record from gb_mdl_mexico_manufactura.mf_organizacion group by entidadlegal_id, mf_organizacion_id) sec on tmp.entidadlegal_id = sec.entidadlegal_id and tmp.mf_organizacion_id = sec.mf_organizacion_id and tmp.storeday = sec.first_record;

-- ======================================================
-- mf_regiones
-- gb_mdl_mexico_manufactura.mf_regiones
insert into table gb_mdl_mexico_manufactura.mf_regiones partition(entidadlegal_id) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_mf_regiones tmp;
insert overwrite table  gb_mdl_mexico_manufactura.mf_regiones partition(entidadlegal_id) select tmp.* from  gb_mdl_mexico_manufactura.mf_regiones tmp join (select entidadlegal_id, region_id, max(storeday) as first_record from  gb_mdl_mexico_manufactura.mf_regiones group by entidadlegal_id, region_id) sec on tmp.entidadlegal_id = sec.entidadlegal_id and  tmp.region_id = sec.region_id and tmp.storeday = sec.first_record;



-- ======================================================
-- mf_gerencias
-- gb_mdl_mexico_manufactura.mf_gerencias
insert into table gb_mdl_mexico_manufactura.mf_gerencias partition(entidadlegal_id) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_mf_gerencias tmp;
insert overwrite table  gb_mdl_mexico_manufactura.mf_gerencias partition(entidadlegal_id) select tmp.* from  gb_mdl_mexico_manufactura.mf_gerencias tmp join (select entidadlegal_id, gerencia_id, max(storeday) as first_record from  gb_mdl_mexico_manufactura.mf_gerencias group by entidadlegal_id, gerencia_id) sec on tmp.entidadlegal_id = sec.entidadlegal_id and  tmp.gerencia_id = sec.gerencia_id and tmp.storeday = sec.first_record;


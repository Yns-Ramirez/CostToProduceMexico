-- ALL DEPENDENCIES FOR V_ENTIDADLEGAL_TIPOMONEDA


-- ======================================================
--  o_entidadlegal_organizacion
-- insertar
insert into table gb_mdl_mexico_costoproducir.o_entidadlegal_organizacion partition(entidadlegal_id) select max(wl.std_id_work_locat) as organizacion_id, cast(sc.std_id_count_group as int) as pais_id, 'peoplenet v7' as sistema_fuente, from_unixtime(unix_timestamp()) as fecha_alta, from_unixtime(unix_timestamp()), len.std_id_leg_ent as entidadlegal_id from erp_mexico_sz.std_leg_ent len join erp_mexico_sz.std_work_location wl on len.std_id_leg_ent = wl.id_organization and wl.std_id_wl_type = '5'join erp_mexico_sz.std_country sc on len.ssp_id_pais_emisor = sc.std_id_country where len.std_id_leg_ent is not null and wl.std_id_work_locat is not null and sc.std_id_count_group is not null group by len.std_id_leg_ent,cast(sc.std_id_count_group as int),'peoplenet v7',from_unixtime(unix_timestamp());
insert overwrite table gb_mdl_mexico_costoproducir.o_entidadlegal_organizacion partition(entidadlegal_id) select tmp.* from gb_mdl_mexico_costoproducir.o_entidadlegal_organizacion tmp join (select entidadlegal_id, max(storeday) as first_record from gb_mdl_mexico_costoproducir.o_entidadlegal_organizacion group by entidadlegal_id) sec on tmp.entidadlegal_id = sec.entidadlegal_id and tmp.storeday = sec.first_record;

-- ======================================================
--  o_entidad_legal
insert into table gb_mdl_mexico_costoproducir.o_entidad_legal partition(entidadlegal_id) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_o_entidad_legal tmp;
insert overwrite table gb_mdl_mexico_costoproducir.o_entidad_legal partition(entidadlegal_id) select tmp.* from gb_mdl_mexico_costoproducir.o_entidad_legal tmp join (select entidadlegal_id, max(storeday) as first_record from gb_mdl_mexico_costoproducir.o_entidad_legal group by entidadlegal_id) sec on tmp.entidadlegal_id = sec.entidadlegal_id and tmp.storeday = sec.first_record;

-- ======================================================
--  v_tipo_moneda
truncate table gb_mdl_mexico_costoproducir.v_tipo_moneda;
insert into table gb_mdl_mexico_costoproducir.v_tipo_moneda partition(storeday) 
    select 
    tmp.tipomoneda_id,
    tmp.nombretipomoneda,
    tmp.nombrecorto,
    tmp.pais_id,
    tmp.sistemafuente,
    tmp.usuarioetl,
    tmp.fechacarga,
    tmp.fechacambio,
    max(tmp.storeday)
    from gb_mdl_mexico_costoproducir_views.vdw_v_tipo_moneda tmp where nombretipomoneda is not null
    group by 
    tmp.tipomoneda_id,
    tmp.nombretipomoneda,
    tmp.nombrecorto,
    tmp.pais_id,
    tmp.sistemafuente,
    tmp.usuarioetl,
    tmp.fechacarga,
    tmp.fechacambio;


insert overwrite table gb_mdl_mexico_costoproducir.v_tipo_moneda partition(storeday) select tmp.* from gb_mdl_mexico_costoproducir.v_tipo_moneda tmp join (select tipomoneda_id, max(storeday) as first_record from gb_mdl_mexico_costoproducir.v_tipo_moneda group by tipomoneda_id) sec on tmp.tipomoneda_id = sec.tipomoneda_id and tmp.storeday = sec.first_record;

-- ======================================================
--  g_pais
truncate table gb_mdl_mexico_costoproducir.g_pais;
insert into table gb_mdl_mexico_costoproducir.g_pais partition(storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_g_pais tmp;
insert overwrite table  gb_mdl_mexico_costoproducir.g_pais partition(storeday) select tmp.* from  gb_mdl_mexico_costoproducir.g_pais tmp join (select pais_id, max(storeday) as first_record from  gb_mdl_mexico_costoproducir.g_pais group by pais_id) sec on tmp.pais_id = sec.pais_id and tmp.storeday = sec.first_record;

-- ======================================================
--  e_organizacion
truncate table gb_mdl_mexico_costoproducir.e_organizacion;
insert into table gb_mdl_mexico_costoproducir.e_organizacion partition(storeday) select tmp.* from gb_mdl_mexico_costoproducir_views.vdw_e_organizacion tmp;
insert overwrite table  gb_mdl_mexico_costoproducir.e_organizacion partition(storeday) select tmp.* from  gb_mdl_mexico_costoproducir.e_organizacion tmp join (select organizacion_id, max(storeday) as first_record from  gb_mdl_mexico_costoproducir.e_organizacion group by organizacion_id) sec on tmp.organizacion_id = sec.organizacion_id and tmp.storeday = sec.first_record;
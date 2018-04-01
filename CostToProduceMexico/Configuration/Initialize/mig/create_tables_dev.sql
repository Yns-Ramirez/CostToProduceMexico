create database if not exists mig location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db';

create table if not exists mig.cp_control_entidadeslegales location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/cp_control_entidadeslegales' as select * from gb_mdl_mexico_costoproducir.cp_control_entidadeslegales;
create table if not exists mig.gx_control_el_peoplenet location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/gx_control_el_peoplenet' as select * from gb_mdl_mexico_costoproducir.gx_control_el_peoplenet;
create table if not exists mig.gx_control_entidades_app location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/gx_control_entidades_app' as select * from gb_mdl_mexico_costoproducir.gx_control_entidades_app;
create table if not exists mig.gx_control_entidadeslegales location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/gx_control_entidadeslegales' as select * from gb_mdl_mexico_costoproducir.gx_control_entidadeslegales;
create table if not exists mig.g_organizacion_geografica location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/g_organizacion_geografica' as select * from gb_mdl_mexico_costoproducir.g_organizacion_geografica;
create table if not exists mig.mf_turno_default location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_turno_default' as select * from gb_mdl_mexico_manufactura.mf_turno_default;
create table if not exists mig.mf_tipo_costo location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_tipo_costo' as select * from gb_smntc_mexico_costoproducir.mf_tipo_costo;
create table if not exists mig.cp_factores location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/cp_factores' as select * from cp_flat_files.cp_factores;
create table if not exists mig.ft_jer_producto location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/ft_jer_producto' as select * from cp_flat_files.ft_jer_producto;
create table if not exists mig.ft_producto location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/ft_producto' as select * from cp_flat_files.ft_producto;
create table if not exists mig.ic_calendario_dim location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/ic_calendario_dim' as select * from cp_flat_files.ic_calendario_dim;
create table if not exists mig.v_organizacion_inactiva_mf location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/v_organizacion_inactiva_mf' as select * from cp_flat_files.v_organizacion_inactiva_mf;
create table if not exists mig.cp_parametros location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/cp_parametros' as select * from gb_smntc_mexico_costoproducir.cp_parametros;
create table if not exists mig.calendar location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/calendar' as select * from cp_sys_calendar.calendar;
create table if not exists mig.wip_repetitive_items location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/wip_repetitive_items' as select * from gb_mdl_mexico_costoproducir.wip_repetitive_items;
create table if not exists mig.mtl_referencia_cruzada_mat location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mtl_referencia_cruzada_mat' as select * from gb_mdl_mexico_costoproducir.mtl_referencia_cruzada_mat;
create table if not exists mig.mf_unidad_medida location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_unidad_medida' as select * from gb_mdl_mexico_manufactura.mf_unidad_medida;
create table if not exists mig.mf_linea location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_linea' as select * from gb_mdl_mexico_manufactura.mf_linea;
create table if not exists mig.mf_marca location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_marca' as select * from gb_mdl_mexico_manufactura.mf_marca;
create table if not exists mig.mf_presentacion location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_presentacion' as select * from gb_mdl_mexico_manufactura.mf_presentacion;
create table if not exists mig.mf_categoria location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_categoria' as select * from gb_mdl_mexico_manufactura.mf_categoria;
create table if not exists mig.mf_sublinea location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/mf_sublinea' as select * from gb_mdl_mexico_manufactura.mf_sublinea;


create table if not exists mig.a_pago_empleado location 's3a://prod-cloudera-s3/user/hive/warehouse/mig.db/a_pago_empleado' as select * from gb_smntc_mexico_costoproducir.a_pago_empleado;

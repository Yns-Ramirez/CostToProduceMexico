-- ======================================================
--  cp_dwh.acum_gl_daily_rates
truncate table cp_dwh.acum_gl_daily_rates;
insert into table cp_dwh.acum_gl_daily_rates
    select
    -1000 as fuente_mak,
    from_currency,
    to_currency,
    conversion_date,
    conversion_type,
    conversion_rate,
    status_code,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    context,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    attribute9,
    attribute10,
    attribute11,
    attribute12,
    attribute13,
    attribute14,
    attribute15,
    from_unixtime(unix_timestamp()) as storeday
    from erp_mexico_sz.gl_daily_rates;

insert overwrite table cp_dwh.acum_gl_daily_rates select tmp.* from cp_dwh.acum_gl_daily_rates tmp join (select from_currency ,to_currency ,conversion_date ,conversion_type, max(storeday) as first_record from cp_dwh.acum_gl_daily_rates group by from_currency ,to_currency ,conversion_date ,conversion_type) sec on tmp.from_currency = sec.from_currency and tmp.to_currency = sec.to_currency and tmp.conversion_date = sec.conversion_date and tmp.conversion_type = sec.conversion_type and tmp.storeday = sec.first_record;



-- ======================================================
--  a_tipo_cambio
insert into table cp_app_costoproducir.a_tipo_cambio select tmp.* from cp_view.vdw_v_tipo_cambio_erp tmp;
insert overwrite table cp_app_costoproducir.a_tipo_cambio select tmp.* from cp_app_costoproducir.a_tipo_cambio tmp join (select monedaorigen_id, monedadestino_id, fechatipocambio, max(storeday) as first_record from cp_app_costoproducir.a_tipo_cambio group by monedaorigen_id, monedadestino_id, fechatipocambio) sec on tmp.monedaorigen_id = sec.monedaorigen_id and tmp.monedadestino_id = sec.monedadestino_id and tmp.fechatipocambio = sec.fechatipocambio and tmp.storeday = sec.first_record;
create table cp_dwh.tmp_tipocambio_huecos(monedaorigen_id string, monedadestino_id string, fecha string, tipocambio decimal(15,5), fuente_id string, fechacarga string, fechanulos string);
insert into cp_dwh.tmp_tipocambio_huecos select aa.monedaorigen_id ,aa.monedadestino_id ,aa.fecha ,bb.tipocambio ,bb.fuente_id ,bb.fechacarga ,to_date(from_unixtime(unix_timestamp())) as fechanulos from (select a.fecha ,b.monedaorigen_id ,b.monedadestino_id ,max(c.fechatipocambio) over (partition by b.monedaorigen_id,b.monedadestino_id order by a.fecha rows unbounded preceding) as  ultimafecha from cp_flat_files.ic_calendario_dim a left join (select distinct monedaorigen_id, monedadestino_id from cp_app_costoproducir.a_tipo_cambio) b on b.monedaorigen_id is not null left join cp_app_costoproducir.a_tipo_cambio c on a.fecha = c.fechatipocambio and b.monedaorigen_id = c.monedaorigen_id and b.monedadestino_id = c.monedadestino_id where a.fecha <= to_date(from_unixtime(unix_timestamp())) ) aa left join cp_app_costoproducir.a_tipo_cambio bb on aa.ultimafecha = bb.fechatipocambio and aa.monedaorigen_id = bb.monedaorigen_id and aa.monedadestino_id = bb.monedadestino_id;
insert into cp_app_costoproducir.a_tipo_cambio select tc.monedaorigen_id ,tc.monedadestino_id ,tc.fecha ,case when tc.tipocambio is not null then tc.tipocambio else 0 end as tipocambio ,'aut','oracle-mx-r11' as sistemafuente ,'user' as usuarioetl ,to_date(from_unixtime(unix_timestamp())) as fechacarga ,to_date(from_unixtime(unix_timestamp())) as fechacambio ,to_date(from_unixtime(unix_timestamp())) as storeday from cp_dwh.tmp_tipocambio_huecos tc left outer join (select fechatipocambio ,monedaorigen_id ,monedadestino_id from cp_app_costoproducir.a_tipo_cambio )atc on tc.fecha = atc.fechatipocambio and tc.monedaorigen_id = atc.monedaorigen_id  and tc.monedadestino_id = atc.monedadestino_id where atc.fechatipocambio is null and atc.monedaorigen_id is null and atc.monedadestino_id is null;
drop table cp_dwh.tmp_tipocambio_huecos;

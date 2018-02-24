-- ======================================================
--  cp_dwh_mf.mf_lineas_prod_centro_costos
insert overwrite table cp_dwh_mf.mf_lineas_prod_centro_costos partition(entidadlegal_id)
select tmp.mf_organizacion_id 
      ,tmp.planta_id
      ,tmp.centrocostos_id
      ,tmp.linea_prod_id                 
      ,tmp.dl                            
      ,tmp.fecha_ini
      , case when tmp.fecha_ini = from_unixtime(unix_timestamp()) then tmp.fecha_ini 
      else date_sub(from_unixtime(unix_timestamp()), 1) end as fecha_fin
      ,from_unixtime(unix_timestamp())
      ,tmp.entidadlegal_id
      from cp_dwh_mf.mf_lineas_prod_centro_costos tmp
      join (
        select sec.EntidadLegal_ID, sec.MF_Organizacion_ID, sec.Planta_ID  from cp_dwh_mf.mf_lineas_prod_centro_costos sec
          join (
            select cc.EntidadLegal_ID, cc.MF_Organizacion_ID, cc.Planta_ID from cp_view.vdw_mf_lineas_prod_cc cc
            group by cc.EntidadLegal_ID, cc.MF_Organizacion_ID, cc.Planta_ID
            ) pcc
          on pcc.entidadlegal_id       = sec.entidadlegal_id
          and pcc.mf_organizacion_id   = sec.mf_organizacion_id
          and pcc.planta_id            = sec.planta_id
          where
            sec.fecha_fin is null
          group by sec.EntidadLegal_ID, sec.MF_Organizacion_ID, sec.Planta_ID
        ) D on tmp.entidadlegal_id         = D.entidadlegal_id
        and tmp.mf_organizacion_id   = D.mf_organizacion_id
        and tmp.planta_id            = D.planta_id
      where tmp.fecha_fin is null;


insert into table cp_dwh_mf.mf_lineas_prod_centro_costos partition(entidadlegal_id)
  select
          tmp.mf_organizacion_id 
          ,tmp.planta_id
          ,tmp.centrocostos_id
          ,tmp.linea_prod_id                 
          ,tmp.dl                            
          ,tmp.fecha_ini
          ,tmp.fecha_fin
          ,from_unixtime(unix_timestamp())
          ,tmp.entidadlegal_id
      from cp_view.vdw_mf_lineas_prod_cc tmp
     left outer join 
     (
      select cc.EntidadLegal_ID, cc.MF_Organizacion_ID, cc.Planta_ID from cp_dwh_mf.mf_lineas_prod_centro_costos cc
            where cc.fecha_fin is null
            group by cc.EntidadLegal_ID, cc.MF_Organizacion_ID, cc.Planta_ID
     ) lpcc on 
     lpcc.entidadlegal_id          = tmp.entidadlegal_id
     and lpcc.mf_organizacion_id   = tmp.mf_organizacion_id
     and lpcc.planta_id            = tmp.planta_id
     where
     lpcc.entidadlegal_id is null and 
     lpcc.mf_organizacion_id is null and 
     lpcc.planta_id is null;

--compactation
insert overwrite table  cp_dwh_mf.mf_lineas_prod_centro_costos partition(entidadlegal_id) select tmp.* from  cp_dwh_mf.mf_lineas_prod_centro_costos tmp join (select entidadlegal_id, mf_organizacion_id, planta_id, centrocostos_id, max(storeday) as first_record from  cp_dwh_mf.mf_lineas_prod_centro_costos group by entidadlegal_id, mf_organizacion_id, planta_id, centrocostos_id) sec on tmp.entidadlegal_id = sec.entidadlegal_id and tmp.mf_organizacion_id = sec.mf_organizacion_id and tmp.planta_id = sec.planta_id and tmp.centrocostos_id = sec.centrocostos_id and tmp.storeday = sec.first_record;

 -- Se inserta a espejo los turnos
insert overwrite table cp_dwh_mf.mf_turnos_espejo partition(entidadlegal_id)
     select *
     from cp_view.vdw_mf_turnos;


-- Cierro el Periodo actual si es que se esta volviendo a cargar.
-- Se actualiza la vigencia de los turnos en cp_dwh_mf.MF_Turnos
insert overwrite table cp_dwh_mf.mf_turnos partition(entidadlegal_id)
     select 
     tt.mf_organizacion_id,
     tt.planta_id,
     tt.linea_prod_id,
     tt.turno_id,
     tt.turno_ds,
     tt.periodo,
     tt.fecha_ini,
     tt.fecha_fin,
     tt.hora_ini,
     tt.hora_fin,
     tt.origen,
     tt.observaciones,
     tt.fecha_carga,
     case 
          when tt.fecha_vigencia = to_date(from_unixtime(unix_timestamp())) 
               then tt.fecha_vigencia 
          else date_sub(to_date(from_unixtime(unix_timestamp())),1) 
     end as fecha_vigencia,
     from_unixtime(unix_timestamp()),
     tt.entidadlegal_id
     from cp_dwh_mf.mf_turnos tt
     join 
     (
          select t.entidadlegal_id,
                 t.periodo
          from cp_dwh_mf.mf_turnos t
          join(select entidadlegal_id, periodo from cp_dwh_mf.mf_turnos_espejo group by entidadlegal_id, periodo) te
          on t.entidadlegal_id = te.entidadlegal_id and t.periodo = te.periodo
          join (select entidadlegal_id from cp_view.v_entidadeslegales_activas_mf group by entidadlegal_id) ea on ea.entidadlegal_id = t.entidadlegal_id
          where t.fecha_vigencia is null
          group by
          t.entidadlegal_id,
          t.periodo
     ) stg
     on  tt.entidadlegal_id = stg.entidadlegal_id and tt.periodo = stg.periodo
     where 
          tt.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadeslegales_activas_mf group by entidadlegal_id)
          and tt.fecha_vigencia is null;


-- Se carga a MF_Turnos, pase de ESPEJO a DWH de los turnos enviados por TXT
insert into table cp_dwh_mf.mf_turnos partition(entidadlegal_id)
     select * from cp_dwh_mf.mf_turnos_espejo;
     
     

-- Se borra STG de tabla de archivo plano
-- insert overwrite table cp_flat_files.mf_turnos partition(entidadlegal_id)
     select fft.* 
     from cp_flat_files.mf_turnos fft
     where fft.entidadlegal_id not in (select entidadlegal_id from cp_view.v_entidadeslegales_activas_mf group by entidadlegal_id);


--se materializa vista cp_view.v_mf_turno_default_dia
insert overwrite table cp_view.t_mf_turno_default_dia
     select * from cp_view.v_mf_turno_default_dia;
--=================================================================================================================
-- Primero procedemos a generar los turnos por default para aquellas Entidades legales que no mandaron TXT.
--=================================================================================================================


-- ------ Inicio del Proceso ------ 

-- Se crean los turnos del periodo con los turnos del mes anterior inmediato debido a que no se proporciono txt
insert overwrite table cp_view.wrkt_mf_turnos_default partition(entidadlegal_id)
select  
     t2.mf_organizacion_id
     ,t2.planta_id
     ,t2.linea_prod_id
     ,t2.turno_id
     ,t2.turno_ds
     ,t2.periodo
     ,t2.fecha_ini_sig  as fecha_ini
     ,t2.fecha_fin_sig as fecha_fin
     ,t2.hora_ini
     ,t2.hora_fin
     ,t2.origen
     ,'turno default. no se proporciono txt mes actual' as observaciones
     ,from_unixtime(unix_timestamp()) as fecha_carga
     ,null as fecha_vigencia
     ,from_unixtime(unix_timestamp())
     ,t2.entidadlegal_id
from 
     (
          select 
               t.entidadlegal_id          as  entidadlegal_id
               ,t.mf_organizacion_id      as mf_organizacion_id
               ,t.planta_id               as planta_id
               ,t.linea_prod_id           as linea_prod_id
               ,t.turno_id                as turno_id
               ,t.turno_ds                as turno_ds
               ,c.perido_act              as periodo
               ,t.fecha_ini               as fecha_ini
               ,t.fecha_fin               as fecha_fin
               ,t.hora_ini                as hora_ini
               ,t.hora_fin                as hora_fin
               ,t.origen as origen
               ,count(*) over (partition by t.entidadlegal_id, t.mf_organizacion_id, t.planta_id, t.linea_prod_id,t.turno_id, t.periodo order by t.turno_id) as cuenta_turnos 
               ,add_months(t.fecha_ini, 1) as fecha_ini_sig 
               ,case 
                    when  
                         count(*) over (partition by t.entidadlegal_id, t.mf_organizacion_id, t.planta_id, t.linea_prod_id,t.turno_id, t.periodo order by t.turno_id)
                         = 1 and  add_months(t.fecha_fin,1) < c.fecha_finact  then  c.fecha_finact 
                    when 
                         count(*) over (partition by t.entidadlegal_id, t.mf_organizacion_id, t.planta_id, t.linea_prod_id,t.turno_id, t.periodo order by t.turno_id)
                         > 1 and  add_months(t.fecha_ini,1) <> c.fecha_iniact and add_months(t.fecha_fin,1) < c.fecha_finact  then  c.fecha_finact  
                    else add_months(t.fecha_fin,1)
               end as fecha_fin_sig
               ,t.storeday
          from cp_dwh_mf.mf_turnos t
               join
                    (
                         select
                         substr(add_months(from_unixtime(unix_timestamp()), -1),1,7)                                    as perido_ant,
                         substr(from_unixtime(unix_timestamp()),1,7)                                                    as perido_act,
                         concat(substr(from_unixtime(unix_timestamp()),1,7),'-01')                                      as fecha_iniact,
                         to_date(date_sub(add_months(concat(substr(from_unixtime(unix_timestamp()),1,7),'-01'),1), 1))  as fecha_finact
                  ) c
                  on  t.periodo = c.perido_ant
               join (
                         select eamf.entidadlegal_id 
                         from cp_view.V_ENTIDADESLEGALES_ACTIVAS_MF eamf 
                         where eamf.entidadlegal_id not in (select entidadlegal_id from cp_flat_files.MF_TURNOS group by entidadlegal_id)
                         group by eamf.entidadlegal_id
                    ) ea on t.entidadlegal_id = ea.entidadlegal_id
          where 
          t.fecha_vigencia is null
) t2;

-- Se inserta a espejo los turnos de EL-Periodo nuevos que no estan en DWH
insert overwrite table cp_dwh_mf.mf_turnos_espejo partition(entidadlegal_id)
select 
  td.mf_organizacion_id
  ,td.planta_id
  ,td.linea_prod_id
  ,td.turno_id
  ,td.turno_ds
  ,td.periodo
  ,td.fecha_inicia
  ,td.fecha_fin
  ,td.hora_inicia
  ,td.hora_fin
  ,td.origen
  ,td.observaciones
  ,td.fecha_carga
  ,td.fecha_vigencia
  ,from_unixtime(unix_timestamp())
  ,td.entidadlegal_id
from cp_view.wrkt_mf_turnos_default td
left outer join (
     select entidadlegal_id, periodo from cp_dwh_mf.mf_turnos where fecha_vigencia is null group by entidadlegal_id, periodo
     ) t on td.entidadlegal_id = t.entidadlegal_id and td.periodo = t.periodo
where t.entidadlegal_id is null and t.periodo is null;


-- Cierro el Periodo actual si es que se esta volviendo a cargar.
-- Se actualiza la vigencia de los turnos generados por default en cp_dwh_mf.mf_turnos
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


-- Se carga a MF_Turnos 
-- Pase de DWH.ESPEJO a DWH de los turnos generados por default
insert overwrite table cp_dwh_mf.mf_turnos partition(entidadlegal_id)
select * from cp_dwh_mf.mf_turnos_espejo;
     
-- ------ Fin de Turnos por Default ------

--=================================================================================================================
--  Una vez generados los turnos por default se procede a generar los turnos enviados por el usuario en el TXT para las Entidades legales que no se les genero turno por default
--=================================================================================================================

------ Inicio de Turnos por TXT ------

-- se insertan los casos donde no existen turnos rotativos en la tabla de trabajo wrkt_mf_turnos
INSERT overwrite table cp_view.wrkt_mf_turnos partition(entidadlegal_id)
SELECT 
TC1.MF_Organizacion_ID
,TC1.Planta_ID
,TC1.Linea_Prod_ID
,TC1.Turno_ID 
,TC1.Periodo
,TC1.TurnoFechaIni
,TC1.TurnoFechaFin
,TC1.FechaInicio_Ant 
,TC1.FechaFin_Ant
,TC1.FechaInicio_Sig
,TC1.FechaFin_Sig
,TC1.TurnoHraIni
,TC1.TurnoHraFinal
,TC1.HoraInicio_Ant 
,TC1.HoraFin_Ant
,TC1.HoraInicio_Sig
,TC1.HoraFin_Sig
,TC1.Turno_HrInicio
,TC1.Turno_HrFin
,1 AS TipoCaso  -- Es 1 cuando son turnos consecutivos
,CASE 
     WHEN TC1.TurnoHraFinal <> TC1.Turno_HrFin THEN 'Se corrigio gap de tiempo'
     ELSE 'S/I'
END AS Obervaciones 
,from_unixtime(unix_timestamp())
,from_unixtime(unix_timestamp())
,TC1.EntidadLegal_ID
FROM
(
     select
          t.entidadlegal_id
          ,t.mf_organizacion_id
          ,t.planta_id
          ,t.linea_prod_id
          ,t.turno_id 
          ,t.periodo
          ,t.turnofechaini
          ,t.turnofechafin
          ,t.turnohraini
          ,t.turnohrafinal
          ,t.cuenta_turnos
          ,case when t.cuenta_turnos = 3 then coalesce(t.fecini_ant, t.fecini_antp) 
                   when t.cuenta_turnos = 2 then coalesce(t.fecini_ant, t.fecini_sig )
                   when t.cuenta_turnos = 1 then t.turnofechaini
                   else t.fecini_ant
          end as  fechainicio_ant 
          ,case when t.cuenta_turnos = 3 then coalesce(t.fecfin_ant,t.fecfin_antp)
                   when t.cuenta_turnos = 2 then coalesce(t.fecfin_ant, t.fecfin_sig)
                   when t.cuenta_turnos = 1 then t.turnofechafin
                   else t.fecfin_ant
          end as fechafin_ant
          ,case when t.cuenta_turnos = 3 then coalesce(t.fecini_sig, t.fecini_sigp)
                   when t.cuenta_turnos = 2 then  coalesce(t.fecini_sig, t.fecini_ant)
                   when t.cuenta_turnos = 1 then turnofechafin
                   else t.fecini_sig
          end as  fechainicio_sig
          ,case when t.cuenta_turnos = 3 then coalesce(t.fecfin_sig, t.fecfin_sigp)
                   when t.cuenta_turnos = 2 then coalesce(t.fecfin_sig, t.fecfin_ant) 
                   when t.cuenta_turnos = 1 then t.turnofechaini
                   else t.fecfin_sig
          end as  fechafin_sig
          
          ,case when t.cuenta_turnos = 3 then coalesce(t.hrini_ant, t.hrini_antp) 
                   when t.cuenta_turnos = 2 then coalesce(t.hrini_ant, t.hrini_sig)
                   when t.cuenta_turnos = 1 then from_unixtime(unix_timestamp(t.turnohraini,'HH:mm:ss')-1,'HH:mm:ss')
                   else t.hrini_ant
          end as  horainicio_ant 
          ,case when t.cuenta_turnos = 3 then coalesce(t.hrfin_ant,t.hrfin_antp)
                   when t.cuenta_turnos = 2 then coalesce(t.hrfin_ant, t.hrfin_sig)
                   when t.cuenta_turnos = 1 then from_unixtime(unix_timestamp(t.turnohrafinal,'HH:mm:ss')-1,'HH:mm:ss') 
                   else t.hrfin_ant
          end as horafin_ant
          ,case when t.cuenta_turnos = 3 then coalesce(t.hrini_sig, t.hrini_sigp)
                   when t.cuenta_turnos = 2 then  coalesce(t.hrini_sig, t.hrini_ant)
                   when t.cuenta_turnos = 1 then from_unixtime(unix_timestamp(t.turnohrafinal,'HH:mm:ss')-1,'HH:mm:ss') 
                   else t.hrini_sig
          end as  horainicio_sig
          ,case when t.cuenta_turnos = 3 then coalesce(t.hrfin_sig, t.hrfin_sigp)
                   when t.cuenta_turnos = 2 then coalesce(t.hrfin_sig, t.hrfin_ant) 
                   when t.cuenta_turnos = 1 then from_unixtime(unix_timestamp(t.turnohraini,'HH:mm:ss')-1,'HH:mm:ss')
                   else t.hrfin_sig
          end as  horafin_sig
          ,t.turnohraini as turno_hrinicio  
          ,case when  t.cuenta_turnos = 1 then from_unixtime(unix_timestamp(t.turnohraini,'HH:mm:ss')-1,'HH:mm:ss')
                  else from_unixtime(unix_timestamp(
                    case when t.cuenta_turnos = 3 then coalesce(t.hrini_sig, t.hrini_sigp)
                         when t.cuenta_turnos = 2 then  coalesce(t.hrini_sig, t.hrini_ant)
                         when t.cuenta_turnos = 1 then from_unixtime(unix_timestamp(t.turnohrafinal,'HH:mm:ss')-1,'HH:mm:ss') 
                         else t.hrini_sig
                    end
                    ,'HH:mm:ss')-1,'HH:mm:ss')
          end as turno_hrfin 
     from 
               (
                    select
                    t.entidadlegal_id                      as entidadlegal_id
                    ,p.mf_organizacion_id                  as  mf_organizacion_id
                    ,t.planta_id                           as planta_id
                    ,t.linea_id                            as linea_prod_id
                    ,t.turno_id                            as turno_id
                    ,t.periodo                             as periodo
                    ,c.cuenta_turnos                       as cuenta_turnos
                    ,t.fecha_inicia                        as  turnofechaini
                    ,t.fecha_fin                           as turnofechafin
                    ,t.hora_inicia                         as turnohraini
                    ,t.hora_fin                            as turnohrafinal
                    
                    ,min(t.fecha_inicia)     over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id asc rows between 1 preceding and 1 preceding)     as fecini_ant
                    ,min(t.fecha_fin)        over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id asc rows between 1 preceding and 1 preceding)        as fecfin_ant
                    ,min(t.fecha_inicia)     over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id desc rows between 2 preceding and 2 preceding)  as fecini_antp
                    ,min(t.fecha_fin)        over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id desc rows between 2 preceding and 2 preceding)      as fecfin_antp
                    
                    ,min(t.fecha_inicia)     over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id asc rows between 1 following and 1 following)     as fecini_sig
                    ,min(t.fecha_fin)        over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id asc rows between 1 following and 1 following)        as fecfin_sig
                    ,min(t.fecha_inicia)     over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id desc rows between 2 following and 2 following)   as fecini_sigp
                    ,min(t.fecha_fin)        over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id desc rows between 2 following and 2 following)      as fecfin_sigp
                    
                    ,min(t.hora_inicia)      over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id asc rows between 1 preceding and 1 preceding)        as hrini_ant
                    ,min(t.hora_fin)         over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id asc rows between 1 preceding and 1 preceding)           as hrfin_ant
                    ,min(t.hora_inicia)      over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id desc rows between 2 preceding and 2 preceding)      as hrini_antp
                    ,min(t.hora_fin)         over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id desc rows between 2 preceding and 2 preceding)         as hrfin_antp
                    
                    ,min(t.hora_inicia)      over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id asc rows between 1 following and 1 following)       as hrini_sig
                    ,min(t.hora_fin)         over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id asc rows between 1 following and 1 following)          as hrfin_sig
                    ,min(t.hora_inicia)      over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id desc rows between 2 following and 2 following)     as hrini_sigp
                    ,min(t.hora_fin)         over(partition by  t.entidadlegal_id, p.mf_organizacion_id, t.planta_id,t.linea_id, t.periodo order by t.turno_id desc rows between 2 following and 2 following)        as hrfin_sigp
                    from  cp_flat_files.MF_TURNOS t
                    inner join cp_dwh_mf.mf_plantas p  on   trim(t.entidadlegal_id)  = trim(p.entidadlegal_id) and trim(t.planta_id) = trim(p.planta_id) and lower(sistema_fuente) = 'cp'
                    inner join cp_dwh_mf.mf_lineas_prod l on  trim(t.entidadlegal_id)  = trim(l.entidadlegal_id) and  t.linea_id =  l.linea_prod_id  
                    inner join 
                         (
                              select  
                                   tc.entidadlegal_id
                                   ,tc.planta_id
                                   ,tc.linea_prod_id
                                   ,tc.turno_id
                                   ,tc.periodo
                                   ,tc.cuenta_tl as cuenta_turnos
                              from cp_view.v_mf_turnos_cuenta tc
                              where tc.entidadlegal_id  in 
                                   (
                                        select ea.entidadlegal_id
                                        from cp_view.v_entidadeslegales_activas_mf ea
                                        join (select entidadlegal_id from cp_flat_files.MF_TURNOS group by entidadlegal_id) t_ on ea.entidadlegal_id = t_.entidadlegal_id
                                        group by ea.entidadlegal_id
                                   )
                              and tc.cuenta_tt = 1
                              group by 
                                   tc.entidadlegal_id
                                   ,tc.planta_id
                                   ,tc.linea_prod_id
                                   ,tc.turno_id
                                   ,tc.periodo
                                   ,tc.cuenta_tl
                         ) c
                         on  trim(t.entidadlegal_id)  = trim(c.entidadlegal_id)
                         and trim(t.planta_id) = trim(c.planta_id)         
                         and t.linea_id = c.linea_prod_id
                         and t.turno_id = c.turno_id
                         and trim(t.periodo) = trim(c.periodo)
                    join (
                              select ea2.entidadlegal_id 
                              from cp_view.v_entidadeslegales_activas_mf ea2
                              where ea2.entidadlegal_id  in (select entidadlegal_id from cp_flat_files.MF_TURNOS group by entidadlegal_id)
                              group by ea2.entidadlegal_id
                         )ela on p.entidadlegal_id = ela.entidadlegal_id
               where t.turno_id in (1,2,3)
          ) T
     ) TC1
WHERE  (TC1.TurnoHraFinal <> TC1.HoraInicio_Sig)
AND TC1.TurnoHraIni <> TC1.HoraFin_Ant;




-- Se insertan los turnos rotativos en la tabla de trabajo WRKT_MF_Turnos_Rotativos
INSERT overwrite table cp_view.wrkt_mf_turnos_rotativos partition(entidadlegal_id)
SELECT
P.MF_Organizacion_ID
,TR3.Planta_ID
,TR3.Linea_Prod_ID
,TR3.Turno_ID
,TR3.Periodo
,TR3.Fecha_Inicia
,TR3.Fecha_finC
,TR3.Hora_Inicia
,TR3.Hora_Fin
,RANK() OVER(PARTITION BY TR3.EntidadLegal_Id, P.MF_Organizacion_ID, TR3.Planta_ID, TR3.Linea_Prod_ID, TR3.Turno_ID ORDER BY TR3.Fecha_Inicia DESC)
,NULL
,TR3.Observaciones
,from_unixtime(unix_timestamp())
,TR3.EntidadLegal_ID
FROM 
(
     SELECT 
     TR2.EntidadLegal_ID
     ,TR2.Planta_ID
     ,TR2.Linea_Prod_ID
     ,TR2.Turno_ID
     ,TR2.Periodo
     ,TR2.Fecha_Inicia
     ,TR2.Fecha_fin
     ,COALESCE(TR2.FecIni_Sig,'2100/01/01') AS FechaIniSig
     ,COALESCE(TR2.FecFin_Ant,'2100/01/01') AS FechaFinAnt
     ,CASE 
          WHEN TR2.FecIni_Sig IS NOT NULL AND TR2.Fecha_Fin > TR2.FecIni_Sig THEN to_date(date_sub(TR2.FecIni_Sig, 1))
          WHEN TR2.FecIni_Sig IS NOT NULL AND TR2.Fecha_Fin < TR2.FecIni_Sig THEN to_date(date_sub(TR2.FecIni_Sig, 1))
          ELSE TR2.Fecha_Fin
     END AS Fecha_finC
     ,TR2.Hora_Inicia
     ,TR2.Hora_Fin
     ,CASE 
          WHEN TR2.FecIni_Sig IS NOT NULL AND TR2.Fecha_Fin > TR2.FecIni_Sig THEN 'Se corrigio la fecha'
          WHEN TR2.FecIni_Sig IS NOT NULL AND TR2.Fecha_Fin < TR2.FecIni_Sig THEN 'Se corrigio la fecha'
          ELSE  'S/I'
     END AS Observaciones
     FROM 
          (
               SELECT  
                    TR.EntidadLegal_ID
                    ,TR.Planta_ID
                    ,TR.Linea_Prod_ID
                    ,TR.Turno_ID
                    ,TR.Periodo
                    ,TR.Fecha_Inicia
                    ,TR.Fecha_fin
                    ,TR.Hora_Inicia
                    ,TR.Hora_Fin
                    ,MIN(TR.Fecha_Fin) OVER(PARTITION BY TR.EntidadLegal_ID, TR.Planta_ID, TR.Linea_Prod_ID, TR.Turno_ID ORDER BY TR.Fecha_Inicia ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS FecFin_Ant
                    ,MIN(TR.Fecha_Inicia) OVER(PARTITION BY TR.EntidadLegal_ID, TR.Planta_ID, TR.Linea_Prod_ID, TR.Turno_ID ORDER BY TR.Fecha_Inicia ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS FecIni_Sig
               FROM           
                    (
                         select 
                         t2_.entidadlegal_id
                         ,t2_.planta_id
                         ,t2_.Linea_ID as Linea_Prod_ID
                         ,t2_.turno_id
                         ,t2_.periodo
                         ,t2_.fecha_inicia
                         ,t2_.fecha_fin
                         ,t2_.hora_inicia 
                         ,t2_.hora_fin 
                         from  cp_flat_files.MF_TURNOS t2_
                         join(
                              select  
                                   tc.entidadlegal_id
                                   ,tc.planta_id
                                   ,tc.linea_prod_id
                                   ,tc.turno_id
                                   ,tc.periodo                                   
                              from cp_view.v_mf_turnos_cuenta tc
                              where tc.entidadlegal_id  in 
                                   (
                                        select ea.entidadlegal_id
                                        from cp_view.v_entidadeslegales_activas_mf ea
                                        join (select entidadlegal_id from cp_flat_files.MF_TURNOS group by entidadlegal_id) t_ on ea.entidadlegal_id = t_.entidadlegal_id
                                        group by ea.entidadlegal_id
                                   )
                              and tc.cuenta_tt > 1
                              group by 
                                   tc.entidadlegal_id
                                   ,tc.planta_id
                                   ,tc.linea_prod_id
                                   ,tc.turno_id
                                   ,tc.periodo
                         )c2 on t2_.entidadlegal_id = c2.entidadlegal_id and t2_.planta_id = c2.planta_id and t2_.Linea_ID = c2.linea_prod_id and t2_.turno_id = c2.turno_id and t2_.periodo = c2.periodo
                    ) TR
          )TR2
     WHERE TR2.Fecha_fin < COALESCE(TR2.FecIni_Sig,'2100/01/01')
) TR3
INNER JOIN cp_dwh_mf.MF_PLANTAS P  ON   TRIM(TR3.EntidadLegal_ID)  = TRIM(P.EntidadLegal_ID) AND TRIM(TR3.Planta_ID) = TRIM(P.Planta_ID)
where lower(P.Sistema_Fuente) = 'CP';
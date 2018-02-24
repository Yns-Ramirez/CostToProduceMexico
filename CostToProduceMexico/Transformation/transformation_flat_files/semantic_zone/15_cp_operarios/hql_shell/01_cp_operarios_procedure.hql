-- ======================================================
-- wrkt_a_rubros_fsg

insert into table cp_dwh.wrkt_a_rubros_fsg partition(entidadlegal_id)
     select fsg.* from cp_app_costoproducir.v_a_rubros_fsg fsg
          where fsg.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id) and fsg.aniosaldo = substr('${hiveconf:speriod}',1, 4)
          and fsg.messaldo  = substr('${hiveconf:speriod}',6, 2);

-- compactation
insert overwrite table cp_dwh.wrkt_a_rubros_fsg partition(entidadlegal_id) select tmp.* from cp_dwh.wrkt_a_rubros_fsg tmp join (select aniosaldo, messaldo, entidadlegal_id, linea_id, areanegocio_id, cuentanatural_id, centrocostos_id, analisislocal_id, max(storeday) as first_record from cp_dwh.wrkt_a_rubros_fsg group by aniosaldo, messaldo, entidadlegal_id, linea_id, areanegocio_id, cuentanatural_id, centrocostos_id, analisislocal_id) sec on tmp.aniosaldo = sec.aniosaldo and tmp.messaldo = sec.messaldo and  tmp.entidadlegal_id = sec.entidadlegal_id and tmp.linea_id = sec.linea_id and tmp.areanegocio_id = sec.areanegocio_id and tmp.cuentanatural_id = sec.cuentanatural_id and tmp.centrocostos_id = sec.centrocostos_id and  tmp.analisislocal_id = sec.analisislocal_id and tmp.storeday = sec.first_record;

-- ======================================================
-- cp_operarios

-- step 1 
insert overwrite table cp_wkrt.wkrt_cp_operarios_p_1
select
     ep.empleado_id
     ,p.posicion_id
     ,p.puesto_id
     ,p.entidadlegal_id
     ,p.areanegocio_id
     ,fsg_.mf_organizacion_id
     ,fsg_.planta_id
     ,p.centrocosto_id 
     ,p.unidadtrabajo_id
     ,p.centrotrabajo_id
     ,ep.motivobaja_id
     ,ep.fechainicioposicion
     ,ep.fechafinposicion
from cp_app_costoproducir.e_empleado_posicion ep
     join cp_app_costoproducir.e_posicion p on ep.posicion_id = p.posicion_id 
     join 
          (

          select
               fsg.aniosaldo
               ,fsg.messaldo 
               ,fsg.entidadlegal_id
               ,fsg.areanegocio_id
               ,p.mf_organizacion_id
               ,p.planta_id
               ,fsg.linea_id
               ,fsg.nombreconcepto
               ,fsg.centrocostos_id
          from cp_dwh.wrkt_a_rubros_fsg fsg
               join cp_app_costoproducir.v_mf_plantas p on fsg.entidadlegal_id = p.entidadlegal_id and fsg.areanegocio_id = p.planta_id 
               join (

                         select lc.entidadlegal_id, lc.mf_organizacion_id, lc.planta_id, lc.centrocostos_id
                         from cp_app_costoproducir.v_mf_lineas_prod_centro_costos lc
                              inner join cp_app_costoproducir.v_mf_plantas p on lc.entidadlegal_id = p.entidadlegal_id and lc.mf_organizacion_id = p.mf_organizacion_id and lc.planta_id = p.planta_id
                         where lc.dl = 1 and lc.fecha_fin is null  
                         and lc.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
                         group by lc.entidadlegal_id, lc.mf_organizacion_id, lc.planta_id, lc.centrocostos_id


                    ) lc2 
                    on  fsg.entidadlegal_id = lc2.entidadlegal_id and p.planta_id = lc2.planta_id and fsg.centrocostos_id = lc2.centrocostos_id
               join (
                    select  cpj.entidadlegal_id , trim(cpj.condicion) as condicion from cp_app_costoproducir.cp_parametros cpj where cpj.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id) and lower(cpj.objeto) = lower('cp_operarios') and lower(cpj.campo) = lower('cuentanatural_id') and cpj.subrubro_id is null group by cpj.entidadlegal_id,trim(cpj.condicion)
                    ) cpj2 
                    on fsg.entidadlegal_id = cpj2.entidadlegal_id and fsg.cuentanatural_id = cpj2.condicion
          where fsg.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
               and fsg.aniosaldo = substr('${hiveconf:speriod}',1, 4)
               and fsg.messaldo  = substr('${hiveconf:speriod}',6, 2)
               and fsg.linea_id = 30
               
               and fsg.messaldo <> 13
          group by fsg.aniosaldo, fsg.messaldo, fsg.entidadlegal_id, fsg.areanegocio_id, p.mf_organizacion_id, p.planta_id, fsg.linea_id, fsg.nombreconcepto, fsg.centrocostos_id

          ) fsg_ on p.entidadlegal_id  = fsg_.entidadlegal_id  and p.areanegocio_id = fsg_.areanegocio_id  and fsg_.centrocostos_id = p.centrocosto_id 
where p.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
     and p.puesto_id not in ('1938','1939','1940','1942')
     and ep.motivobaja_id is null
     and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1) >= ep.fechainicioposicion and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1) <= ep.fechafinposicion;

-- step 2
insert overwrite table cp_wkrt.wkrt_cp_operarios_e_2
select
     substr(p.fechapago,1,7)                 as periodo
     ,e.entidadlegal_id                      as entidadlegal_id
     ,e.mf_organizacion_id                   as mf_organizacion_id
     ,e.planta_id                            as planta_id
     ,e.centrocosto_id                       as centrocostos_id
     ,count(distinct p.empleado_id)          as total_empleados_cc
     ,sum(p.montopago)                       as total_pago_cc
from cp_view.v_a_pago_empleado p
     -- step 1
     join cp_wkrt.wkrt_cp_operarios_p_1 e on p.empleado_id = e.empleado_id

     join (
          select  cpj3.entidadlegal_id , trim(cpj3.condicion) as condicion from cp_app_costoproducir.cp_parametros cpj3 where cpj3.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id) and lower(cpj3.objeto) = lower('cp_operarios') and lower(cpj3.campo) = lower('tiponomina_id') and cpj3.subrubro_id is null group by cpj3.entidadlegal_id,trim(cpj3.condicion)
     ) cpj4 on e.entidadlegal_id = cpj4.entidadlegal_id and lower(trim(p.tiponomina_id)) = lower(trim(cpj4.condicion))

     join(

          select  cpj5.entidadlegal_id , trim(cpj5.condicion) as condicion from cp_app_costoproducir.cp_parametros cpj5 where cpj5.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id) and lower(cpj5.objeto) = lower('cp_operarios') and lower(cpj5.campo) = lower('cuentanatural_id') and cpj5.subrubro_id is null group by cpj5.entidadlegal_id,trim(cpj5.condicion)
     ) cpj6 on e.entidadlegal_id = cpj6.entidadlegal_id and p.cuentanatural_id = cpj6.condicion

where p.fechapago >= concat('${hiveconf:speriod}','-01') and p.fechapago <= date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
     and p.fechapago >= e.fechainicioposicion and p.fechapago <= e.fechafinposicion
group by substr(p.fechapago,1,7),e.entidadlegal_id,e.mf_organizacion_id,e.planta_id,e.centrocosto_id;


--step 3
insert overwrite table cp_wkrt.wkrt_cp_operarios_m_3
select
e.periodo
,e.entidadlegal_id
,e.mf_organizacion_id
,e.planta_id
,e.centrocostos_id
,e.total_empleados_cc
,e.total_pago_cc
,kgs.linea_prod_id
,kgs.tipomedida_id
,kgs.medida_linea
,kgs.medida_planta
,kgs.medida_cc
,kgs.factor_cc_linea_prod
,(e.total_empleados_cc * kgs.factor_cc_linea_prod) as empleados_asignados_linea
,case
     when kgs.factor_cc_linea_prod <> 1 then 'compartido' 
     when kgs.factor_cc_linea_prod = 1 then 'directo' 
     else 'error'
end as tipo_asignacion        
-- step 2
from cp_wkrt.wkrt_cp_operarios_e_2 e
inner join
(
     select
          m2.periodo
          ,m2.entidadlegal_id
          ,m2.mf_organizacion_id
          ,m2.planta_id
          ,m2.centrocostos_id
          ,m2.linea_prod_id
          ,m2.medida_linea
          ,m2.medida_planta
          ,sum(m2.medida_linea) over (partition by m2.periodo, m2.entidadlegal_id, m2.mf_organizacion_id, m2.centrocostos_id rows between unbounded preceding and unbounded following)  as medida_cc
          ,coalesce(
                    (m2.medida_linea / (sum(
                                             case when m2.medida_linea = 0 then null else m2.medida_linea end
                                       ) over (partition by m2.periodo, m2.entidadlegal_id, m2.mf_organizacion_id, m2.centrocostos_id rows between unbounded preceding and unbounded following))
                    )
                    ,0
          ) as factor_cc_linea_prod
          ,m2.tipomedida_id
     from 
          (
               select 
               '${hiveconf:speriod}' as periodo, 
               lc.entidadlegal_id, 
               lc.mf_organizacion_id, 
               lc.planta_id, 
               lc.centrocostos_id, 
               lc.linea_prod_id, 
               ml.medida_linea, 
               ml.medida_planta, 
               ml.tipomedida_id
               from cp_app_costoproducir.v_mf_lineas_prod_centro_costos lc
                    join
                    (
                         select cmpi.entidadlegal_id, cmpi.mf_organizacion_id, cmpi.planta_id, cmpi.linea_prod_id, cmpi.medida_linea, cmpi.medida_planta, cmpi.tipomedida_id
                         from cp_app_costoproducir.cp_medidas_prorrateo cmpi
                         where cmpi.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)  and tipomedida_id = 1 and periodo = '${hiveconf:speriod}'
                         group by cmpi.entidadlegal_id, cmpi.mf_organizacion_id, cmpi.planta_id, cmpi.linea_prod_id, cmpi.medida_linea, cmpi.medida_planta, cmpi.tipomedida_id
                    )  ml
                    on   lc.entidadlegal_id = ml.entidadlegal_id and lc.mf_organizacion_id = ml.mf_organizacion_id and lc.planta_id = ml.planta_id and lc.linea_prod_id = ml.linea_prod_id
               where lc.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id) and lc.dl = 1 and lc.fecha_fin is null 
               group by '${hiveconf:speriod}', lc.entidadlegal_id, lc.mf_organizacion_id, lc.planta_id, lc.centrocostos_id, lc.linea_prod_id, ml.medida_linea, ml.medida_planta, ml.tipomedida_id
          ) m2
          group by m2.periodo, m2.entidadlegal_id, m2.mf_organizacion_id, m2.planta_id, m2.centrocostos_id, m2.linea_prod_id, m2.medida_linea, m2.medida_planta, m2.tipomedida_id

) kgs on e.periodo = kgs.periodo and e.entidadlegal_id = kgs.entidadlegal_id and e.mf_organizacion_id = kgs.mf_organizacion_id and e.centrocostos_id = kgs.centrocostos_id;


-- step 4
insert overwrite table cp_wkrt.wkrt_cp_operarios_t_4
select
     substr(pr.fecha, 1, 7) as periodo
     ,pr.entidadlegal_id
     ,pr.mf_organizacion_id
     ,pr.planta_id
     ,pr.linea_prod_id
     ,tot.turnos_planta_asignados
     ,tot.turnos_planta_produccion
     ,tot.turnos_default           as turnos_linea_default
     ,count(distinct pr.turno_id)  as turnos_linea_produccion
from cp_view.v_mf_produccion pr
     inner join 
          (

               select 
                    substr(pr2.fecha, 1, 7) as periodo
                    ,pr2.entidadlegal_id
                    ,pr2.mf_organizacion_id
                    ,pr2.planta_id
                    ,count(distinct pr2.linea_prod_id) * max(td.total_turnos_default)     as turnos_planta_asignados
                    ,count( distinct concat(pr2.linea_prod_id, pr2.turno_id) )            as turnos_planta_produccion
                    ,max(td.total_turnos_default)                                         as turnos_default
               from cp_view.v_mf_produccion pr2
                    join 
                         (
                              select
                                   j.entidadlegal_id
                                   ,count (distinct j.turno_id) as total_turnos_default
                              from cp_dwh_mf.mf_turno_default j
                              group by j.entidadlegal_id

                         ) td on trim(pr2.entidadlegal_id) = trim(td.entidadlegal_id)
               where pr2.linea_prod_id <> -1 and pr2.total_registrado <> 0
                    and pr2.fecha >= concat('${hiveconf:speriod}','-01') and pr2.fecha <= date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                    and pr2.entidadlegal_id in (
                         select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id
                         )
               group by substr(pr2.fecha, 1, 7), pr2.entidadlegal_id, pr2.mf_organizacion_id, pr2.planta_id
               having (count(distinct pr2.linea_prod_id) * max(td.total_turnos_default))  <> 0

          ) tot on substr(pr.fecha, 1, 7) = tot.periodo and pr.entidadlegal_id = tot.entidadlegal_id and pr.mf_organizacion_id = tot.mf_organizacion_id and pr.planta_id = tot.planta_id
where pr.linea_prod_id <> -1 and pr.total_registrado <> 0
     and pr.fecha >= concat('${hiveconf:speriod}','-01') and pr.fecha <= date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
     and pr.entidadlegal_id in  (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
group by substr(pr.fecha, 1, 7), pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id, pr.linea_prod_id, tot.turnos_planta_asignados, tot.turnos_planta_produccion, tot.turnos_default
having count(distinct pr.turno_id) <> 0;

-- step 5
insert into cp_app_costoproducir.cp_operarios partition(entidadlegal_id)
select
     m.periodo
     ,m.mf_organizacion_id
     ,m.planta_id
     ,m.centrocostos_id
     ,m.linea_prod_id
     ,-1                           as turno_id
     ,-1                           as mf_producto_id
     ,m.tipomedida_id
     ,t.turnos_linea_default
     ,t.turnos_linea_produccion
     ,t.turnos_planta_asignados
     ,t.turnos_planta_produccion
     ,m.total_empleados_cc         as total_cc_empleado
     ,m.total_pago_cc              as total_cc_pago
     ,m.factor_cc_linea_prod       as medida_factor                 
     ,0                            as medida_producto             
     ,0                            as medida_turno                  
     ,m.empleados_asignados_linea  as medida_linea                  
     ,sum(m.empleados_asignados_linea) over (partition by m.periodo, m.entidadlegal_id, m.mf_organizacion_id rows between unbounded preceding and unbounded following) as medida_planta
     ,sum(m.empleados_asignados_linea) over (partition by m.periodo, m.entidadlegal_id rows between unbounded preceding and unbounded following) as medida_entidadlegal
     ,null                              as observaciones
     ,from_unixtime(unix_timestamp()) as storeday
     ,m.entidadlegal_id
      -- step 3
from cp_wkrt.wkrt_cp_operarios_m_3 m
     -- step 4
     inner join cp_wkrt.wkrt_cp_operarios_t_4 t
          on m.periodo = t.periodo and m.entidadlegal_id = t.entidadlegal_id and m.mf_organizacion_id = t.mf_organizacion_id and m.planta_id = t.planta_id and m.linea_prod_id = t.linea_prod_id
     group by m.periodo, m.entidadlegal_id, m.mf_organizacion_id, m.planta_id, m.centrocostos_id, m.linea_prod_id, m.tipomedida_id, t.turnos_linea_default, t.turnos_linea_produccion, t.turnos_planta_asignados, t.turnos_planta_produccion, m.total_empleados_cc, m.total_pago_cc, m.factor_cc_linea_prod, m.empleados_asignados_linea;

--compactation
insert overwrite table cp_app_costoproducir.cp_operarios partition(entidadlegal_id) select tmp.* from cp_app_costoproducir.cp_operarios tmp join (select periodo, entidadlegal_id, mf_organizacion_id, centrocostos_id, linea_prod_id, turno_id, mf_producto_id, tipomedida_id, max(storeday) as first_record from cp_app_costoproducir.cp_operarios group by periodo, entidadlegal_id, mf_organizacion_id, centrocostos_id, linea_prod_id, turno_id, mf_producto_id, tipomedida_id) sec on tmp.periodo = sec.periodo and tmp.entidadlegal_id = sec.entidadlegal_id and tmp.mf_organizacion_id = sec.mf_organizacion_id and tmp.centrocostos_id = sec.centrocostos_id and tmp.linea_prod_id = sec.linea_prod_id and tmp.turno_id = sec.turno_id and tmp.mf_producto_id = sec.mf_producto_id and tmp.tipomedida_id = sec.tipomedida_id and tmp.storeday = sec.first_record;

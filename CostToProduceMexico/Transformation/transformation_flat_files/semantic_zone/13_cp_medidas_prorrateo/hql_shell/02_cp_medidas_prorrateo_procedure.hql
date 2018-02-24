-- Se eliminan previos de ejecucion de acuerdo al periodo a ejecutar en mf_produccion
insert overwrite table cp_app_costoproducir.cp_medidas_prorrateo partition(entidadlegal_id)
    select p.* from cp_app_costoproducir.cp_medidas_prorrateo p
    left join 
        (select 
            0 as mf_organizacion_id ,
            '0' as planta_id ,
            0 as mf_producto_id) tmp on
        p.mf_organizacion_id = tmp.mf_organizacion_id and p.planta_id = tmp.planta_id and p.planta_id = tmp.planta_id
        where tmp.mf_organizacion_id is null
    union all
    select 
    '1900-04' as periodo
    ,0 as mf_organizacion_id
    ,0 as planta_id
    ,0 as linea_prod_id
    ,0 as turno_id
    ,0 as mf_producto_id
    ,0 as tipomedida_id
    ,0 as turnos_linea_default
    ,0 as turnos_linea_produccion
    ,0 as turnos_planta_asignados
    ,0 as turnos_planta_produccion
    ,0 as factor
    ,0 as medida_factor
    ,0 as medida_producto
    ,0 as medida_turno
    ,0 as medida_linea
    ,0 as medida_planta
    ,0 as medida_entidadlegal
    ,0 as fecha_carga
    ,from_unixtime(unix_timestamp()) as storeday
    ,el.entidadlegal_id
    from (select distinct entidadlegal_id from cp_app_costoproducir.cp_medidas_prorrateo) el;
    
insert overwrite table cp_app_costoproducir.cp_medidas_prorrateo partition(entidadlegal_id)
  select * from cp_app_costoproducir.cp_medidas_prorrateo
  where periodo != '${hiveconf:speriod}';

-- set paso = 7
-- obtenemos medida de prorrateo de kilos producidos   
insert into table cp_app_costoproducir.cp_medidas_prorrateo partition(entidadlegal_id)
select
     pr.periodo
     ,pr.mf_organizacion_id
     ,pr.planta_id
     ,pr.linea_prod_id 
     ,pr.turno_id
     ,pr.mf_producto_id
     ,pr.tipomedida_id    
     ,t.turnos_linea_default
     ,t.turnos_linea_produccion
     ,t.turnos_planta_asignados
     ,t.turnos_planta_produccion
     ,pr.factor
     ,case    
          when sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following) <> 0 then (sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following) /  case when sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following) = 0 then null else sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following) end)
          when sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following) = 0 and sum(abs(pr.total_medida)) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id rows between unbounded preceding and unbounded following) = 0 then 0                         
          else abs(sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following) / sum(abs(pr.total_medida)) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id rows between unbounded preceding and unbounded following))                                     
     end  as medida_factor                                     
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following)  as medida_producto                                  
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following)  as medida_turno
     ,case
          when pr.entidadlegal_id = '089' then                          
               case       
                    when cast(abs(sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)) as decimal(18,6)) > 0 then sum(cast(pr.total_medida as decimal (18,6))) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)  
                    else 0   
               end        
          else sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)                                      
     end as medida_linea     
              
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id rows between unbounded preceding and unbounded following)  as medida_planta                                      
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id rows between unbounded preceding and unbounded following)  as medida_entidadlegal     
     ,from_unixtime(unix_timestamp()) as fecha_carga
     ,pr.storeday
     ,pr.entidadlegal_id
from          
     (

          select
               pr.periodo                                      
               ,pr.entidadlegal_id                             
               ,pr.mf_organizacion_id                          
               ,pr.planta_id                                   
               ,pr.linea_prod_id                               
               ,pr.turno_id                                    
               ,pr.mf_producto_id                              
               ,coalesce(p.gramaje,-1) as factor               
               ,1 as tipomedida_id
               ,sum(pr.total_registrado * coalesce(p.gramaje,0)) as total_medida
               ,max(pr.storeday) as storeday
          from
               (
                    
                   select                                     
                         substr(pr.fecha,1, 7) as periodo
                         ,pr.entidadlegal_id                   
                         ,pr.mf_organizacion_id                
                         ,pr.planta_id                         
                         ,pr.linea_prod_id                     
                         ,pr.turno_id                          
                         ,pr.mf_producto_id                    
                         ,sum(pr.total_registrado) as total_registrado
                         ,max(pr.storeday) as storeday
                    from cp_view.v_mf_produccion pr
                    left join
                    (
                         select vpm2.periodo, vpm2.entidadlegal_id, vpm2.mf_organizacion_id, vpm2.mf_producto_id 
                         from cp_app_costoproducir.v_producto_maquilado vpm2
                         where vpm2.periodo = '${hiveconf:speriod}' and cast(vpm2.entidadlegal_id as int) in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
                         group by vpm2.periodo, vpm2.entidadlegal_id, vpm2.mf_organizacion_id, vpm2.mf_producto_id
                    ) vpm
                    on vpm.periodo = substr(pr.fecha,1,7) and vpm.entidadlegal_id = pr.entidadlegal_id and vpm.mf_organizacion_id = pr.mf_organizacion_id and vpm.mf_producto_id = pr.mf_producto_id

                    where pr.linea_prod_id <> -1 and pr.total_registrado <> 0                                    
                         and pr.fecha between concat('${hiveconf:speriod}','-01') and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)  
                         and cast(pr.entidadlegal_id as int) in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
                         and vpm.periodo is null and vpm.entidadlegal_id is null and vpm.mf_organizacion_id is null and vpm.mf_producto_id is null
                    group by substr(pr.fecha,1, 7), pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id
                    having sum(pr.total_registrado) <> 0
                    
               ) pr    
               left outer join cp_app_costoproducir.v_mf_producto_organizacion p                            
                    on pr.entidadlegal_id     = p.entidadlegal_id 
                    and pr.mf_organizacion_id = p.mf_organizacion_id     
                    and pr.planta_id          = p.planta_id    
                    and pr.mf_producto_id     = p.mf_producto_id  
          where pr.periodo = '${hiveconf:speriod}' and cast(pr.entidadlegal_id as int) in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)         
          group by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id, coalesce(p.gramaje,-1), 1
         
     ) pr     
     join 
          ( 

               select
                    substr(pr.fecha, 1, 7)  as periodo
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
                                   substr(pr.fecha, 1, 7) as periodo
                                   ,pr.entidadlegal_id
                                   ,pr.mf_organizacion_id
                                   ,pr.planta_id
                                   ,count(distinct pr.linea_prod_id) * max(td.total_turnos_default) as turnos_planta_asignados
                                   ,count(distinct concat(pr.linea_prod_id, pr.turno_id) )   as turnos_planta_produccion
                                   ,max(td.total_turnos_default) as turnos_default
                              from cp_view.v_mf_produccion pr
                                   inner join
                                        (
                                             select
                                                  t.entidadlegal_id
                                                  ,count (distinct t.turno_id) as total_turnos_default
                                             from cp_dwh_mf.mf_turno_default t
                                             group by t.entidadlegal_id
                                        ) td on trim(pr.entidadlegal_id) = trim(td.entidadlegal_id)
                              where pr.linea_prod_id <> -1 and pr.total_registrado <> 0
                                   and pr.fecha >= concat('${hiveconf:speriod}','-01') and pr.fecha <= date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                                   and pr.entidadlegal_id in  (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
                              group by substr(pr.fecha, 1, 7), pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id
                              having (count(distinct pr.linea_prod_id) * max(td.total_turnos_default))  <> 0
                         ) tot on substr(pr.fecha, 1, 7) = tot.periodo and pr.entidadlegal_id = tot.entidadlegal_id and pr.mf_organizacion_id = tot.mf_organizacion_id and pr.planta_id = tot.planta_id
               where pr.linea_prod_id <> -1 and pr.total_registrado <> 0
                    and pr.fecha >= concat('${hiveconf:speriod}','-01') and pr.fecha <= date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                    and pr.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
               group by substr(pr.fecha, 1, 7), pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id, pr.linea_prod_id, tot.turnos_planta_asignados, tot.turnos_planta_produccion, tot.turnos_default
               having count(distinct pr.turno_id) <> 0  
                   
          ) t 
     on pr.periodo = t.periodo and pr.entidadlegal_id = t.entidadlegal_id and pr.mf_organizacion_id = t.mf_organizacion_id and pr.planta_id = t.planta_id and pr.linea_prod_id = t.linea_prod_id;

-- set paso = 8
-- obtenemos medida de prorrateo de tiempo de produccion       
insert into table cp_app_costoproducir.cp_medidas_prorrateo partition(entidadlegal_id)
select
     pr.periodo 
     ,pr.mf_organizacion_id                                    
     ,pr.planta_id     
     ,pr.linea_prod_id 
     ,pr.turno_id
     ,pr.mf_producto_id
     ,pr.tipomedida_id    
     ,t.turnos_linea_default                                   
     ,t.turnos_linea_produccion                                
     ,t.turnos_planta_asignados                                
     ,t.turnos_planta_produccion                               
     ,pr.factor
     ,case    
          when sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following) <> 0 then (sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following) /  case when sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following) = 0 then null else sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following) end)
          when sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following) = 0 and sum(abs(pr.total_medida)) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id rows between unbounded preceding and unbounded following) = 0 then 0                         
          else abs(sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following) / sum(abs(pr.total_medida)) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id rows between unbounded preceding and unbounded following))                                     
     end  as medida_factor                                     
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following)  as medida_producto                                  
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following)  as medida_turno
     ,case
          when pr.entidadlegal_id = '089' then                          
               case       
                    when cast(abs(sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)) as decimal(18,6)) > 0 then sum(cast(pr.total_medida as decimal (18,6))) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)  
                    else 0   
               end        
          else sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)                                      
     end as medida_linea     
              
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id rows between unbounded preceding and unbounded following)  as medida_planta                                      
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id rows between unbounded preceding and unbounded following)  as medida_entidadlegal     
     ,from_unixtime(unix_timestamp()) as fecha_carga
     ,pr.storeday
     ,pr.entidadlegal_id
from
     (        

          select
               pr.periodo                                      
               ,pr.entidadlegal_id                             
               ,pr.mf_organizacion_id                          
               ,pr.planta_id                                   
               ,pr.linea_prod_id                               
               ,pr.turno_id                                    
               ,pr.mf_producto_id                              
               ,2 as tipomedida_id
               ,pr.ritmo as factor                             
               ,sum(pr.total_registrado / pr.ritmo ) as total_medida 
               ,max(pr.storeday) as storeday
          from
               (
                    
                    select
                         substr(pr.fecha, 1, 7) as periodo
                         ,pr.entidadlegal_id
                         ,pr.mf_organizacion_id
                         ,pr.planta_id
                         ,pr.linea_prod_id
                         ,pr.turno_id
                         ,pr.mf_producto_id
                         ,max(pr.ritmo) as ritmo
                         ,sum(pr.total_registrado) as total_registrado
                         ,max(pr.storeday) as storeday
                    from cp_view.v_mf_produccion pr
                    left join (
                              select vpm2.periodo, vpm2.entidadlegal_id, vpm2.mf_organizacion_id, vpm2.mf_producto_id
                              from cp_app_costoproducir.v_producto_maquilado vpm2
                              where vpm2.periodo = '${hiveconf:speriod}' and vpm2.entidadlegal_id in (select ea1.entidadlegal_id from cp_view.v_entidadlegal_activas ea1 group by ea1.entidadlegal_id)
                              group by vpm2.periodo, vpm2.entidadlegal_id, vpm2.mf_organizacion_id, vpm2.mf_producto_id
                         ) vpm1 on substr(pr.fecha, 1, 7) = vpm1.periodo and pr.entidadlegal_id = vpm1.entidadlegal_id and pr.mf_organizacion_id = vpm1.mf_organizacion_id and pr.mf_producto_id = vpm1.mf_producto_id
                         where pr.linea_prod_id <> -1 and pr.total_registrado <> 0
                              and pr.fecha between concat('${hiveconf:speriod}','-01') and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                              and pr.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
                              and vpm1.periodo is null and vpm1.entidadlegal_id is null and vpm1.mf_organizacion_id is null and vpm1.mf_producto_id is null
                    group by substr(pr.fecha, 1, 7), pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id
                    
               ) pr
          where pr.periodo = '${hiveconf:speriod}' and pr.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)         
          group by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id, 2, pr.ritmo
          having sum(pr.total_registrado / pr.ritmo ) <> 0
     ) pr     
     inner join 
          (
               select
                    substr(pr.fecha, 1,7)  as periodo
                    ,pr.entidadlegal_id
                    ,pr.mf_organizacion_id
                    ,pr.planta_id
                    ,pr.linea_prod_id
                    ,tot.turnos_planta_asignados
                    ,tot.turnos_planta_produccion
                    ,tot.turnos_default           as turnos_linea_default
                    ,count(distinct pr.turno_id)  as turnos_linea_produccion
               from cp_view.v_mf_produccion pr
               join
                    (
                         select
                              substr(pr.fecha, 1,7)  as periodo
                              ,pr.entidadlegal_id
                              ,pr.mf_organizacion_id
                              ,pr.planta_id
                              ,count(distinct pr.linea_prod_id) * max(td.total_turnos_default) as turnos_planta_asignados
                              ,count(distinct concat(pr.linea_prod_id, pr.turno_id) ) as turnos_planta_produccion
                              ,max(td.total_turnos_default) as turnos_default
                         from cp_view.v_mf_produccion pr
                              inner join
                                   (
                                        select
                                             t.entidadlegal_id
                                             ,count (distinct t.turno_id) as total_turnos_default
                                        from cp_dwh_mf.mf_turno_default t
                                        group by t.entidadlegal_id
                                   ) td on trim(pr.entidadlegal_id) = trim(td.entidadlegal_id)
                         where pr.linea_prod_id <> -1 and pr.total_registrado <> 0
                              and pr.fecha between concat('${hiveconf:speriod}','-01') and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                              and pr.entidadlegal_id in  (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
                         group by substr(pr.fecha, 1,7),pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id
                         having (count(distinct pr.linea_prod_id) * max(td.total_turnos_default))  <> 0
                    ) tot on substr(pr.fecha, 1,7) = tot.periodo and pr.entidadlegal_id = tot.entidadlegal_id and pr.mf_organizacion_id = tot.mf_organizacion_id and pr.planta_id = tot.planta_id
               where pr.linea_prod_id <> -1 and pr.total_registrado <> 0
                    and pr.fecha between concat('${hiveconf:speriod}','-01') and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                    and pr.entidadlegal_id in  (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
               group by substr(pr.fecha, 1,7), pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id, pr.linea_prod_id, tot.turnos_planta_asignados, tot.turnos_planta_produccion, tot.turnos_default
               having count(distinct pr.turno_id) <> 0
          ) t 
     on pr.periodo = t.periodo and pr.entidadlegal_id = t.entidadlegal_id and pr.mf_organizacion_id = t.mf_organizacion_id and pr.planta_id = t.planta_id and pr.linea_prod_id = t.linea_prod_id;

-- set paso = 9
-- obtenemos medida de prorrateo mxn costo estandard
insert into table cp_app_costoproducir.cp_medidas_prorrateo partition(entidadlegal_id)
select
     pr.periodo
     ,pr.mf_organizacion_id
     ,pr.planta_id
     ,pr.linea_prod_id
     ,pr.turno_id
     ,pr.mf_producto_id
     ,pr.tipomedida_id
     ,t.turnos_linea_default
     ,t.turnos_linea_produccion
     ,t.turnos_planta_asignados
     ,t.turnos_planta_produccion
     ,pr.factor
     ,case
          when sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following) <> 0 then (sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following) /  case when sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following) = 0 then null else sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following) end)
          when sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following) = 0 and sum(abs(pr.total_medida)) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id rows between unbounded preceding and unbounded following) = 0 then 0
          else abs(sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following) / sum(abs(pr.total_medida)) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id rows between unbounded preceding and unbounded following))
     end  as medida_factor
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following)  as medida_producto
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following)  as medida_turno
     ,case
          when pr.entidadlegal_id = '089' then
               case
                    when cast(abs(sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)) as decimal(18,6)) > 0 then sum(cast(pr.total_medida as decimal (18,6))) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)
                    else 0
               end
          else sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)
     end as medida_linea
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id rows between unbounded preceding and unbounded following)  as medida_planta
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id rows between unbounded preceding and unbounded following)  as medida_entidadlegal
     ,from_unixtime(unix_timestamp()) as fecha_carga
     ,pr.storeday
     ,pr.entidadlegal_id
from
     (
          select
               pr.periodo
               ,pr.entidadlegal_id
               ,pr.mf_organizacion_id
               ,pr.planta_id
               ,pr.linea_prod_id
               ,pr.turno_id
               ,pr.mf_producto_id
               ,coalesce(cp.costo_pt,-1) as factor
               ,3 as tipomedida_id
               ,sum(pr.total_registrado * coalesce(cp.costo_pt,0))  as total_medida
               ,max(pr.storeday) as storeday
          from
               (
                    select
                         substr(pr.fecha,1, 7) as periodo
                         ,pr.entidadlegal_id
                         ,pr.mf_organizacion_id
                         ,pr.planta_id
                         ,pr.linea_prod_id
                         ,pr.turno_id
                         ,pr.mf_producto_id
                         ,sum(pr.total_registrado) as total_registrado
                         ,max(pr.storeday) as storeday
                    from cp_view.v_mf_produccion pr
                    left join
                    (
                         select vpm2.periodo, vpm2.entidadlegal_id, vpm2.mf_organizacion_id, vpm2.mf_producto_id
                         from cp_app_costoproducir.v_producto_maquilado vpm2
                         where vpm2.periodo = '${hiveconf:speriod}' and vpm2.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
                         group by vpm2.periodo, vpm2.entidadlegal_id, vpm2.mf_organizacion_id, vpm2.mf_producto_id
                    ) vpm
                    on vpm.periodo = substr(pr.fecha,1,7) and vpm.entidadlegal_id = pr.entidadlegal_id and vpm.mf_organizacion_id = pr.mf_organizacion_id and vpm.mf_producto_id = pr.mf_producto_id 
                    where pr.linea_prod_id <> -1 and pr.total_registrado <> 0
                         and pr.fecha between concat('${hiveconf:speriod}','-01') and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                         and pr.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
                         and vpm.periodo is null and vpm.entidadlegal_id is null and vpm.mf_organizacion_id is null and vpm.mf_producto_id is null
                    group by substr(pr.fecha,1, 7), pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id
                    having sum(pr.total_registrado) <> 0
               )pr
               left outer join
                    (
                         select cp1.periodo, cp1.entidadlegal_id, cp1.mf_organizacion_id, cp1.planta_id, cp1.mf_producto_id,  sum(cp1.costo) as costo_pt
                         from cp_app_costoproducir.v_mf_costo_prod cp1
                         where cp1.tipo_costo_id in (1,2,3,4,9)
                              and cp1.entidadlegal_id in  (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id) and periodo = '${hiveconf:speriod}'
                         group by cp1.periodo, cp1.entidadlegal_id, cp1.mf_organizacion_id, cp1.planta_id, cp1.mf_producto_id
                    ) cp
               on pr.entidadlegal_id = cp.entidadlegal_id
                    and pr.mf_organizacion_id = cp.mf_organizacion_id
                    and pr.planta_id = cp.planta_id
                    and pr.mf_producto_id = cp.mf_producto_id
                    and pr.periodo = cp.periodo
          where pr.periodo = '${hiveconf:speriod}' and pr.entidadlegal_id in  (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
          group by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id, coalesce(cp.costo_pt,-1), 3
     ) pr
     inner join
          (
               select
                    substr(pr.fecha, 1,7)  as periodo
                    ,pr.entidadlegal_id
                    ,pr.mf_organizacion_id
                    ,pr.planta_id
                    ,pr.linea_prod_id
                    ,tot.turnos_planta_asignados
                    ,tot.turnos_planta_produccion
                    ,tot.turnos_default           as turnos_linea_default
                    ,count(distinct pr.turno_id)  as turnos_linea_produccion
               from cp_view.v_mf_produccion pr
               join
                    (
                         select
                              substr(pr.fecha, 1,7)  as periodo
                              ,pr.entidadlegal_id
                              ,pr.mf_organizacion_id
                              ,pr.planta_id
                              ,count(distinct pr.linea_prod_id) * max(td.total_turnos_default) as turnos_planta_asignados
                              ,count(distinct concat(pr.linea_prod_id, pr.turno_id) ) as turnos_planta_produccion
                              ,max(td.total_turnos_default) as turnos_default
                         from cp_view.v_mf_produccion pr
                              inner join
                                   (
                                        select
                                             t.entidadlegal_id
                                             ,count (distinct t.turno_id) as total_turnos_default
                                        from cp_dwh_mf.mf_turno_default t
                                        group by t.entidadlegal_id
                                   ) td on trim(pr.entidadlegal_id) = trim(td.entidadlegal_id)
                         where pr.linea_prod_id <> -1 and pr.total_registrado <> 0
                              and pr.fecha between concat('${hiveconf:speriod}','-01') and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                              and pr.entidadlegal_id in  (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
                         group by substr(pr.fecha, 1,7),pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id
                         having (count(distinct pr.linea_prod_id) * max(td.total_turnos_default))  <> 0
                    ) tot on substr(pr.fecha, 1,7) = tot.periodo and pr.entidadlegal_id = tot.entidadlegal_id and pr.mf_organizacion_id = tot.mf_organizacion_id and pr.planta_id = tot.planta_id
               where pr.linea_prod_id <> -1 and pr.total_registrado <> 0
                    and pr.fecha between concat('${hiveconf:speriod}','-01') and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                    and pr.entidadlegal_id in  (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
               group by substr(pr.fecha, 1,7), pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id, pr.linea_prod_id, tot.turnos_planta_asignados, tot.turnos_planta_produccion, tot.turnos_default
               having count(distinct pr.turno_id) <> 0
          ) t
     on pr.periodo = t.periodo and pr.entidadlegal_id = t.entidadlegal_id and pr.mf_organizacion_id = t.mf_organizacion_id and pr.planta_id = t.planta_id and pr.linea_prod_id = t.linea_prod_id;

-- set paso = 10
-- obtenemos medida de prorrateo costo costo real
insert into table cp_app_costoproducir.cp_medidas_prorrateo partition(entidadlegal_id)
select
     pr.periodo
     ,pr.mf_organizacion_id
     ,pr.planta_id
     ,pr.linea_prod_id
     ,pr.turno_id
     ,pr.mf_producto_id
     ,pr.tipomedida_id
     ,t.turnos_linea_default
     ,t.turnos_linea_produccion
     ,t.turnos_planta_asignados
     ,t.turnos_planta_produccion
     ,pr.factor
     ,case
          when sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following) <> 0 then (sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following) /  case when sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following) = 0 then null else sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following) end)
          when sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following) = 0 and sum(abs(pr.total_medida)) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id rows between unbounded preceding and unbounded following) = 0 then 0
          else abs(sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following) / sum(abs(pr.total_medida)) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id rows between unbounded preceding and unbounded following))
     end  as medida_factor
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following)  as medida_producto
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following)  as medida_turno
     ,case
          when pr.entidadlegal_id = '089' then
               case
                    when cast(abs(sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)) as decimal(22,6)) > 0 then sum(cast(pr.total_medida as decimal (22,6))) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)
                    else 0
               end
          else sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)
     end as medida_linea
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id rows between unbounded preceding and unbounded following)  as medida_planta
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id rows between unbounded preceding and unbounded following)  as medida_entidadlegal
     ,from_unixtime(unix_timestamp()) as fecha_carga
     ,pr.storeday
     ,pr.entidadlegal_id
from
     (
          select
               pr.periodo
               ,pr.entidadlegal_id
               ,pr.mf_organizacion_id
               ,pr.planta_id
               ,pr.linea_prod_id
               ,pr.turno_id
               ,pr.mf_producto_id
               ,coalesce(f.costo_pt,-1) as factor
               ,4 as tipomedida_id
               ,sum(coalesce(f.costo_pt,0) * pr.total_registrado) as total_medida
               ,max(pr.storeday) as storeday
          from
               (
                    select
                         substr(pr.fecha,1, 7) as periodo
                         ,pr.entidadlegal_id
                         ,pr.mf_organizacion_id
                         ,pr.planta_id
                         ,pr.linea_prod_id
                         ,pr.turno_id
                         ,pr.mf_producto_id
                         ,sum(pr.total_registrado) as total_registrado
                         ,max(pr.storeday) as storeday
                    from cp_view.v_mf_produccion pr
                    left join
                    (
                         select vpm2.periodo, vpm2.entidadlegal_id, vpm2.mf_organizacion_id, vpm2.mf_producto_id
                         from cp_app_costoproducir.v_producto_maquilado vpm2
                         where vpm2.periodo = '${hiveconf:speriod}' and vpm2.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
                         group by vpm2.periodo, vpm2.entidadlegal_id, vpm2.mf_organizacion_id, vpm2.mf_producto_id
                    ) vpm
                    on vpm.periodo = substr(pr.fecha,1,7) and vpm.entidadlegal_id = pr.entidadlegal_id and vpm.mf_organizacion_id = pr.mf_organizacion_id and vpm.mf_producto_id = pr.mf_producto_id
                    where pr.linea_prod_id <> -1 and pr.total_registrado <> 0
                         and pr.fecha between concat('${hiveconf:speriod}','-01') and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                         and pr.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
                         and vpm.periodo is null and vpm.entidadlegal_id is null and vpm.mf_organizacion_id is null and vpm.mf_producto_id is null
                    group by substr(pr.fecha,1, 7), pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id
                    having sum(pr.total_registrado) <> 0
               ) pr
               left outer join
                    (
                         select f.periodo, f.entidadlegal_id, f.mf_organizacion_id, f.planta_id, f.mf_producto_id, sum (f.cantidad * f.costoreal) as costo_pt
                         from cp_app_costoproducir.v_mf_formulas f
                         where f.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id) and periodo = '${hiveconf:speriod}'
                         group by f.periodo, f.entidadlegal_id, f.mf_organizacion_id, f.planta_id, f.mf_producto_id
                    ) f
               on pr.entidadlegal_id = f.entidadlegal_id and pr.mf_organizacion_id = f.mf_organizacion_id and  pr.planta_id = f.planta_id and  pr.mf_producto_id = f.mf_producto_id and pr.periodo = f.periodo
          where pr.periodo = '${hiveconf:speriod}' and pr.entidadlegal_id in  (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
          group by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id, coalesce(f.costo_pt,-1)
     ) pr
     inner join
          (
               select
                    substr(pr.fecha, 1,7)  as periodo
                    ,pr.entidadlegal_id
                    ,pr.mf_organizacion_id
                    ,pr.planta_id
                    ,pr.linea_prod_id
                    ,tot.turnos_planta_asignados
                    ,tot.turnos_planta_produccion
                    ,tot.turnos_default           as turnos_linea_default
                    ,count(distinct pr.turno_id)  as turnos_linea_produccion
               from cp_view.v_mf_produccion pr
               join
                    (
                         select
                              substr(pr.fecha, 1,7)  as periodo
                              ,pr.entidadlegal_id
                              ,pr.mf_organizacion_id
                              ,pr.planta_id
                              ,count(distinct pr.linea_prod_id) * max(td.total_turnos_default) as turnos_planta_asignados
                              ,count(distinct concat(pr.linea_prod_id, pr.turno_id) ) as turnos_planta_produccion
                              ,max(td.total_turnos_default) as turnos_default
                         from cp_view.v_mf_produccion pr
                              inner join
                                   (
                                        select
                                             t.entidadlegal_id
                                             ,count (distinct t.turno_id) as total_turnos_default
                                        from cp_dwh_mf.mf_turno_default t
                                        group by t.entidadlegal_id
                                   ) td on trim(pr.entidadlegal_id) = trim(td.entidadlegal_id)
                         where pr.linea_prod_id <> -1 and pr.total_registrado <> 0
                              and pr.fecha between concat('${hiveconf:speriod}','-01') and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                              and pr.entidadlegal_id in  (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
                         group by substr(pr.fecha, 1,7),pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id
                         having (count(distinct pr.linea_prod_id) * max(td.total_turnos_default))  <> 0
                    ) tot on substr(pr.fecha, 1,7) = tot.periodo and pr.entidadlegal_id = tot.entidadlegal_id and pr.mf_organizacion_id = tot.mf_organizacion_id and pr.planta_id = tot.planta_id
               where pr.linea_prod_id <> -1 and pr.total_registrado <> 0
                    and pr.fecha between concat('${hiveconf:speriod}','-01') and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                    and pr.entidadlegal_id in  (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
               group by substr(pr.fecha, 1,7), pr.entidadlegal_id, pr.mf_organizacion_id, pr.planta_id, pr.linea_prod_id, tot.turnos_planta_asignados, tot.turnos_planta_produccion, tot.turnos_default
               having count(distinct pr.turno_id) <> 0
          ) t
     on pr.periodo = t.periodo and pr.entidadlegal_id = t.entidadlegal_id and pr.mf_organizacion_id = t.mf_organizacion_id and pr.planta_id = t.planta_id and pr.linea_prod_id = t.linea_prod_id;

-- set paso = 11
-- obtenemos metrica pzas producidas
insert into table cp_app_costoproducir.cp_medidas_prorrateo partition(entidadlegal_id)
select
     pr.periodo 
     ,pr.mf_organizacion_id                                    
     ,pr.planta_id 
     ,pr.linea_prod_id                                         
     ,pr.turno_id
     ,pr.mf_producto_id                                        
     ,pr.tipomedida_id
     ,t.turnos_linea_default                                   
     ,t.turnos_linea_produccion                                
     ,t.turnos_planta_asignados                                
     ,t.turnos_planta_produccion                               
     ,pr.factor 
     ,case    
          when (sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following)) <> 0 then ((sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following)) / (case when (sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following)) = 0 then null else (sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following))end))                               
          when (sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following)) = 0 and sum(abs(pr.total_medida)) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id rows between unbounded preceding and unbounded following) = 0 then 0                         
          else abs((sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following)) / sum(abs(pr.total_medida)) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id rows between unbounded preceding and unbounded following))                                     
     end  as medida_factor                                     
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id, pr.mf_producto_id  rows between unbounded preceding and unbounded following)  as medida_producto                                  
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id, pr.turno_id  rows between unbounded preceding and unbounded following)  as medida_turno       
     ,case          
          when pr.entidadlegal_id = '089' then                          
               case       
                    when cast(abs(sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)) as decimal(18,6)) > 0 then sum(cast(pr.total_medida as decimal (18,6))) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)  
                    else 0   
               end        
          else sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id, pr.linea_prod_id rows between unbounded preceding and unbounded following)                                      
     end as medida_linea 
              
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id, pr.mf_organizacion_id rows between unbounded preceding and unbounded following)  as medida_planta                                      
     ,sum(pr.total_medida) over (partition by pr.periodo, pr.entidadlegal_id rows between unbounded preceding and unbounded following)  as medida_entidadlegal     
     ,from_unixtime(unix_timestamp()) as fecha_carga
     ,pr.storeday
     ,pr.entidadlegal_id
from          
     (        
          select
               cast(substr(pr.fecha,1,7) as string) as periodo                                      
               ,pr.entidadlegal_id                             
               ,pr.mf_organizacion_id
               ,pr.planta_id
               ,pr.linea_prod_id
               ,pr.turno_id
               ,pr.mf_producto_id
               ,sum(pr.total_registrado) as factor                
               ,5 as tipomedida_id -- pzas producidas tabla cp_tipo_medida                                       
               ,sum(pr.total_registrado) as total_medida 
               ,max(pr.storeday) as storeday 
          from cp_view.v_mf_produccion pr    
          where pr.linea_prod_id <> -1 and pr.total_registrado <> 0
               and pr.fecha between concat('${hiveconf:speriod}','-01') and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
               and pr.entidadlegal_id in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)                                 
          group by substr(pr.fecha,1,7),pr.entidadlegal_id,pr.mf_organizacion_id,pr.planta_id,pr.linea_prod_id,pr.turno_id,pr.mf_producto_id
          having sum(pr.total_registrado) <> 0                    
     ) pr     
     inner join 
          (   
               --obtenemos datos del turno                     
               select
                    cast(substr(pr.fecha,1,7) as string) as periodo                                 
                    ,pr.entidadlegal_id
                    ,pr.mf_organizacion_id
                    ,pr.planta_id
                    ,pr.linea_prod_id
                    ,tot.turnos_planta_asignados
                    ,tot.turnos_planta_produccion
                    ,tot.turnos_default as turnos_linea_default
                    ,count(distinct pr.turno_id) as turnos_linea_produccion
               from cp_view.v_mf_produccion pr  
                    inner join                                 
                         (                                     
                              --   obtenemos el total de turnos que produjeron y asignados por planta                 
                              select
                                   cast(substr(pr.fecha,1,7) as string) as periodo                       
                                   ,pr.entidadlegal_id
                                   ,pr.mf_organizacion_id              
                                   ,pr.planta_id                       
                                   ,count(distinct pr.linea_prod_id) * max(td.total_turnos_default) as turnos_planta_asignados
                                   ,count(distinct concat(pr.linea_prod_id, pr.turno_id) ) as turnos_planta_produccion
                                   ,max(td.total_turnos_default) as turnos_default 
                              from cp_view.v_mf_produccion pr
                                   inner join 
                                        (
                                             select
                                                  t.entidadlegal_id
                                                  ,count (distinct t.turno_id) as total_turnos_default
                                             from cp_dwh_mf.mf_turno_default t
                                             group by t.entidadlegal_id
                                        ) td on trim(pr.entidadlegal_id) = trim(td.entidadlegal_id)
                              where pr.linea_prod_id <> -1 and pr.total_registrado <> 0                               
                                   and pr.fecha between concat('${hiveconf:speriod}','-01') and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                                   and pr.entidadlegal_id in (select entidadlegal_id 
                                        from cp_view.v_entidadlegal_activas group by entidadlegal_id)               
                              group by substr(pr.fecha,1,7),pr.entidadlegal_id,pr.mf_organizacion_id,pr.planta_id
                              having (count(distinct pr.linea_prod_id) * max(td.total_turnos_default)) <> 0                   
                         ) tot on substr(pr.fecha,1,7) = tot.periodo 
                              and pr.entidadlegal_id = tot.entidadlegal_id 
                              and pr.mf_organizacion_id = tot.mf_organizacion_id 
                              and pr.planta_id = tot.planta_id    
               where pr.linea_prod_id <> -1 and pr.total_registrado <> 0                                         
                    and pr.fecha between concat('${hiveconf:speriod}','-01') and date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                    and pr.entidadlegal_id in  (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)                           
              group by substr(pr.fecha,1,7),pr.entidadlegal_id,pr.mf_organizacion_id,pr.planta_id,pr.linea_prod_id
              ,tot.turnos_planta_asignados,tot.turnos_planta_produccion,tot.turnos_default
              having count(distinct pr.turno_id) <> 0          
          ) t 
     on pr.periodo = t.periodo 
     and pr.entidadlegal_id = t.entidadlegal_id 
     and pr.mf_organizacion_id = t.mf_organizacion_id 
     and pr.planta_id = t.planta_id 
     and pr.linea_prod_id = t.linea_prod_id;

     
-- set paso = 12
-- Obtenemos metrica Metros Cuadrados                          
INSERT INTO table cp_app_costoproducir.CP_Medidas_Prorrateo partition(entidadlegal_id)
  SELECT        
     ML.Periodo
     ,ML.MF_Organizacion_ID
     ,ML.Planta_ID
     ,ML.Linea_Prod_ID
     ,-1 AS Turno_ID
     ,-1 AS MF_Producto_ID
     ,6 AS TipoMedida_ID -- Medida de Metros Cuadrados  
     ,T.Turnos_Linea_Default                                   
     ,T.Turnos_Linea_Produccion                                
     ,T.Turnos_Planta_Asignados                                
     ,T.Turnos_Planta_Produccion                               
     ,ML.Metros2 AS Factor                                  
     ,((ML.Metros2/T.Turnos_Linea_Produccion) / ML.Metros2) AS Medida_Factor                                     
     ,0 AS Medida_Producto                               
     ,ML.Metros2/T.Turnos_Linea_Produccion AS Medida_Turno     
     ,ML.Metros2 AS Medida_Linea                               
     ,SUM(ML.Metros2) OVER (PARTITION BY ML.Periodo, ML.EntidadLegal_Id, ML.MF_Organizacion_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS Medida_Planta 
     ,SUM(ML.Metros2) OVER (PARTITION BY ML.Periodo, ML.EntidadLegal_Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS Medida_EntidadLegal          
     ,from_unixtime(unix_timestamp()) as fecha_carga
     ,from_unixtime(unix_timestamp()) as storeday
     ,ML.EntidadLegal_ID                                
FROM          
     (        
          SELECT
               CAST('${hiveconf:speriod}' AS STRING ) AS Periodo
               ,a.EntidadLegal_ID                                
               ,a.MF_Organizacion_ID                             
               ,a.Planta_ID                                      
               ,a.Linea_Prod_ID                                  
               ,SUM(a.Metros2) AS Metros2                        
          FROM cp_app_costoproducir.CP_Lineas_Prod_Metros a
          WHERE a.Fecha_Fin IS NULL
           AND a.EntidadLegal_ID IN (SELECT EntidadLegal_ID FROM cp_view.V_EntidadLegal_Activas GROUP BY EntidadLegal_ID)                                     
          GROUP BY a.EntidadLegal_ID,a.MF_Organizacion_ID,a.Planta_ID,a.Linea_Prod_ID
          HAVING SUM(a.Metros2) <> 0               
     ) ML     
     INNER JOIN 
          (   
               --Obtenemos datos del turno                     
               SELECT
                    SUBSTRING(PR.Fecha,1,7) AS Periodo                                 
                    ,PR.EntidadLegal_ID                        
                    ,PR.MF_Organizacion_ID                     
                    ,PR.Planta_ID                              
                    ,PR.Linea_Prod_ID                          
                    ,TOT.Turnos_Planta_Asignados               
                    ,TOT.Turnos_Planta_Produccion              
                    ,TOT.Turnos_Default AS Turnos_Linea_Default 
                    ,COUNT(DISTINCT PR.Turno_ID) AS Turnos_Linea_Produccion                                     
               FROM cp_view.v_mf_produccion PR  
                    INNER JOIN                                 
                         (                                     
                              --   Obtenemos el total de turnos que produjeron y asignados por planta                 
                              SELECT
                                   SUBSTRING(PR.Fecha,1,7) AS Periodo                       
                                   ,PR.EntidadLegal_ID                 
                                   ,PR.MF_Organizacion_ID              
                                   ,PR.Planta_ID                       
                                   ,COUNT(DISTINCT PR.Linea_Prod_ID) * MAX(TD.Total_Turnos_Default) AS Turnos_Planta_Asignados
                                   ,count(distinct concat(trim(cast(pr.linea_prod_id as string)),trim(cast(pr.turno_id as string)))) as turnos_planta_produccion
                                   ,MAX(TD.Total_Turnos_Default) AS Turnos_Default 
                              FROM cp_view.v_mf_produccion PR
                                   INNER JOIN 
                                        (
                                        SELECT
                                             T.EntidadLegal_ID
                                             ,COUNT (DISTINCT T.Turno_ID) AS Total_Turnos_Default
                                             FROM cp_dwh_mf.MF_Turno_Default T
                                             GROUP BY T.EntidadLegal_ID
                                        ) td on trim(pr.entidadlegal_id) = trim(td.entidadlegal_id)
                              WHERE PR.Linea_Prod_ID <> -1 AND PR.Total_Registrado <> 0                               
                                   AND PR.Fecha BETWEEN concat('${hiveconf:speriod}','-01') AND date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                                   AND PR.EntidadLegal_ID IN  (SELECT EntidadLegal_ID FROM cp_view.V_EntidadLegal_Activas GROUP BY EntidadLegal_ID)               
                              GROUP BY SUBSTRING(PR.Fecha,1,7),PR.EntidadLegal_ID,PR.MF_Organizacion_ID,PR.Planta_ID
                              HAVING (COUNT(DISTINCT PR.Linea_Prod_ID) * MAX(TD.Total_Turnos_Default))  <> 0                   
                         ) TOT ON SUBSTRING(PR.Fecha,1,7) = TOT.Periodo 
                         AND PR.EntidadLegal_ID = TOT.EntidadLegal_ID 
                         AND PR.MF_Organizacion_ID = TOT.MF_Organizacion_ID 
                         AND PR.Planta_ID = TOT.Planta_ID    
               WHERE PR.Linea_Prod_ID <> -1 AND PR.Total_Registrado <> 0                                         
                    AND PR.Fecha BETWEEN concat('${hiveconf:speriod}','-01') AND date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                    AND PR.EntidadLegal_ID IN  (SELECT EntidadLegal_ID 
                         FROM cp_view.V_EntidadLegal_Activas GROUP BY EntidadLegal_ID)                          
              GROUP BY SUBSTRING(PR.Fecha,1,7),PR.EntidadLegal_ID,PR.MF_Organizacion_ID,PR.Planta_ID,PR.Linea_Prod_ID,TOT.Turnos_Planta_Asignados
                    ,TOT.Turnos_Planta_Produccion,TOT.Turnos_Default
              HAVING COUNT(DISTINCT PR.Turno_ID) <> 0          
          ) T 
     ON ML.Periodo = T.Periodo 
     AND ML.EntidadLegal_ID = T.EntidadLegal_ID 
     AND ML.MF_Organizacion_ID = T.MF_Organizacion_ID 
     AND ML.Planta_ID = T.Planta_ID 
     AND ML.Linea_Prod_ID = T.Linea_Prod_ID;


-- set paso = 13
-- Obtenemos la metrica de Productos Producidos sin Línea      
INSERT INTO table cp_app_costoproducir.CP_Medidas_Prorrateo partition(entidadlegal_id)
    SELECT        
     PR.Periodo                                       
     ,PR.MF_Organizacion_ID                                    
     ,PR.Planta_ID 
     ,PR.Linea_Prod_ID                                         
     ,PR.Turno_ID
     ,PR.MF_Producto_ID                                        
     ,PR.TipoMedida_ID                                         
     ,0 AS Turnos_Linea_Default                                
     ,0 AS Turnos_Linea_Produccion                             
     ,T.Turnos_Planta_Asignados                                
     ,T.Turnos_Planta_Produccion                               
     ,PR.Factor 
     ,((SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID, PR.Turno_Id, PR.MF_Producto_ID  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))) / COALESCE((SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID, PR.Turno_Id  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)),0) AS Medida_Factor
     ,SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID, PR.Turno_Id, PR.MF_Producto_ID  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS Medida_Producto                                  
     ,SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID, PR.Turno_Id  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS Medida_Turno
     ,CASE          
          WHEN PR.EntidadLegal_ID = '089' THEN                          
               CASE       
                    WHEN CAST(ABS(SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS DECIMAL(18,6)) > 0 THEN SUM(CAST(PR.Total_Medida AS DECIMAL (18,6))) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  
                    ELSE 0   
               END        
          ELSE SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)                                      
     END AS Medida_Linea        
     ,SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS Medida_Planta                                      
     ,SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS Medida_EntidadLegal     
     ,from_unixtime(unix_timestamp()) as fecha_carga
     ,from_unixtime(unix_timestamp()) AS storeday
     ,PR.EntidadLegal_ID                                     
FROM          
     (        
          SELECT
          CAST(SUBSTRING(PR.Fecha,1,7) AS STRING) AS Periodo                                      
          ,PR.EntidadLegal_ID                             
          ,PR.MF_Organizacion_ID                          
          ,PR.Planta_ID                                   
          ,PR.Linea_Prod_ID                               
          ,-1 AS Turno_ID            
          ,PR.MF_Producto_ID                              
          ,SUM(PR.Total_Registrado) AS Factor              
          ,7 AS TipoMedida_ID -- Pzas Producidas sin línea Tabla CP_Tipo_Medida       
          ,SUM(PR.Total_Registrado) AS Total_Medida        
          FROM cp_view.V_MF_Produccion PR
          LEFT OUTER JOIN  (
                    SELECT vpm.Periodo, vpm.EntidadLegal_ID, vpm.MF_Organizacion_ID, vpm.MF_Producto_ID 
                    FROM cp_app_costoproducir.V_Producto_Maquilado vpm
                    WHERE vpm.Periodo = '${hiveconf:speriod}'  ---------------================>>>>>>>> :sPeriodo --- PARAMETRO
                    AND vpm.EntidadLegal_ID IN (SELECT EntidadLegal_ID FROM cp_view.V_EntidadLegal_Activas GROUP BY EntidadLegal_ID) 
                    GROUP BY vpm.Periodo, vpm.EntidadLegal_ID, vpm.MF_Organizacion_ID, vpm.MF_Producto_ID 
               ) pm
          ON CAST(SUBSTRING(PR.Fecha,1,7) AS STRING)=CAST(pm.Periodo AS STRING)
          AND PR.EntidadLegal_ID= pm.EntidadLegal_ID
          AND PR.MF_Organizacion_ID=pm.MF_Organizacion_ID
          AND PR.MF_Producto_ID=pm.MF_Producto_ID
          WHERE pm.Periodo IS NULL
          AND pm.EntidadLegal_ID IS NULL
          AND pm.MF_Organizacion_ID IS NULL
          AND pm.MF_Producto_ID IS NULL
          AND PR.Linea_Prod_ID = -1 
          --AND PR.EntidadLegal_ID='101'
          AND PR.Total_Registrado <> 0
          AND PR.Fecha BETWEEN concat('${hiveconf:speriod}','-01') AND date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1) 
          AND PR.EntidadLegal_ID IN (SELECT EntidadLegal_ID FROM cp_view.V_EntidadLegal_Activas GROUP BY EntidadLegal_ID)
          GROUP BY SUBSTRING(PR.Fecha,1,7),PR.EntidadLegal_ID,PR.MF_Organizacion_ID,PR.Planta_ID,PR.Linea_Prod_ID,-1,PR.MF_Producto_ID
          HAVING SUM(Total_Registrado) <> 0                
     ) PR     
     INNER JOIN 
          (   
               SELECT
                    SUBSTRING(PR.Fecha,1,7) AS Periodo                       
                    ,PR.EntidadLegal_ID                 
                    ,PR.MF_Organizacion_ID              
                    ,PR.Planta_ID                       
                    ,COUNT(DISTINCT PR.Linea_Prod_ID) * MAX(TD.Total_Turnos_Default) AS Turnos_Planta_Asignados
                    ,count(distinct concat(pr.linea_prod_id, pr.turno_id) ) as turnos_planta_produccion
                    ,MAX(TD.Total_Turnos_Default) AS Turnos_Default 
               FROM cp_view.v_mf_produccion PR
                    INNER JOIN 
                         (
                              SELECT
                                   T.EntidadLegal_ID
                                   ,COUNT (DISTINCT T.Turno_ID) AS Total_Turnos_Default
                              FROM cp_dwh_mf.MF_Turno_Default T
                              GROUP BY T.EntidadLegal_ID
                         ) td on trim(pr.entidadlegal_id) = trim(td.entidadlegal_id)
               WHERE PR.Linea_Prod_ID <> -1 
               AND PR.Total_Registrado <> 0                               
               AND PR.Fecha BETWEEN concat('${hiveconf:speriod}','-01') AND date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
               AND PR.EntidadLegal_ID IN (SELECT EntidadLegal_ID 
                    FROM cp_view.V_EntidadLegal_Activas GROUP BY EntidadLegal_ID)               
               GROUP BY SUBSTRING(PR.Fecha,1,7),PR.EntidadLegal_ID,PR.MF_Organizacion_ID,PR.Planta_ID                         
               HAVING (COUNT(DISTINCT PR.Linea_Prod_ID) * MAX(TD.Total_Turnos_Default)) <> 0
          ) T 
     ON PR.Periodo = T.Periodo 
     AND PR.EntidadLegal_ID = T.EntidadLegal_ID 
     AND PR.MF_Organizacion_ID = T.MF_Organizacion_ID;


-- set paso = 14
-- Obtenemos la metrica de Piezas de Baja                    
insert into table cp_app_costoproducir.cp_medidas_prorrateo partition(entidadlegal_id)   
SELECT        
     PR.Periodo 
     ,PR.MF_Organizacion_ID                                    
     ,PR.Planta_ID 
     ,PR.Linea_Prod_ID                                         
     ,PR.Turno_ID
     ,PR.MF_Producto_ID                                        
     ,pr.TipoMedida_ID
     ,COALESCE(T.Turnos_Linea_Default,0)                       
     ,COALESCE(T.Turnos_Linea_Produccion,0)                    
     ,CASE    
          WHEN T.Turnos_Planta_Asignados IS NOT NULL THEN T.Turnos_Planta_Asignados                              
          ELSE COALESCE(MAX(T.Turnos_Planta_Asignados) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),0)                         
     END      
     ,CASE    
          WHEN T.Turnos_Planta_Produccion IS NOT NULL THEN T.Turnos_Planta_Produccion                            
          ELSE COALESCE(MAX(T.Turnos_Planta_Produccion) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),0)                        
     END      
     ,PR.Factor 
     ,((SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID, PR.Turno_Id, PR.MF_Producto_ID  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) / (case when (SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID, PR.Turno_Id  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) = 0 then null else (SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID, PR.Turno_Id  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) end)) AS Medida_Factor
     ,SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID, PR.Turno_Id, PR.MF_Producto_ID  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS Medida_Producto                                  
     ,SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID, PR.Turno_Id  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS Medida_Turno
     ,CASE          
          WHEN PR.EntidadLegal_ID = '089' THEN                          
               CASE       
                    WHEN CAST(ABS(SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS DECIMAL(18,6)) > 0 THEN SUM(CAST(PR.Total_Medida AS DECIMAL (18,6))) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  
                    ELSE 0   
               END        
          ELSE SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID, PR.Linea_Prod_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)                                      
     END AS Medida_Linea 
              
     ,SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id, PR.MF_Organizacion_ID ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS Medida_Planta                                      
     ,SUM(PR.Total_Medida) OVER (PARTITION BY PR.Periodo, PR.EntidadLegal_Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  AS Medida_EntidadLegal     
     ,from_unixtime(unix_timestamp()) as fecha_carga
     ,from_unixtime(unix_timestamp()) AS storeday
     ,PR.EntidadLegal_ID
FROM          
     (        
          -- Obtenemos las bajas de producto, con y sin línea asignada 
          SELECT
               substr(PR.Fecha,1,7) AS Periodo                                      
               ,PR.EntidadLegal_ID                             
               ,PR.MF_Organizacion_ID                          
               ,PR.Planta_ID                                   
               ,PR.Linea_Prod_ID                               
               ,PR.Turno_ID                                    
               ,PR.MF_Producto_ID                              
               ,SUM(PR.Bajas) AS Factor                           
               ,8 AS TipoMedida_ID -- Pzas Baja Tabla CP_Tipo_Medida 
               ,SUM(PR.Bajas) AS Total_Medida
               ,max(pr.storeday) as storeday
          FROM cp_view.v_mf_produccion PR
          -- CC 101-12 Maquilas: Se agrega filtro para no incluir en el calculo de la Medida de Prorrateo los PTs maquilados
          left join
          (
               select vpm2.periodo, vpm2.entidadlegal_id, vpm2.mf_organizacion_id, vpm2.mf_producto_id 
               from cp_app_costoproducir.v_producto_maquilado vpm2
               where vpm2.periodo = '${hiveconf:speriod}' and cast(vpm2.entidadlegal_id as int) in (select entidadlegal_id from cp_view.v_entidadlegal_activas group by entidadlegal_id)
               group by vpm2.periodo, vpm2.entidadlegal_id, vpm2.mf_organizacion_id, vpm2.mf_producto_id
          ) vpm
          on vpm.periodo = substr(pr.fecha,1,7) and vpm.entidadlegal_id = pr.entidadlegal_id and vpm.mf_organizacion_id = pr.mf_organizacion_id and vpm.mf_producto_id = pr.mf_producto_id
          WHERE PR.Bajas <> 0                                  
               AND PR.Fecha BETWEEN concat('${hiveconf:speriod}','-01') AND date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
               AND PR.EntidadLegal_ID IN (SELECT EntidadLegal_ID 
                    FROM cp_view.v_entidadlegal_activas GROUP BY EntidadLegal_ID)                                 
               and vpm.periodo is null and vpm.entidadlegal_id is null and vpm.mf_organizacion_id is null and vpm.mf_producto_id is null
          GROUP BY substr(PR.Fecha,1,7), PR.EntidadLegal_ID, PR.MF_Organizacion_ID, PR.Planta_ID, PR.Linea_Prod_ID, PR.Turno_ID, PR.MF_Producto_ID
          HAVING SUM(PR.Bajas) <> 0                               
     ) PR     
     LEFT OUTER JOIN 
          (   
               --Obtenemos datos del turno                     
               SELECT
                    substr(PR.Fecha,1,7) AS Periodo                                 
                    ,PR.EntidadLegal_ID
                    ,PR.MF_Organizacion_ID
                    ,PR.Planta_ID
                    ,PR.Linea_Prod_ID
                    ,TOT.Turnos_Planta_Asignados
                    ,TOT.Turnos_Planta_Produccion
                    ,TOT.Turnos_Default AS Turnos_Linea_Default 
                    ,COUNT(DISTINCT PR.Turno_ID) AS Turnos_Linea_Produccion                                     
               FROM cp_view.v_mf_produccion PR  
                    INNER JOIN                                 
                         (                                     
                              --   Obtenemos el total de turnos que produjeron y asignados por planta                 
                              SELECT
                                   substr(PR.Fecha,1,7) AS Periodo                       
                                   ,PR.EntidadLegal_ID                 
                                   ,PR.MF_Organizacion_ID              
                                   ,PR.Planta_ID                       
                                   ,COUNT(DISTINCT PR.Linea_Prod_ID) * MAX(TD.Total_Turnos_Default) AS Turnos_Planta_Asignados
                                   ,count(distinct concat(pr.linea_prod_id, pr.turno_id) ) as turnos_planta_produccion
                                   ,MAX(TD.Total_Turnos_Default) AS Turnos_Default 
                              FROM cp_view.v_mf_produccion PR
                                   INNER JOIN 
                                        (
                                             SELECT
                                                  T.EntidadLegal_ID
                                                  ,COUNT (DISTINCT T.Turno_ID) AS Total_Turnos_Default
                                             FROM cp_dwh_mf.mf_turno_default T
                                             GROUP BY T.EntidadLegal_ID
                                        ) td on trim(pr.entidadlegal_id) = trim(td.entidadlegal_id)
                              WHERE PR.Linea_Prod_ID <> -1 AND PR.Total_Registrado <> 0                               
                                   AND PR.Fecha BETWEEN concat('${hiveconf:speriod}','-01') AND date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                                   AND PR.EntidadLegal_ID IN  (SELECT EntidadLegal_ID FROM cp_view.v_entidadlegal_activas GROUP BY EntidadLegal_ID)               
                              GROUP BY substr(PR.Fecha,1,7),PR.EntidadLegal_ID,PR.MF_Organizacion_ID,PR.Planta_ID                         
                              HAVING (COUNT(DISTINCT PR.Linea_Prod_ID) * MAX(TD.Total_Turnos_Default))  <> 0                   
                         ) TOT ON substr(PR.Fecha,1,7) = TOT.Periodo 
                         AND PR.EntidadLegal_ID = TOT.EntidadLegal_ID 
                         AND PR.MF_Organizacion_ID = TOT.MF_Organizacion_ID AND PR.Planta_ID = TOT.Planta_ID    
               WHERE PR.Linea_Prod_ID <> -1 AND PR.Total_Registrado <> 0                                         
                    AND PR.Fecha BETWEEN concat('${hiveconf:speriod}','-01') AND date_sub(add_months(concat('${hiveconf:speriod}','-01'),1), 1)
                    AND PR.EntidadLegal_ID IN (SELECT EntidadLegal_ID FROM cp_view.v_entidadlegal_activas GROUP BY EntidadLegal_ID)                            
              GROUP BY substr(PR.Fecha,1,7),PR.EntidadLegal_ID,PR.MF_Organizacion_ID,PR.Planta_ID,PR.Linea_Prod_ID,TOT.Turnos_Planta_Asignados,TOT.Turnos_Planta_Produccion,TOT.Turnos_Default
              HAVING COUNT(DISTINCT PR.Turno_ID) <> 0          
          ) T 
     ON PR.Periodo = T.Periodo 
     AND PR.EntidadLegal_ID = T.EntidadLegal_ID 
     AND PR.MF_Organizacion_ID = T.MF_Organizacion_ID 
     AND PR.Planta_ID = T.Planta_ID 
     AND PR.Linea_Prod_ID = T.Linea_Prod_ID;

-- Compactacion de la tabla, para quedarnos con los registros de ultima carga con las agrupaciones definidas.
insert overwrite table cp_app_costoproducir.cp_medidas_prorrateo partition(entidadlegal_id) select tmp.* from cp_app_costoproducir.cp_medidas_prorrateo tmp join (select periodo, entidadlegal_id, mf_organizacion_id, planta_id, linea_prod_id, turno_id, mf_producto_id, tipomedida_id, max(storeday) as first_record from cp_app_costoproducir.cp_medidas_prorrateo group by periodo, entidadlegal_id, mf_organizacion_id, planta_id, linea_prod_id, turno_id, mf_producto_id, tipomedida_id) sec on tmp.periodo = sec.periodo and tmp.entidadlegal_id = sec.entidadlegal_id and tmp.mf_organizacion_id = sec.mf_organizacion_id and tmp.planta_id = sec.planta_id and tmp.linea_prod_id = sec.linea_prod_id and tmp.turno_id = sec.turno_id and tmp.mf_producto_id = sec.mf_producto_id and tmp.tipomedida_id = sec.tipomedida_id and tmp.storeday = sec.first_record;

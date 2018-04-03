-- Autor:      Jaime chavez y Cesar Morquecho
-- Fecha:      Octubre 2011
-- Proyecto:   Bimbo Area Sujeto MF y LG
-- Objetivo:   Vista utilizada para transformar los datos de STG --> a DWH
-- Modificacion
--             Octubre 23, 2011 FIX: Se incluye EntidadLegal
--              Enero 20,2012   Se cambia la regla para el campo de Valor_Bajas. La formula es: El total del valor de la produccion, entre total registrado por las bajas
--                              Se corrige el calculo de TipoMoneda_ID          
--              Marzo 23, 2012. Se agrego manejo de Entidades Legales Parametrizables
--              Mayo  16, 2012. Se Reemplaza tabla gb_mdl_mexico_costoproducir.WIP_REPETITIVE_ITEMS por gb_mdl_mexico_costoproducir.V_WIP_REPETITIVE_ITEMS_HIST, para procesar con datos correspondientes a cada mes. Cambio hecho por Marcos Diaz. CC 52-12
--              Agosto 16, 2012. Para los campos Bajas y Costo_Bajas se agrega el transaction_type_id 92
--             Septiembre, 2012. Se cambian los turnos.
--                              Se eliminan campos de Valor_Bajas, MF_UnidadMedida_ID
--                              Los campos de Fecha_alta y Fecha_Mod cambian a timestamp
--              Enero, 2014. Marcos Diaz. CC 14-09 Bajas Produccion. Se agregan los campos Costo_Precio y Gramaje
--              Mayo,2014 Valeria Mtz. Se hace hace un join con la WRKT gb_mdl_mexico_costoproducir_views.WRKT_MF_CROSS_REFERENCES que se materializa en el mf_procedure_procedure
--              Agosto 2014, Valeria Mtz. Se agrega manejo de Fechas Carga parametrizadas.


-- Se eliminan previos de ejecucion de acuerdo al periodo a ejecutar en mf_produccion
insert overwrite table gb_mdl_mexico_manufactura.mf_produccion partition(entidadlegal_id)
    select p.* from gb_mdl_mexico_manufactura.mf_produccion p
    left join 
        (select 
            0 as mf_organizacion_id ,
            '0' as planta_id ,
            0 as mf_producto_id) tmp on
        p.mf_organizacion_id = tmp.mf_organizacion_id and p.planta_id = tmp.planta_id and p.planta_id = tmp.planta_id
        where tmp.mf_organizacion_id is null
    union all
    select 
        0 as mf_organizacion_id ,
        '0' as planta_id ,
        0 as mf_producto_id ,
        0 as linea_prod_id ,
        -1 as turno_id ,
        0 as tipomoneda_id ,
        '1900-01-01' as fecha ,
        0 as total_registrado ,
        0 as total_embarcado ,
        0 as bajas ,
        0 as valor_produccion ,
        0 as costo_prod_teorico ,
        0 as costo_prod_real ,
        0 as costo_actual ,
        0 as toneladas ,
        0 as ritmo ,
        0 as costo_precio ,
        0 as gramaje ,
        0 as fecha_alta ,
        0 as fecha_mod ,
        from_unixtime(unix_timestamp()) as storeday ,
        el.entidadlegal_id
        from (select distinct entidadlegal_id from gb_mdl_mexico_manufactura.mf_produccion) el;

insert overwrite table gb_mdl_mexico_manufactura.mf_produccion partition(entidadlegal_id)
  select p.* from gb_mdl_mexico_manufactura.mf_produccion p, gb_mdl_mexico_costoproducir_views.v_fechas_extraccion e
  where p.fecha not between e.fechaini and e.fechafin;

-- step1
-- Se materializa gb_mdl_mexico_costoproducir_views.vdw_mf_cross_references
insert overwrite table gb_mdl_mexico_costoproducir_views.wrkt_mf_cross_reference
     select vmcr.*
     from gb_mdl_mexico_costoproducir_views.vdw_mf_cross_references vmcr;


-- step1
insert overwrite table gb_mdl_mexico_costoproducir_views.vdw_mf_produccion_mt_1
select 
     t2.transaction_source_id
     ,t2.organization_id
     ,t2.inventory_item_id
     ,t2.transaction_date
     ,t2.transaction_date_h
     ,coalesce(wri_h.line_id, wrip_h.line_id, wri.line_id, wrip.line_id)            as line_id
     ,coalesce(wri_h.production_line_rate, wrip_h.production_line_rate, wri.production_line_rate, wrip.production_line_rate)                                                                            as production_line_rate
     ,case when t2.transaction_type_id in (44,17) and t2.transaction_source_type_id = 5
               then coalesce(t2.primary_quantity,0)
               else 0 
          end                                                                       as pt_productoregistrado
     ,case when t2.transaction_type_id in (33)
               then coalesce(t2.primary_quantity,0)*-1
               else 0 
          end                                                                       as pt_productoembarcado
     ,case when t2.transaction_type_id in (44,17) and t2.transaction_source_type_id = 5 
               then coalesce(t2.primary_quantity,0)*t2.actual_cost
               else 0 
          end                                                                       as pt_productoregistrado_
     ,case when t2.transaction_type_id in (90,91,92) 
              then coalesce(t2.primary_quantity,0) 
              else 0 
          end                                                                       as pt_bajas
     ,case when t2.transaction_type_id in (90,91,92) 
          then coalesce(t2.primary_quantity,0)* coalesce(t2.actual_cost,0) 
          else 0 
      end                                                                           as pt_bajas_
     ,0                                                                             as pt_costoproduccionreal  
     ,0                                                                             as toneladas
     ,0                                                                             as pt_valor_produccion
from  gb_mdl_mexico_costoproducir.mtl_transaccion_materiales t2, gb_mdl_mexico_costoproducir_views.v_fechas_extraccion fe

left outer join gb_mdl_mexico_costoproducir.wip_flow_schedules ws
    on t2.transaction_source_id    =   ws.wip_entity_id 
    and t2.organization_id         =   ws.organization_id
    and t2.inventory_item_id       =   ws.primary_item_id

left outer join 
    (
     select 
        h.aniobimbo
        ,h.mesbimbo
        ,h.fecha_actualizacion
        ,h.organization_id
        ,h.primary_item_id
        ,coalesce(h.alternate_routing_designator ,-1)     as alternate_routing_designator
        ,min(h.line_id)                                   as line_id
        ,max(h.production_line_rate)                      as production_line_rate
        ,max(case 
                  when h.attribute6 is not null then h.attribute6
                  else 0 
               end)                                       as attribute6
        from gb_mdl_mexico_costoproducir_views.v_wip_repetitive_items_hist h
             inner join 
             (
                  select h_.aniobimbo, h_.mesbimbo, h_.organization_id, h_.primary_item_id, max(h_.fecha_actualizacion) as fecha_actualizacion
                  from gb_mdl_mexico_costoproducir_views.v_wip_repetitive_items_hist h_
                  group by h_.aniobimbo, h_.mesbimbo, h_.organization_id, h_.primary_item_id
             ) hm on  h.aniobimbo = hm.aniobimbo and h.mesbimbo = hm.mesbimbo and h.organization_id = hm.organization_id and h.primary_item_id = hm.primary_item_id and h.fecha_actualizacion = hm.fecha_actualizacion
      group by h.aniobimbo
        ,h.mesbimbo
        ,h.fecha_actualizacion
        ,h.organization_id
        ,h.primary_item_id
        ,coalesce(h.alternate_routing_designator ,-1)
    ) wri_h  on ws.organization_id                         = wri_h.organization_id  
         and ws.primary_item_id                            = wri_h.primary_item_id 
         and coalesce(ws.alternate_routing_designator,-1)  = wri_h.alternate_routing_designator 
         and year(t2.transaction_date)                 = wri_h.aniobimbo
         and month(t2.transaction_date)                = wri_h.mesbimbo
left outer join 
    (
         select 
              wri_.organization_id
              ,wri_.primary_item_id
              ,coalesce(wri_.alternate_routing_designator ,-1)     as alternate_routing_designator
              ,min(wri_.line_id)                                   as line_id
              ,max(wri_.production_line_rate)                      as production_line_rate
              ,max(case 
                        when wri_.attribute6 is not null then wri_.attribute6
                        else 0 
                     end)                                     as attribute6
         from gb_mdl_mexico_costoproducir.wip_repetitive_items wri_
         group by 
              wri_.organization_id
              ,wri_.primary_item_id
              ,coalesce(wri_.alternate_routing_designator ,-1)
    ) wri  on ws.organization_id                               =   wri.organization_id  
           and ws.primary_item_id                              =   wri.primary_item_id 
           and coalesce(ws.alternate_routing_designator,-1)    =   wri.alternate_routing_designator 
left outer join 
    (
        select 
          h.aniobimbo
          ,h.mesbimbo
          ,h.fecha_actualizacion
          ,h.organization_id
          ,h.primary_item_id
          ,min(h.line_id)                                   as line_id
          ,max(h.production_line_rate)                      as production_line_rate
          ,max(case 
                        when h.attribute6 is not null then h.attribute6
                        else 0
                 end)                                       as attribute6
        from gb_mdl_mexico_costoproducir_views.v_wip_repetitive_items_hist h
               inner join 
               (
                    select vwrih.aniobimbo, vwrih.mesbimbo, vwrih.organization_id, vwrih.primary_item_id, max(vwrih.fecha_actualizacion) as fecha_actualizacion
                    from gb_mdl_mexico_costoproducir_views.v_wip_repetitive_items_hist vwrih
                    group by vwrih.aniobimbo, vwrih.mesbimbo, vwrih.organization_id, vwrih.primary_item_id
               ) hm  on  h.aniobimbo = hm.aniobimbo and h.mesbimbo = hm.mesbimbo and h.organization_id = hm.organization_id and h.primary_item_id = hm.primary_item_id and h.fecha_actualizacion = hm.fecha_actualizacion
        group by 
              h.aniobimbo
              ,h.mesbimbo
              ,h.fecha_actualizacion
              ,h.organization_id
              ,h.primary_item_id
    )wrip_h
    on t2.organization_id = wrip_h.organization_id  and t2.inventory_item_id = wrip_h.primary_item_id
         and year(t2.transaction_date)  = wrip_h.aniobimbo
         and month(t2.transaction_date) = wrip_h.mesbimbo
LEFT outer JOIN 
    (
         SELECT 
             wri2_.ORGANIZATION_ID
             ,wri2_.PRIMARY_ITEM_ID
             ,MIN(wri2_.line_id)                                   AS Line_id
             ,MAX(wri2_.PRODUCTION_LINE_RATE)                      AS PRODUCTION_LINE_RATE
             ,MAX(CASE 
                           WHEN wri2_.ATTRIBUTE6 IS NOT NULL THEN wri2_.ATTRIBUTE6
                           ELSE 0
                    END)                                     AS ATTRIBUTE6
           FROM gb_mdl_mexico_costoproducir.WIP_REPETITIVE_ITEMS wri2_
               GROUP BY 
                    wri2_.ORGANIZATION_ID,
                    wri2_.PRIMARY_ITEM_ID
    ) WRIP
    ON T2.ORGANIZATION_ID =  WRIP.ORGANIZATION_ID  AND T2.INVENTORY_ITEM_ID = WRIP.PRIMARY_ITEM_ID
    JOIN  gb_mdl_mexico_costoproducir.mtl_catalogo_materiales  MAT
    ON T2.ORGANIZATION_ID                                 =   MAT.ORGANIZATION_ID
    AND T2.INVENTORY_ITEM_ID                              =   MAT.INVENTORY_ITEM_ID
    AND MAT.ITEM_TYPE                                     =   'PT'

where t2.TRANSACTION_DATE BETWEEN fe.fechaini and fe.fechafin
GROUP BY 
      t2.transaction_source_id
     ,t2.organization_id
     ,t2.inventory_item_id
     ,t2.transaction_date
     ,t2.transaction_date_h
     ,coalesce(wri_h.line_id, wrip_h.line_id, wri.line_id, wrip.line_id)
     ,coalesce(wri_h.production_line_rate, wrip_h.production_line_rate, wri.production_line_rate, wrip.production_line_rate)
     ,case when t2.transaction_type_id in (44,17) and t2.transaction_source_type_id = 5
               then coalesce(t2.primary_quantity,0)
               else 0 
          end
     ,case when t2.transaction_type_id in (33)
               then coalesce(t2.primary_quantity,0)*-1
               else 0 
          end
     ,case when t2.transaction_type_id in (44,17) and t2.transaction_source_type_id = 5 
               then coalesce(t2.primary_quantity,0)*t2.actual_cost
               else 0 
          end
     ,case when t2.transaction_type_id in (90,91,92) 
              then coalesce(t2.primary_quantity,0) 
              else 0 
          end
     ,case when t2.transaction_type_id in (90,91,92) 
          then coalesce(t2.primary_quantity,0)* coalesce(t2.actual_cost,0) 
          else 0 
      end
     ,0
     ,0
     ,0
HAVING 
COALESCE(case when t2.transaction_type_id in (44,17) and t2.transaction_source_type_id = 5
                   then coalesce(t2.primary_quantity,0)
                   else 0 
              end 
          ,0)
+ COALESCE(case when t2.transaction_type_id in (33)
               then coalesce(t2.primary_quantity,0)*-1
               else 0 
          end 
          ,0)  
+ COALESCE(case when t2.transaction_type_id in (44,17) and t2.transaction_source_type_id = 5 
               then coalesce(t2.primary_quantity,0)*t2.actual_cost
               else 0 
          end
          ,0) 
+ COALESCE(case when t2.transaction_type_id in (90,91,92) 
              then coalesce(t2.primary_quantity,0) 
              else 0 
          end
          ,0)  
+ COALESCE(case when t2.transaction_type_id in (90,91,92) 
              then coalesce(t2.primary_quantity,0) 
              else 0 
          end
          ,0)  <> 0;


-- step2
insert overwrite table gb_mdl_mexico_costoproducir_views.vdw_mf_produccion_p1_2
select  
     mt.transaction_source_id
     ,mt.transaction_date               as  transaction_date
     ,mt.transaction_date_h             as transaction_date_h
     ,mt.organization_id                as organization_id
     ,mt.inventory_item_id              as inventory_item_id 
     ,coalesce(
        case when (concat(mt.transaction_date,' ',mt.transaction_date_h) between mf_turno_0.fechahoraini and mf_turno_0.fechahorafin) then
              mf_turno_0.turno_id 
              else null
        end,
        case when (concat(mt.transaction_date,' ',mt.transaction_date_h) between mf_turno_1.fechahoraini and mf_turno_1.fechahorafin) then
              mf_turno_1.turno_id
              else null
        end,
        -1) as turno_id
     ,coalesce(mt.line_id, -1)          as line_id
     ,mt.production_line_rate           as production_line_rate
     ,mt.pt_productoregistrado
     ,mt.pt_productoembarcado
     ,mt.pt_productoregistrado_
     ,mt.pt_bajas
     ,mt.pt_bajas_
     ,mt.pt_costoproduccionreal       
     ,mt.toneladas
     ,mt.pt_valor_produccion
    --step 1
from gb_mdl_mexico_costoproducir_views.vdw_mf_produccion_mt_1 mt,
  (
    select entidadlegal_id from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf group by entidadlegal_id
  )ela

left outer join gb_mdl_mexico_costoproducir_views.v_mf_turno_dia mf_turno_0
     on mt.organization_id  =  mf_turno_0.mf_organizacion_id
          and mt.line_id = mf_turno_0.linea_prod_id
          and mt.transaction_date =  mf_turno_0.fechaini
          and mf_turno_0.entidadlegal_id = ela.entidadlegal_id

left outer join gb_mdl_mexico_costoproducir_views.v_mf_turno_dia mf_turno_1
     on mt.organization_id  =  mf_turno_1.mf_organizacion_id
          and mt.line_id = mf_turno_1.linea_prod_id
          and mt.transaction_date =  mf_turno_1.fechafin
          and mf_turno_0.entidadlegal_id = ela.entidadlegal_id;





-- Se eliminan duplicados
insert overwrite table gb_mdl_mexico_costoproducir_views.vdw_mf_produccion_p1_2 
select 
transaction_source_id,
transaction_date,
transaction_date_h,
organization_id,
inventory_item_id,
max(turno_id),
line_id,
production_line_rate,
pt_productoregistrado,
pt_productoembarcado,
pt_productoregistrado_,
pt_bajas,
pt_bajas_,
pt_costoproduccionreal,
toneladas,
pt_valor_produccion
from gb_mdl_mexico_costoproducir_views.vdw_mf_produccion_p1_2 
group by 
transaction_source_id,
transaction_date,
transaction_date_h,
organization_id,
inventory_item_id,
line_id,
production_line_rate,
pt_productoregistrado,
pt_productoembarcado,
pt_productoregistrado_,
pt_bajas,
pt_bajas_,
pt_costoproduccionreal,
toneladas,
pt_valor_produccion;


-- step3
insert overwrite table gb_mdl_mexico_costoproducir_views.vdw_mf_produccion_principal_3
select
 p1.transaction_source_id       as transaction_source_id
 ,p1.transaction_date           as transaction_date
 ,p1.transaction_date_h         as transaction_date_h
 ,p1.organization_id            as organization_id
 ,p1.inventory_item_id          as inventory_item_id 
 ,case
      when p1.turno_id = -1 then coalesce(
                                    case when (concat(p1.transaction_date,' ',p1.transaction_date_h) between mf_turno2_0.fechahoraini and mf_turno2_0.fechahorafin)
                                        then mf_turno2_0.turno_id
                                        else null
                                    end,
                                    case when (concat(p1.transaction_date,' ',p1.transaction_date_h) between mf_turno2_1.fechahoraini and mf_turno2_1.fechahorafin)
                                        then mf_turno2_1.turno_id
                                        else null
                                    end,
                                    -1)
      else p1.turno_id
 end as turno_id
 ,coalesce(p1.line_id, -1)     as line_id
 ,p1.production_line_rate      as production_line_rate
 ,p1.pt_productoregistrado
 ,p1.pt_productoembarcado
 ,p1.pt_productoregistrado_
 ,p1.pt_bajas
 ,p1.pt_bajas_
 ,p1.pt_costoproduccionreal 
 ,p1.toneladas
 ,p1.pt_valor_produccion
 --step2
from gb_mdl_mexico_costoproducir_views.vdw_mf_produccion_p1_2 p1,(
        select entidadlegal_id from  gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf group by entidadlegal_id
      ) ela2
left outer join gb_mdl_mexico_costoproducir_views.t_mf_turno_default_dia mf_turno2_0
      on p1.organization_id  =  mf_turno2_0.mf_organizacion_id
      and p1.transaction_date =  mf_turno2_0.fechaini
      and p1.turno_id = -1
      and mf_turno2_0.entidadlegal_id = ela2.entidadlegal_id
                                   
left outer join gb_mdl_mexico_costoproducir_views.t_mf_turno_default_dia mf_turno2_1
      on p1.organization_id  =  mf_turno2_1.mf_organizacion_id
      and p1.transaction_date =  mf_turno2_1.fechafin
      and p1.turno_id = -1
      and mf_turno2_1.entidadlegal_id = ela2.entidadlegal_id;


insert overwrite table gb_mdl_mexico_costoproducir_views.vdw_mf_produccion_principal_3
  select
  transaction_source_id,
  transaction_date,
  transaction_date_h,
  organization_id,
  inventory_item_id,
  turno_id,
  line_id,
  production_line_rate,
  pt_productoregistrado,
  pt_productoembarcado,
  pt_productoregistrado_,
  pt_bajas,
  pt_bajas_,
  pt_costoproduccionreal,
  toneladas,
  pt_valor_produccion
  from gb_mdl_mexico_costoproducir_views.vdw_mf_produccion_principal_3
  group by
  transaction_source_id,
  transaction_date,
  transaction_date_h,
  organization_id,
  inventory_item_id,
  turno_id,
  line_id,
  production_line_rate,
  pt_productoregistrado,
  pt_productoembarcado,
  pt_productoregistrado_,
  pt_bajas,
  pt_bajas_,
  pt_costoproduccionreal,
  toneladas,
  pt_valor_produccion;

-- step4
insert into table gb_mdl_mexico_manufactura.mf_produccion partition(entidadlegal_id)
SELECT 
      Fin.MF_Organizacion_ID                                              AS MF_Organizacion_ID 
      ,Fin.Planta_ID                                                       AS Planta_ID          
      ,Fin.MF_Producto_ID                                                  AS MF_Producto_ID     
      ,Fin.Linea_Prod_ID                                                   AS Linea_Prod_ID      
      ,Fin.Turno_ID                                                        AS Turno_ID           
      ,Fin.TipoMoneda_ID                                                   AS TipoMoneda_ID      
      ,Fin.Fecha                                                           AS Fecha              
      ,Fin.Total_Registrado                                                AS Total_Registrado   
      ,Fin.Total_Embarcado                                                 AS Total_Embarcado    
      ,Fin.Bajas                                                           AS Bajas              
      ,coalesce(Fin.Valor_Produccion,0)                                    AS Valor_Produccion       
      ,Fin.Costo_Prod_Teorico                                              AS Costo_Prod_Teorico
      ,Fin.Costo_Prod_Real                                                 AS Costo_Prod_Real   
      ,Fin.Costo_Actual                                                    AS Costo_Actual      
      ,coalesce(Fin.Toneladas,0)                                           AS Toneladas              
      ,Fin.Ritmo                                                           AS Ritmo            
      ,Fin.Costo_Precio                                                    AS Costo_Precio
      ,Fin.Gramaje                                                         AS Gramaje
      ,Fin.Fecha_alta                                                      AS Fecha_alta       
      ,Fin.Fecha_Mod                                                       AS Fecha_Mod
      ,from_unixtime(unix_timestamp())
      ,Fin.EntidadLegal_ID                                                 AS EntidadLegal_ID
FROM
     (
          SELECT 
               COALESCE(MF_Plantas_0.EntidadLegal_ID, '-1')                     AS EntidadLegal_ID
               ,uno.organization_id                                             AS MF_Organizacion_ID
               ,COALESCE(MF_Plantas_0.Host_ID, '-1')                            AS Planta_ID
               ,COALESCE(uno.inventory_item_id, -1)                             AS MF_Producto_ID
               ,COALESCE(uno.line_id , -1)                                      AS Linea_Prod_ID
               ,uno.turno_id                                                    AS Turno_ID
               ,COALESCE(TM.TipoMoneda_ID,-1)                                   AS TipoMoneda_ID
               ,COALESCE(
                         case when (uno.transaction_date between CREF.pvm_fechaini and CREF.pvm_fechafin and uno.transaction_date between vfe.fechaini and vfe.fechafin)
                            then CREF.CROSS_REFERENCE_PVM_FL
                            else null
                          end
                          ,0)                                                   AS Costo_Precio
               ,COALESCE(
                          case when (uno.transaction_date between CREF.pe_fechaini and CREF.pe_fechafin and uno.transaction_date between CREF.pvm_fechaini and CREF.pvm_fechafin and uno.transaction_date between vfe.fechaini and vfe.fechafin) 
                            then CREF.CROSS_REFERENCE_PE_FL
                            else null
                          end
                          ,0)                                                   AS Gramaje
               ,uno.transaction_date                                            AS Fecha
               ,Uno.PT_ProductoRegistrado                                       AS Total_Registrado
               ,Uno.PT_ProductoEmbarcado                                        AS Total_Embarcado
               ,Uno.PT_Bajas                                                    AS Bajas
               ,Uno.PT_ProductoRegistrado * 
                                            case when (uno.transaction_date between CREF.pvm_fechaini and CREF.pvm_fechafin and uno.transaction_date between vfe.fechaini and vfe.fechafin)
                                              then CREF.CROSS_REFERENCE_PVM_FL
                                              else null
                                              end                               AS Valor_Produccion
               ,Uno.PT_ProductoRegistrado_                                      AS Costo_Prod_Teorico
               ,Uno.PT_CostoProduccionReal                                      AS Costo_Prod_Real
               ,Uno.PT_Valor_Produccion                                         AS Costo_Actual
               ,Uno.PT_ProductoRegistrado * 
                                            case when ( uno.transaction_date between CREF.pe_fechaini and CREF.pe_fechafin and uno.transaction_date between CREF.pvm_fechaini and CREF.pvm_fechafin and uno.transaction_date between vfe.fechaini and vfe.fechafin)
                                              then CREF.CROSS_REFERENCE_PE_FL
                                              else null
                                              end                               AS Toneladas
               ,COALESCE(uno.production_line_rate, 0)                           AS Ritmo
               ,from_unixtime(unix_timestamp())                                 AS Fecha_alta
               ,from_unixtime(unix_timestamp())                                 AS Fecha_Mod
          FROM
               (
                    SELECT 
                         Principal.transaction_date                                     AS transaction_date
                         ,Principal.Organization_ID                                     AS Organization_ID
                         ,Principal.Inventory_Item_ID                                   AS Inventory_Item_ID
                         ,Principal.Turno_ID                                            AS Turno_ID
                         ,Principal.Line_ID                                             AS Line_ID
                         ,MAX(Principal.production_line_rate)                           AS production_line_rate
                         ,COALESCE(SUM(Principal.PT_ProductoRegistrado ),0)             AS PT_ProductoRegistrado
                         ,COALESCE(SUM(Principal.PT_ProductoEmbarcado ),0)              AS PT_ProductoEmbarcado
                         ,COALESCE(SUM(Principal.PT_ProductoRegistrado_),0)             AS PT_ProductoRegistrado_
                         ,COALESCE(SUM(Principal.PT_Bajas ),0)                          AS PT_Bajas
                         ,COALESCE(SUM(Principal.PT_Bajas_ ),0)                         AS PT_Bajas_
                         ,COALESCE(SUM(Principal.PT_CostoProduccionReal ),0)            AS PT_CostoProduccionReal
                         ,COALESCE(SUM(Principal.Toneladas ),0)                         AS Toneladas
                         ,COALESCE(SUM(Principal.PT_Valor_Produccion ),0)               AS PT_Valor_Produccion
                         --step3
                    FROM gb_mdl_mexico_costoproducir_views.vdw_mf_produccion_principal_3 Principal
                    GROUP BY 
                          Principal.transaction_date
                         ,Principal.Organization_ID
                         ,Principal.Inventory_Item_ID
                         ,Principal.Turno_ID
                         ,Principal.Line_ID
               )Uno, gb_mdl_mexico_costoproducir_views.v_fechas_extraccion vfe
          
               JOIN gb_mdl_mexico_manufactura.mf_organizacion MF_Plantas_0 
                    ON uno.organization_id = MF_Plantas_0.MF_Organizacion_ID
          
               LEFT outer JOIN gb_mdl_mexico_costoproducir_views.wrkt_mf_cross_reference     CREF
               ON MF_Plantas_0.EntidadLegal_ID         =   CREF.EntidadLegal_ID
               AND uno.inventory_item_id               =   CREF.MF_Producto_ID

               LEFT OUTER JOIN        
                    (
                           SELECT 
                                O.EntidadLegal_ID
                                ,COALESCE(M.TipoMoneda_ID,-1)           AS TipoMoneda_ID
                                ,O.Organizacion_ID
                                ,COALESCE(M.Pais_ID,-1)                 AS Pais_ID
                           FROM gb_mdl_mexico_costoproducir.O_ENTIDADLEGAL_ORGANIZACION O
                                LEFT OUTER JOIN gb_mdl_mexico_costoproducir.O_ENTIDAD_LEGAL El ON O.EntidadLegal_ID = EL.EntidadLegal_ID
                                LEFT OUTER JOIN gb_mdl_mexico_costoproducir.V_TIPO_MONEDA M ON M.Pais_ID = O.Pais_ID
                                LEFT OUTER JOIN gb_mdl_mexico_costoproducir.G_PAIS G ON O.Pais_ID = G.Pais_ID
                                LEFT OUTER JOIN gb_mdl_mexico_costoproducir.E_ORGANIZACION E ON E.Organizacion_ID = O.Organizacion_ID
                      ) TM
               ON MF_Plantas_0.EntidadLegal_ID = TM.EntidadLegal_ID
               where 
                  MF_Plantas_0.EntidadLegal_ID IN (SELECT EntidadLegal_ID FROM  gb_mdl_mexico_costoproducir_views.V_ENTIDADESLEGALES_ACTIVAS_MF GROUP BY EntidadLegal_ID)
                  AND lower(MF_Plantas_0.Organizacion_Tipo) = lower('PLANTA')
)Fin;

-- step5
-- Eliminar duplicados de gb_mdl_mexico_manufactura.mf_produccion
insert overwrite table gb_mdl_mexico_manufactura.mf_produccion partition(entidadlegal_id)
  select 
mf_organizacion_id,
max(planta_id),
mf_producto_id,
linea_prod_id,
turno_id,
max(tipomoneda_id),
fecha,
max(total_registrado),
max(total_embarcado),
max(bajas),
max(valor_produccion),
max(costo_prod_teorico),
max(costo_prod_real),
max(costo_actual),
max(toneladas),
max(ritmo),
max(costo_precio),
max(gramaje),
max(fecha_alta),
max(fecha_mod),
max(storeday),
entidadlegal_id
from gb_mdl_mexico_manufactura.mf_produccion
group by mf_organizacion_id, mf_producto_id, linea_prod_id, turno_id, fecha, entidadlegal_id;


insert overwrite table gb_mdl_mexico_manufactura.mf_produccion partition(entidadlegal_id)
select 
mf_organizacion_id,
(planta_id),
mf_producto_id,
linea_prod_id,
turno_id,
(tipomoneda_id),
fecha,
(total_registrado),
(total_embarcado),
(bajas),
COALESCE(total_registrado*costo_precio,0) as valor_produccion,
(costo_prod_teorico),
(costo_prod_real),
(costo_actual),
COALESCE(total_registrado * gramaje,0) as toneladas,
(ritmo),
(costo_precio),
(gramaje),
(fecha_alta),
(fecha_mod),
(storeday),
entidadlegal_id
from gb_mdl_mexico_manufactura.mf_produccion;


-- Agregamos a la tabla de MF_Turnos, los turnos por default que se generaron en la MF_Produccion 
insert into table gb_mdl_mexico_manufactura.mf_turnos partition(entidadlegal_id)
     select 
          p.mf_organizacion_id
          ,p.planta_id
          ,p.linea_prod_id
          ,p.turno_id 
          ,case 
               when p.turno_id = 1 then 'Turno Matutino'
               when p.turno_id = 2 then 'Turno Vespertino'
               when p.turno_id = 3 then 'Turno Nocturno'
               end                                               as turno_ds
          ,substr(p.fecha,1,7)
          ,f.fecha_ini 
          ,f.fecha_fin
          ,d.turnohraini
          ,d.turnohrafinal
          ,'Turno por Default'                                   as origen
          ,'Turno por Default obtenido de MF_PRODUCCION'         as observaciones
          ,from_unixtime(unix_timestamp())                       as fecha_carga
          ,null                                                  as fecha_vigencia
          ,from_unixtime(unix_timestamp())                       as storeday
          ,p.entidadlegal_id
     from gb_mdl_mexico_manufactura.mf_produccion p
          inner join
               (
                    select 
                         trim(td.entidadlegal_id) as entidadlegal_id
                         ,td.turno_id
                         ,td.turnohraini
                         ,td.turnohrafinal
                    from gb_mdl_mexico_manufactura.mf_turno_default td
                    group by 
                         trim(td.entidadlegal_id)
                         ,td.turno_id
                         ,td.turnohraini
                         ,td.turnohrafinal
               )d
          on p.entidadlegal_id = d.entidadlegal_id and p.turno_id = d.turno_id

          inner join
               (         
                    select c.year_of_calendar, c.month_of_year, c.calendar_date, s.fecha_ini, s.fecha_fin
                    from cp_sys_calendar.calendar c
                         inner join
                              (
                                   select  
                                        year_of_calendar
                                        ,month_of_year
                                        ,min (calendar_date)     as fecha_ini
                                        ,max(calendar_date)      as fecha_fin
                                   from cp_sys_calendar.calendar
                                   group by 
                                        year_of_calendar
                                        ,month_of_year
                              ) s on c.year_of_calendar = s.year_of_calendar and c.month_of_year = s.month_of_year
                    where c.calendar_date between '2010-01-01' and to_date(from_unixtime(unix_timestamp()))
               ) f
          on p.fecha = f.calendar_date

          left join 
          (
               select t_.entidadlegal_id, t_.mf_organizacion_id, t_.planta_id, t_.linea_prod_id, t_.periodo
               from gb_mdl_mexico_manufactura.mf_turnos t_
               where t_.entidadlegal_id in (select entidadlegal_id from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf group by entidadlegal_id) 
                    and t_.fecha_vigencia is null 
               group by t_.entidadlegal_id, t_.mf_organizacion_id, t_.planta_id, t_.linea_prod_id, t_.periodo
          ) t on p.entidadlegal_id = t.entidadlegal_id and p.mf_organizacion_id = t.mf_organizacion_id and p.planta_id = t.planta_id and p.linea_prod_id = t.linea_prod_id and substr(p.fecha, 1, 7) = t.periodo

     where p.entidadlegal_id in (select entidadlegal_id from gb_mdl_mexico_costoproducir_views.v_entidadeslegales_activas_mf group by entidadlegal_id)
     and p.linea_prod_id <> -1

     and t.entidadlegal_id is null
     and t.mf_organizacion_id is null
     and t.planta_id is null
     and t.linea_prod_id is null 
     and t.periodo is null

     GROUP BY
          p.mf_organizacion_id
          ,p.planta_id
          ,p.linea_prod_id
          ,p.turno_id 
          ,case 
               when p.turno_id = 1 then 'Turno Matutino'
               when p.turno_id = 2 then 'Turno Vespertino'
               when p.turno_id = 3 then 'Turno Nocturno'
               end
          ,substr(p.fecha,1,7)
          ,f.fecha_ini 
          ,f.fecha_fin
          ,d.turnohraini
          ,d.turnohrafinal
          ,'Turno por Default'
          ,'Turno por Default obtenido de MF_PRODUCCION'
          ,to_date(from_unixtime(unix_timestamp()))
          ,null
          ,p.entidadlegal_id;

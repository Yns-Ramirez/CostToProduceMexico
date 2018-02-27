-- ======================================================
--  gb_mdl_mexico_manufactura.mf_lineas_prod
insert overwrite table gb_mdl_mexico_manufactura.mf_lineas_prod partition(entidadlegal_id)
     select 
           fte.linea_prod_id
           ,case when fte.grupo_lineas_prod_id <> -1
                 then fte.grupo_lineas_prod_id
                 else coalesce(fte.grupo_lineas_prod_id,-1)
            end
           ,case when lower(fte.linea_prod_ds) <> 's/i'
                 then fte.linea_prod_ds
                 else coalesce(fte.linea_prod_ds,'s/i')
            end
            ,fte.storeday
            ,fte.entidadlegal_id
       from gb_mdl_mexico_costoproducir_views.vdw_mf_lineas_prod fte;
insert overwrite table gb_smntc_mexico_costoproducir.v_rubro81_costo_inv partition(entidadlegal_id)
    select 
        mf_organizacion_id
        ,planta_id
        ,mf_producto_id
        ,periodo
        ,tipomoneda_id
        ,tsubinven
        ,costo_capital
        ,costo
        ,cantidad
        ,importe
        ,from_unixtime(unix_timestamp()) as storeday
        ,entidadlegal_id
    from gb_smntc_mexico_costoproducir.v_rubro81_costo_inv_view;
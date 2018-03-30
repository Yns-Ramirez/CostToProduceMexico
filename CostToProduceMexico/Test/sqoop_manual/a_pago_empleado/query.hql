insert overwrite table gb_mdl_mexico_erp.a_pago_empleado
    select 
        tiponomina_id
        ,empleado_id
        ,fechapago
        ,cuentanatural_id
        ,analisislocal_id
        ,concepto_id
        ,region_id
        ,montopago
        ,tipomoneda_id
        ,sistemafuente
        ,usuarioetl
        ,fechacarga
        ,fechacambio
        ,from_unixtime(unix_timestamp()) as storeday
        from gb_mdl_mexico_costoproducir_wrkt.a_pago_empleado;

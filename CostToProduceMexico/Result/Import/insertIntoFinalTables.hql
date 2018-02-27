insert overwrite table jedoxMexico.cp_data_detalle partition(entidadlegal_id, periodo) 
    select 
        execution_date,
        organizacion_id,
        pais_id,
        planta_id,
        lineas_id,
        turno_id,
        producto_id,
        rubro_id,
        value,
        tmoneda_id,
        from_unixtime(unix_timestamp()) as storeday,
        entidadlegal_id,
        regexp_replace(periodo, '\'', '')
    from jedoxMexico.cp_data_detalle_tmp;


insert overwrite table jedoxMexico.cp_data_piezas partition(entidadlegal_id, periodo) 
    select 
        execution_date,
        planta_id,
        lineas_id,
        turno_id,
        producto_id,
        concepto,
        cantidad,
        from_unixtime(unix_timestamp()) as storeday,
        entidadlegal_id,
        regexp_replace(periodo, '\'', '')
    from jedoxMexico.cp_data_piezas_tmp;

insert overwrite table jedoxMexico.cp_data_sumario partition(entidadlegal_id, periodo) 
    select 
        execution_date string, 
        organizacion_id string, 
        pais_id string, 
        planta_id string, 
        lineas_id string, 
        turno_id string, 
        producto_id string, 
        rubro_id string, 
        tipo_costo string, 
        value string, 
        tmoneda_id string,
        from_unixtime(unix_timestamp()) as storeday,
        entidadlegal_id string, 
        regexp_replace(periodo, '\'', '')
    from jedoxMexico.cp_data_sumario_tmp;

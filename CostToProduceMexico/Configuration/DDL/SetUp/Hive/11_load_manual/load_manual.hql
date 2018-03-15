insert overwrite table gb_mdl_mexico_costoproducir.o_entidadlegal_organizacion partition(entidadlegal_id)
    select
    'BAUS' as organizacion_id
    ,'840' as pais_id
    ,'MANUAL' as sistema_fuente
    ,'2016-08-11 00:00:00' as fecha_alta
    ,from_unixtime(unix_timestamp()) as storeday
    ,'125' as entidadlegal_id;
-- ======================================================
--  a_pago_empleado

insert overwrite table gb_mdl_mexico_erp.a_pago_empleado partition(fechapago)
     select
          tmp.tiponomina_id,
          tmp.empleado_id,
          tmp.cuentanatural_id,
          tmp.analisislocal_id,
          tmp.concepto_id,
          tmp.region_id,
          tmp.montopago,
          tmp.tipomoneda_id,
          tmp.sistemafuente,
          tmp.usuarioetl,
          tmp.fechacarga,
          tmp.fechacambio,
          from_unixtime(unix_timestamp()),
          substr(tmp.fechapago,1,10) as fechapago
     from gb_mdl_mexico_costoproducir_views.vdw_a_pago_empleado tmp, gb_mdl_mexico_costoproducir_views.v_fechas_extraccion vfe
     where tmp.fechapago >= vfe.fechaini;
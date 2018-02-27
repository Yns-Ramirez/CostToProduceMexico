-- ======================================================
--  a_pago_empleado

insert overwrite table gb_smntc_mexico_costoproducir.a_pago_empleado
     select
          tmp.tiponomina_id,
          tmp.empleado_id,
          tmp.fechapago,
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
          from_unixtime(unix_timestamp())
     from gb_mdl_mexico_costoproducir_views.vdw_a_pago_empleado tmp, gb_mdl_mexico_costoproducir_views.v_fechas_extraccion vfe
     where tmp.fechapago >= vfe.fechaini;
-- ======================================================
--  a_saldo_nomina

INSERT INTO cp_app_costoproducir.a_saldo_nomina partition(entidadlegal_id)
SELECT g_t1.je_header_id AS encabezado_id,
       g_t1.je_line_num AS detalle_id,
       g_t1.effective_date AS fechamovimiento,
       g_t0.period_name AS periodo,
       g_t2.segment2 AS areanegocio_id,
       g_t2.segment3 AS cuentanatural_id,
       g_t2.segment4 AS analisislocal_id,
       g_t2.segment5 AS marca_id,
       g_t2.segment6 AS centrocostos_id,
       g_t2.segment7 AS intercost_id,
       g_t2.segment8 AS segment8,
       g_t2.segment9 AS segment9,
       g_t2.segment10 AS segment10,
       g_t2.account_type AS tipocuenta,
       g_t1.set_of_books_id AS juegolibros_id,
       g_t0.je_category AS categoriaencabezado_je,
       g_t0.actual_flag AS flagencabezado,
       g_t0.name AS nombreencabezado,
       g_t0.description AS descencabezado,
       g_t1.description AS descmovimiento,
       g_t1.accounted_cr AS montocredito,
       g_t1.accounted_dr AS montodebito,
       1 AS tipomoneda_id,
       g_t0.last_update_date AS hdr_ultimafechamodificacion,
       g_t1.last_update_date AS ultimafechamodificacion,
       g_t0.status AS hdr_status,
       g_t1.status AS status,
       g_t0.je_source AS hdr_je_source,
       g_t0.je_batch_id AS hdr_je_batch_id,
       g_t0.posted_date AS hdr_posted_date,
       from_unixtime(unix_timestamp()) AS storeday,
       g_t2.segment1 AS entidadlegal_id
FROM cp_dwh.gl_je_headers g_t0,
     cp_dwh.gl_je_lines g_t1,
     cp_dwh.gl_estructura_contable g_t2
WHERE g_t1.code_combination_id = g_t2.code_combination_id
  AND g_t1.je_header_id = g_t0.je_header_id
  AND g_t1.set_of_books_id IN (141,
                               161,
                               324,
                               784,
                               382,
                               701,
                               641)
  and g_t1.effective_date between '${hiveconf:fechainicioperiodo}' AND '${hiveconf:fechafinperiodo}';
  


INSERT overwrite TABLE cp_app_costoproducir.a_saldo_nomina partition(entidadlegal_id)
SELECT tmp.*
FROM cp_app_costoproducir.a_saldo_nomina tmp
JOIN
  (SELECT encabezado_id,
          detalle_id,
          fechamovimiento,
          periodo,
          entidadlegal_id,
          areanegocio_id,
          cuentanatural_id,
          analisislocal_id,
          marca_id,
          centrocostos_id,
          tipomoneda_id,
          max(storeday) AS first_record
   FROM cp_app_costoproducir.a_saldo_nomina
   GROUP BY encabezado_id,
            detalle_id,
            fechamovimiento,
            periodo,
            entidadlegal_id,
            areanegocio_id,
            cuentanatural_id,
            analisislocal_id,
            marca_id,
            centrocostos_id,
            tipomoneda_id) sec ON tmp.encabezado_id=sec.encabezado_id
AND tmp.detalle_id=sec.detalle_id
AND tmp.fechamovimiento=sec.fechamovimiento
AND tmp.periodo=sec.periodo
AND tmp.entidadlegal_id = sec.entidadlegal_id
AND tmp.areanegocio_id=sec.areanegocio_id
AND tmp.cuentanatural_id=sec.cuentanatural_id
AND tmp.analisislocal_id=sec.analisislocal_id
AND tmp.marca_id=sec.marca_id
AND tmp.centrocostos_id=sec.centrocostos_id
AND tmp.tipomoneda_id=sec.tipomoneda_id
AND tmp.storeday = sec.first_record;


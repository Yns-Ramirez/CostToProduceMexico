-- ======================================================
--  e_posicion
insert overwrite table cp_app_costoproducir.e_posicion partition(entidadlegal_id)
     select 
     tmp.posicion_id,
     tmp.fechainicio,
     tmp.fechafin,
     tmp.nombreposicion,
     tmp.puesto_id,
     tmp.plan_id,
     tmp.areanegocio_id,
     tmp.unidadtrabajo_id,
     tmp.categoria_id,
     tmp.nivel_id,
     tmp.centrotrabajo_id,
     tmp.centrocosto_id,
     tmp.convenio_id,
     tmp.horasxsemana,
     tmp.tiempocompleto,
     tmp.posicion_id_original,
     tmp.sistema_fuente,
     tmp.fecha_alta,
     from_unixtime(unix_timestamp()),
     tmp.entidadlegal_id
     from cp_view.vdw_e_posicion tmp;

-- la compactacion ya no es necesario ya que se la vista trae informacion total.
-- insert overwrite table  cp_app_costoproducir.e_posicion partition(entidadlegal_id) select tmp.* from  cp_app_costoproducir.e_posicion tmp join (select posicion_id, max(storeday) as first_record from  cp_app_costoproducir.e_posicion group by posicion_id) sec on tmp.posicion_id = sec.posicion_id and tmp.storeday = sec.first_record;
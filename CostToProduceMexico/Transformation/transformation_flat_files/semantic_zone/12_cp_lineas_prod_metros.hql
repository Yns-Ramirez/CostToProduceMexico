-- ======================================================
--  CP_Lineas_Prod_Metros

-- PONERLE FECHA DE FIN A LOS QUE ESTAN NULOS
insert into gb_smntc_mexico_costoproducir.CP_Lineas_Prod_Metros partition(entidadlegal_id) 
select a.MF_Organizacion_ID,a.Planta_ID,a.Linea_Prod_ID,a.Metros2,a.Fecha_Ini,FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'), 
FROM_UNIXTIME(UNIX_TIMESTAMP()),a.entidadlegal_id 
from gb_smntc_mexico_costoproducir.CP_Lineas_Prod_Metros a 
inner join (select b.EntidadLegal_ID,b.MF_Organizacion_ID,b.Planta_ID,b.Linea_Prod_ID,b.Metros2,b.Fecha_Ini,b.Fecha_Fin
from gb_mdl_mexico_costoproducir_views.VT_CP_Lineas_Prod_Metros b) b
on a.entidadlegal_id=b.entidadlegal_id
and a.MF_Organizacion_ID=b.MF_Organizacion_ID
and a.Planta_ID=b.Planta_ID
and a.Linea_Prod_ID=b.Linea_Prod_ID
where a.fecha_fin is null 
and b.Fecha_Ini>= a.Fecha_Ini;
-- Compactacion para cambiar los periodos viejos con fecha_fin null y dejar los que ya tienen fecha de cierre.  
insert overwrite table gb_smntc_mexico_costoproducir.CP_Lineas_Prod_Metros partition(entidadlegal_id) 
select tmp.* from gb_smntc_mexico_costoproducir.CP_Lineas_Prod_Metros tmp join 
(select  entidadlegal_id, MF_Organizacion_ID, Planta_ID, Linea_Prod_ID, Fecha_Ini, max(storeday) as first_record 
from gb_smntc_mexico_costoproducir.CP_Lineas_Prod_Metros 
group by entidadlegal_id, MF_Organizacion_ID, Planta_ID, Linea_Prod_ID, Fecha_Ini) sec 
on tmp.entidadlegal_id = sec.entidadlegal_id 
and tmp.MF_Organizacion_ID = sec.MF_Organizacion_ID 
and tmp.Planta_ID = sec.Planta_ID 
and tmp.Linea_Prod_ID = sec.Linea_Prod_ID 
and tmp.Fecha_Ini = sec.Fecha_Ini
and tmp.storeday = sec.first_record;
-- Insertar los del nuevo periodo.
insert into gb_smntc_mexico_costoproducir.CP_Lineas_Prod_Metros partition(entidadlegal_id) 
select a.MF_Organizacion_ID,a.Planta_ID,a.Linea_Prod_ID,a.Metros2, FROM_UNIXTIME(UNIX_TIMESTAMP()),a.Fecha_Fin, 
FROM_UNIXTIME(UNIX_TIMESTAMP()),a.entidadlegal_id from gb_mdl_mexico_costoproducir_views.VT_CP_Lineas_Prod_Metros a;

-- ================== cp_app_costoproducir.CP_Beneficios_Vol ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information, table cp_app_costoproducir.CP_Beneficios_Vol
-- Subject Area / Area Sujeto :  


-- PONERLE FECHA DE FIN A LOS QUE ESTAN NULOS
insert into cp_app_costoproducir.CP_Beneficios_Vol partition(entidadlegal_id) 
select a.Periodo,a.MF_Organizacion_ID,a.Planta_ID,a.Ingrediente_ID,a.CuentaNatural_ID,
a.Importe,a.Fecha_Ini,FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),FROM_UNIXTIME(UNIX_TIMESTAMP()),a.entidadlegal_id 
from cp_app_costoproducir.CP_Beneficios_Vol a 
inner join (select b.Periodo,b.MF_Organizacion_ID,b.Planta_ID,b.Ingrediente_ID,b.CuentaNatural_ID,b.Importe,b.Fecha_Ini,b.entidadlegal_id
from cp_view.VT_CP_Beneficios_Vol b) b
on a.entidadlegal_id=b.entidadlegal_id
and a.Periodo=b.Periodo
and a.MF_Organizacion_ID=b.MF_Organizacion_ID
and a.Planta_ID=b.Planta_ID
and a.Ingrediente_ID=b.Ingrediente_ID
and a.CuentaNatural_ID=b.CuentaNatural_ID
where a.fecha_fin is null 
and b.Fecha_Ini>= a.Fecha_Ini;

-- Compactacion para cambiar los periodos viejos con fecha_fin null y dejar los que ya tienen fecha de cierre.  
insert overwrite table cp_app_costoproducir.CP_Beneficios_Vol partition(entidadlegal_id) 
select tmp.* from cp_app_costoproducir.CP_Beneficios_Vol tmp join 
(select a.entidadlegal_id,a.Periodo,a.MF_Organizacion_ID,a.Ingrediente_ID,a.CuentaNatural_ID,a.Fecha_Ini, max(storeday) as first_record 
from cp_app_costoproducir.CP_Beneficios_Vol a
group by a.entidadlegal_id,a.Periodo,a.MF_Organizacion_ID,a.Ingrediente_ID,a.CuentaNatural_ID,a.Fecha_Ini) sec 
on tmp.entidadlegal_id = sec.entidadlegal_id 
and tmp.MF_Organizacion_ID = sec.MF_Organizacion_ID 
and tmp.Periodo = sec.Periodo 
and tmp.Ingrediente_ID = sec.Ingrediente_ID 
and tmp.CuentaNatural_ID = sec.CuentaNatural_ID
and tmp.Fecha_Ini = sec.Fecha_Ini
and tmp.storeday = sec.first_record;

-- Insertar los del nuevo periodo.
insert into cp_app_costoproducir.CP_Beneficios_Vol partition(entidadlegal_id) 
select b.Periodo,b.MF_Organizacion_ID,b.Planta_ID,b.Ingrediente_ID,b.CuentaNatural_ID,b.Importe,FROM_UNIXTIME(UNIX_TIMESTAMP())
,b.Fecha_Fin,FROM_UNIXTIME(UNIX_TIMESTAMP()),b.entidadlegal_id
from cp_view.VT_CP_Beneficios_Vol b;
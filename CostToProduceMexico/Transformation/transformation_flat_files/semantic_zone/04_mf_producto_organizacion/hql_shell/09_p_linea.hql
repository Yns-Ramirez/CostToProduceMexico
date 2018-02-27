-- ================== gb_mdl_mexico_costoproducir.P_LINEA ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information weekly, table gb_mdl_mexico_costoproducir.P_LINEA
-- Subject Area / Area Sujeto : Product

-- Insert rows from flat file product
INSERT INTO gb_mdl_mexico_costoproducir.P_LINEA
SELECT a.Categoria_ID,a.Linea_ID,a.NombreLinea,FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM cp_flat_files.FT_JER_PRODUCTO a
where a.linea_id not in (
select b.Linea_ID
from cp_flat_files.FT_JER_PRODUCTO b
group by b.Linea_ID
having count(*)>1);

-- Delete duplicades  
insert overwrite table gb_mdl_mexico_costoproducir.P_LINEA
select tmp.* from gb_mdl_mexico_costoproducir.P_LINEA tmp
join (select Categoria_ID,Linea_ID,max(storeday) as first_record 
from gb_mdl_mexico_costoproducir.P_LINEA group by Categoria_ID,Linea_ID) sec
on tmp.Categoria_ID=sec.Categoria_ID
and tmp.Linea_ID=sec.Linea_ID
and tmp.storeday=sec.first_record;


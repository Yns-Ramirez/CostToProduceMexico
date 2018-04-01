-- ================== MF_PLANTAS HQL ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update "MF_Plantas" information on table gb_mdl_mexico_manufactura.MF_Plantas
-- Subject Area / Area Sujeto : Manufacture 
--                              Plants hierarchical structure Manufacturing

-- Inserting values into gb_mdl_mexico_manufactura.MF_Plantas, using view gb_mdl_mexico_costoproducir_views.VDW_MF_PLANTAS
Insert into gb_mdl_mexico_manufactura.MF_Plantas partition(entidadlegal_id) 
select * from gb_mdl_mexico_costoproducir_views.VDW_MF_PLANTAS;

-- Deleting duplicades
insert overwrite table gb_mdl_mexico_manufactura.MF_Plantas partition(entidadlegal_id) 
select tmp.* from gb_mdl_mexico_manufactura.MF_Plantas tmp join (select entidadlegal_id,MF_Organizacion_ID,max(storeday) as first_record 
from gb_mdl_mexico_manufactura.MF_Plantas group by entidadlegal_id,MF_Organizacion_ID) sec 
on tmp.entidadlegal_id=sec.entidadlegal_id 
and tmp.MF_Organizacion_ID=sec.MF_Organizacion_ID 
and tmp.storeday=sec.first_record;

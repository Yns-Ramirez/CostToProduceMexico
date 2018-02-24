-- ================== MF_PLANTAS HQL ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update "MF_Plantas" information on table cp_dwh_mf.MF_Plantas
-- Subject Area / Area Sujeto : Manufacture 
--                              Plants hierarchical structure Manufacturing

-- Inserting values into cp_dwh_mf.MF_Plantas, using view cp_view.VDW_MF_PLANTAS
Insert into cp_dwh_mf.MF_Plantas partition(entidadlegal_id) 
select * from cp_view.VDW_MF_PLANTAS;

-- Deleting duplicades
insert overwrite table cp_dwh_mf.MF_Plantas partition(entidadlegal_id) 
select tmp.* from cp_dwh_mf.MF_Plantas tmp join (select entidadlegal_id,MF_Organizacion_ID,max(storeday) as first_record 
from cp_dwh_mf.MF_Plantas group by entidadlegal_id,MF_Organizacion_ID) sec 
on tmp.entidadlegal_id=sec.entidadlegal_id 
and tmp.MF_Organizacion_ID=sec.MF_Organizacion_ID 
and tmp.storeday=sec.first_record;

-- ================== gb_mdl_mexico_manufactura.mf_linea ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information, table gb_mdl_mexico_manufactura.mf_linea
-- Subject Area / Area Sujeto : Manufacture 

-- Update information
insert into gb_mdl_mexico_manufactura.mf_linea select Linea_ID,Linea_Desc,storeday from gb_mdl_mexico_costoproducir_views.VDW_MF_LINEA;

-- Delete duplicades
Insert overwrite table gb_mdl_mexico_manufactura.mf_linea
select tmp.* from gb_mdl_mexico_manufactura.mf_linea tmp join (select Linea_ID,max(storeday) as first_record 
from gb_mdl_mexico_manufactura.mf_linea group by Linea_ID) sec 
on tmp.Linea_ID=sec.Linea_ID and tmp.storeday=sec.first_record;


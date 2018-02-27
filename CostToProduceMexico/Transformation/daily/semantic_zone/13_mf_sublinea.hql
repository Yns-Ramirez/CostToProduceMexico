-- ================== gb_mdl_mexico_manufactura.MF_SUBLINEA ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information, table gb_mdl_mexico_manufactura.MF_SUBLINEA
-- Subject Area / Area Sujeto : Manufacture 

-- Update information
insert into gb_mdl_mexico_manufactura.MF_SUBLINEA select SubLinea_ID,Sublinea_Desc,storeday from gb_mdl_mexico_costoproducir_views.VDW_MF_SUBLINEA;

-- Delete duplicades
Insert overwrite table gb_mdl_mexico_manufactura.MF_SUBLINEA
select tmp.* from gb_mdl_mexico_manufactura.MF_SUBLINEA tmp join (select SubLinea_ID,max(storeday) as first_record 
from gb_mdl_mexico_manufactura.MF_SUBLINEA group by SubLinea_ID) sec 
on tmp.SubLinea_ID=sec.SubLinea_ID and tmp.storeday=sec.first_record;

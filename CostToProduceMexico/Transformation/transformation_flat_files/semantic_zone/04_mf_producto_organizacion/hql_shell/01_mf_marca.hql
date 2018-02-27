-- ================== gb_mdl_mexico_manufactura.mf_marca ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information, table gb_mdl_mexico_manufactura.mf_marca
-- Subject Area / Area Sujeto : Manufacture 

-- Update information
insert into gb_mdl_mexico_manufactura.mf_marca select Marca_ID,Marca_Desc,storeday from gb_mdl_mexico_costoproducir_views.VDW_MF_MARCA;

-- Delete duplicades
Insert overwrite table gb_mdl_mexico_manufactura.mf_marca
select tmp.* from gb_mdl_mexico_manufactura.mf_marca tmp join (select Marca_ID,max(storeday) as first_record 
from gb_mdl_mexico_manufactura.mf_marca group by Marca_ID) sec 
on tmp.Marca_ID=sec.Marca_ID 
and tmp.storeday=sec.first_record;

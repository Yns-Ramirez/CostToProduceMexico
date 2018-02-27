-- ================== gb_mdl_mexico_manufactura.MF_CATEGORIA ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information, table gb_mdl_mexico_manufactura.MF_CATEGORIA
-- Subject Area / Area Sujeto : Manufacture 

-- Update information
insert into gb_mdl_mexico_manufactura.MF_CATEGORIA select Categoria_ID,Tipo_Categ_ID,Categoria_Desc,storeday from gb_mdl_mexico_costoproducir_views.VDW_MF_CATEGORIA;

-- Delete duplicades
Insert overwrite table gb_mdl_mexico_manufactura.MF_CATEGORIA
select tmp.* from gb_mdl_mexico_manufactura.MF_CATEGORIA tmp join (select Categoria_ID ,Tipo_Categ_ID,max(storeday) as first_record 
from gb_mdl_mexico_manufactura.MF_CATEGORIA group by Categoria_ID ,Tipo_Categ_ID) sec 
on tmp.Categoria_ID=sec.Categoria_ID and tmp.Tipo_Categ_ID=sec.Tipo_Categ_ID and tmp.storeday=sec.first_record;

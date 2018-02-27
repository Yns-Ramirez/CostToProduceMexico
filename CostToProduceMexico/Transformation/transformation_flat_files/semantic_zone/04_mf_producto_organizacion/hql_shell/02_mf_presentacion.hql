-- ================== gb_mdl_mexico_manufactura.MF_Presentacion ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information, table gb_mdl_mexico_manufactura.MF_Presentacion
-- Subject Area / Area Sujeto : Manufacture 

--Update information
insert into gb_mdl_mexico_manufactura.MF_Presentacion select Presentacion_ID,Presentacion_Desc,storeday from gb_mdl_mexico_costoproducir_views.VDW_MF_PRESENTACION;

-- Delete duplicades
Insert overwrite table gb_mdl_mexico_manufactura.MF_Presentacion
select tmp.* from gb_mdl_mexico_manufactura.MF_Presentacion tmp join (select Presentacion_ID,max(storeday) as first_record 
from gb_mdl_mexico_manufactura.MF_Presentacion group by Presentacion_ID) sec 
on tmp.Presentacion_ID=sec.Presentacion_ID and tmp.storeday=sec.first_record;

-- ================== cp_dwh_mf.MF_Presentacion ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information, table cp_dwh_mf.MF_Presentacion
-- Subject Area / Area Sujeto : Manufacture 

--Update information
insert into cp_dwh_mf.MF_Presentacion select Presentacion_ID,Presentacion_Desc,storeday from cp_view.VDW_MF_PRESENTACION;

-- Delete duplicades
Insert overwrite table cp_dwh_mf.MF_Presentacion
select tmp.* from cp_dwh_mf.MF_Presentacion tmp join (select Presentacion_ID,max(storeday) as first_record 
from cp_dwh_mf.MF_Presentacion group by Presentacion_ID) sec 
on tmp.Presentacion_ID=sec.Presentacion_ID and tmp.storeday=sec.first_record;

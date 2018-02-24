-- ================== cp_dwh_mf.mf_linea ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information, table cp_dwh_mf.mf_linea
-- Subject Area / Area Sujeto : Manufacture 

-- Update information
insert into cp_dwh_mf.mf_linea select Linea_ID,Linea_Desc,storeday from cp_view.VDW_MF_LINEA;

-- Delete duplicades
Insert overwrite table cp_dwh_mf.mf_linea
select tmp.* from cp_dwh_mf.mf_linea tmp join (select Linea_ID,max(storeday) as first_record 
from cp_dwh_mf.mf_linea group by Linea_ID) sec 
on tmp.Linea_ID=sec.Linea_ID and tmp.storeday=sec.first_record;


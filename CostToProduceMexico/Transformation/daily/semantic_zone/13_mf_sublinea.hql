-- ================== cp_dwh_mf.MF_SUBLINEA ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information, table cp_dwh_mf.MF_SUBLINEA
-- Subject Area / Area Sujeto : Manufacture 

-- Update information
insert into cp_dwh_mf.MF_SUBLINEA select SubLinea_ID,Sublinea_Desc,storeday from cp_view.VDW_MF_SUBLINEA;

-- Delete duplicades
Insert overwrite table cp_dwh_mf.MF_SUBLINEA
select tmp.* from cp_dwh_mf.MF_SUBLINEA tmp join (select SubLinea_ID,max(storeday) as first_record 
from cp_dwh_mf.MF_SUBLINEA group by SubLinea_ID) sec 
on tmp.SubLinea_ID=sec.SubLinea_ID and tmp.storeday=sec.first_record;

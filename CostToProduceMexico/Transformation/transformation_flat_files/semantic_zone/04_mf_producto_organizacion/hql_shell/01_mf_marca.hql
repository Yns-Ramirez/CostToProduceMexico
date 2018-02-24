-- ================== cp_dwh_mf.mf_marca ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information, table cp_dwh_mf.mf_marca
-- Subject Area / Area Sujeto : Manufacture 

-- Update information
insert into cp_dwh_mf.mf_marca select Marca_ID,Marca_Desc,storeday from cp_view.VDW_MF_MARCA;

-- Delete duplicades
Insert overwrite table cp_dwh_mf.mf_marca
select tmp.* from cp_dwh_mf.mf_marca tmp join (select Marca_ID,max(storeday) as first_record 
from cp_dwh_mf.mf_marca group by Marca_ID) sec 
on tmp.Marca_ID=sec.Marca_ID 
and tmp.storeday=sec.first_record;

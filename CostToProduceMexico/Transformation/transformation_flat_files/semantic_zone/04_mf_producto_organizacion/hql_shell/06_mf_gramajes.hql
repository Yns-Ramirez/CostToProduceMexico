-- ================== cp_dwh.MF_GRAMAJES ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information, table cp_dwh.MF_GRAMAJES
-- Subject Area / Area Sujeto : Manufacture 

-- Update information
INSERT INTO cp_dwh.MF_GRAMAJES 
SELECT item,contenidoneto,FROM_UNIXTIME(UNIX_TIMESTAMP()),entidadlegal_id 
FROM cp_flat_files.mf_gramajes;

-- Delete duplicades
Insert overwrite table cp_dwh.MF_GRAMAJES
select tmp.* from cp_dwh.MF_GRAMAJES tmp join (select EntidadLegal_id ,Item,max(storeday) as first_record 
from cp_dwh.MF_GRAMAJES group by EntidadLegal_id ,Item) sec 
on tmp.EntidadLegal_id=sec.EntidadLegal_id and tmp.Item=sec.Item and tmp.storeday=sec.first_record;


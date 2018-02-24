-- ================== cp_dwh.P_SUBLINEA ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information weekly, table cp_dwh.P_SUBLINEA
-- Subject Area / Area Sujeto : Product

-- Insert rows from flat file product
INSERT INTO cp_dwh.P_SUBLINEA
SELECT 
P.Categoria_ID
,P.Linea_ID
,CASE
WHEN P.sublinea_id = 21 THEN P.linea_id * -1
ELSE P.sublinea_id  
END AS SubLinea_ID2
,CASE
WHEN P.sublinea_id = 21 THEN concat('SUBLINEA de ',P.NombreLinea)
ELSE P.NombreSubLinea 
END AS NombreSUBLINEA
,FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM cp_flat_files.FT_JER_PRODUCTO P
GROUP BY P.Categoria_ID,P.Linea_ID,CASE
WHEN P.sublinea_id = 21 THEN P.linea_id * -1
ELSE P.sublinea_id  
END,
CASE
WHEN P.sublinea_id = 21 THEN concat('SUBLINEA de ',P.NombreLinea)
ELSE P.NombreSubLinea 
END;

-- Case of sublines 21 into product files, the rule of negative lines is applied
Insert into cp_dwh.P_SUBLINEA 
SELECT a.CATEGORIA_ID, a.LINEA_ID, a.LINEA_ID*-1,concat('SUBLINEA de ',a.nombrelinea),FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM cp_dwh.P_LINEA a
INNER JOIN cp_flat_files.ft_producto c
ON a.linea_id = c.linea_id
WHERE c.sublinea_id=21
and (a.linea_id*-1) not in (select sublinea_id from cp_dwh.p_sublinea)
group by a.CATEGORIA_ID, a.LINEA_ID, a.LINEA_ID*-1,concat('SUBLINEA de ',a.nombrelinea);

-- Delete duplicades
Insert overwrite table cp_dwh.P_SUBLINEA
select tmp.* from cp_dwh.P_SUBLINEA tmp
join (select sublinea_id,max(storeday) as first_record 
from cp_dwh.P_SUBLINEA group by sublinea_id) sec
on tmp.sublinea_id=sec.sublinea_id
and tmp.storeday=sec.first_record;


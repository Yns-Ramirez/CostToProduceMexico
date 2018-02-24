-- ================== cp_dwh.P_CATEGORIA ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information weekly, table cp_dwh.P_CATEGORIA
-- Subject Area / Area Sujeto : Product

-- Insert rows from flat file product
INSERT INTO cp_dwh.P_CATEGORIA
SELECT Categoria_ID,UPPER(TRIM(NombreCategoria)),FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM cp_flat_files.FT_JER_PRODUCTO
group by Categoria_ID,UPPER(TRIM(NombreCategoria));

-- Delete duplicades
insert overwrite table cp_dwh.P_CATEGORIA
select tmp.* from cp_dwh.P_CATEGORIA tmp 
join (select Categoria_ID,NombreCategoria,max(storeday) as first_record from cp_dwh.P_CATEGORIA group by Categoria_ID,NombreCategoria) sec
on tmp.Categoria_ID=sec.Categoria_ID
and UPPER(TRIM(tmp.NombreCategoria))=UPPER(TRIM(sec.NombreCategoria))
and tmp.storeday=sec.first_record;


-- ================== gb_mdl_mexico_costoproducir.P_MARCA ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information weekly, table gb_mdl_mexico_costoproducir.P_MARCA
-- Subject Area / Area Sujeto : Product

-- Update Catalogs from ftp_files.xxx_ic_productosfichatecnica and ftp_files.xxx_ic_jerprodfichatecnica
drop table cp_flat_files.FT_PRODUCTO;
CREATE TABLE cp_flat_files.FT_PRODUCTO as select * from ftp_files.xxx_ic_productosfichatecnica;

drop table cp_flat_files.FT_JER_PRODUCTO;
CREATE TABLE cp_flat_files.FT_JER_PRODUCTO as select * from ftp_files.xxx_ic_jerprodfichatecnica;


-- Insert rows from flat file product
INSERT INTO gb_mdl_mexico_costoproducir.P_MARCA
SELECT MARCA_ID,UPPER(TRIM(NombreMARCA)),FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM cp_flat_files.FT_PRODUCTO
group by MARCA_ID,upper(trim(NombreMarca));

-- Delete duplicades  
insert overwrite table gb_mdl_mexico_costoproducir.P_MARCA
select tmp.* from gb_mdl_mexico_costoproducir.P_MARCA tmp 
join (select Marca_ID,NombreMarca,max(storeday) as first_record from gb_mdl_mexico_costoproducir.P_MARCA group by Marca_ID,NombreMarca) sec
on tmp.Marca_ID=sec.Marca_ID
and UPPER(TRIM(tmp.NombreMarca))=UPPER(TRIM(sec.NombreMarca))
and tmp.storeday=sec.first_record;


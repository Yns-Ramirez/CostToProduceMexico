-- ================== gb_mdl_mexico_costoproducir.P_PRODUCTO ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information weekly, table gb_mdl_mexico_costoproducir.P_PRODUCTO
-- Subject Area / Area Sujeto : Product

-- Insert rows from flat file product
INSERT INTO gb_mdl_mexico_costoproducir.P_PRODUCTO
SELECT 
     P.Producto_ID 
     ,COALESCE(SL.Categoria_ID,9999) AS Categoria_ID
     ,COALESCE(SL.Linea_ID,9999) AS Linea_ID
     ,CASE
          WHEN P.SubLinea_ID = 21 THEN P.Linea_ID * -1
          ELSE P.SubLinea_ID  
     END AS SubLinea_ID 
     ,P.Marca_ID 
     ,P.UnidadMedida_ID 
     ,P.NombreProducto 
     ,P.TopeDevolucion 
     ,P.VidaAnaquel 
     ,P.CodigoBarras 
     ,P.ContenidoNeto 
     ,P.UnidadesEquivalentes
     ,FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM cp_flat_files.FT_PRODUCTO P 
      LEFT OUTER JOIN gb_mdl_mexico_costoproducir.P_SUBLINEA  SL 
            ON (CASE
            WHEN P.SubLinea_ID = 21 THEN P.Linea_ID * -1
            ELSE P.SubLinea_ID  
            END)  = SL.SubLinea_ID
AND P.Linea_ID=SL.Linea_ID;

-- Delete duplicades
Insert overwrite table gb_mdl_mexico_costoproducir.P_PRODUCTO
select tmp.* from gb_mdl_mexico_costoproducir.P_PRODUCTO tmp
join (select Producto_ID,max(storeday) as first_record 
from gb_mdl_mexico_costoproducir.P_PRODUCTO group by Producto_ID) sec
on tmp.Producto_ID=sec.Producto_ID
and tmp.storeday=sec.first_record;


insert overwrite table gb_mdl_mexico_costoproducir.P_PRODUCTO
    select distinct * from gb_mdl_mexico_costoproducir.P_PRODUCTO;
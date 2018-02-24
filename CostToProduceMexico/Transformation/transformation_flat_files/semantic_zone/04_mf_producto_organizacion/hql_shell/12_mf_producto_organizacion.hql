-- ================== mf_producto_organizacion HQL ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update "MF_Producto_organizacion" information on table cp_dwh_mf.MF_Producto_organizacion
-- Subject Area / Area Sujeto : Manufacture 
--                              Organization products


-- Load all registers into cp_dwh_mf.mf_producto_organizacion

INSERT INTO cp_dwh_mf.MF_Producto_Organizacion partition(entidadlegal_id) 
SELECT 
DELT.MF_Organizacion_ID
,COALESCE(HIST.Planta_ID, DELT.Planta_ID)
,DELT.MF_Producto_ID
,CASE WHEN DELT.Producto_ID <> '-1'
  THEN DELT.Producto_ID
  ELSE COALESCE(HIST.Producto_ID,'-1')
END 
,CASE WHEN DELT.Tipo_Producto_ID <> -1
  THEN DELT.Tipo_Producto_ID
  ELSE COALESCE(HIST.Tipo_Producto_ID,-1)
END 
,CASE WHEN DELT.Descripcion <> 'S/I'
  THEN DELT.Descripcion
  ELSE COALESCE(HIST.Descripcion, 'S/I')
END 
,CASE WHEN DELT.Marca_ID <> -1
  THEN DELT.Marca_ID
  ELSE COALESCE(HIST.Marca_ID,-1)
END   
,CASE WHEN DELT.Presentacion_ID <> -1
  THEN DELT.Presentacion_ID
  ELSE COALESCE(HIST.Presentacion_ID,-1)
END  
,CASE WHEN DELT.Categoria_ID <> -1
  THEN DELT.Categoria_ID
  ELSE COALESCE(HIST.Categoria_ID,-1)
END  
,CASE WHEN DELT.Linea_ID <> -1
  THEN DELT.Linea_ID
  ELSE COALESCE(HIST.Linea_ID,-1)
END  
,CASE WHEN DELT.SubLinea_ID <> -1
  THEN DELT.SubLinea_ID
  ELSE COALESCE(HIST.SubLinea_ID,-1)
END  
,CASE WHEN DELT.Gramaje <> 0
  THEN DELT.Gramaje
  ELSE COALESCE(HIST.Gramaje,0)
END  
,CASE WHEN DELT.MF_UnidadMedida_ID <> -1
  THEN DELT.MF_UnidadMedida_ID
  ELSE COALESCE(HIST.MF_UnidadMedida_ID,-1)
END  
,CASE WHEN DELT.MF_Envase_ID <> -1
  THEN DELT.MF_Envase_ID
  ELSE COALESCE(HIST.MF_Envase_ID,-1)
END 
,CASE WHEN DELT.Vida_Anaquel <> 0
  THEN DELT.Vida_Anaquel
  ELSE COALESCE(HIST.Vida_Anaquel,0)
END  
,CASE WHEN DELT.Contenedor_Desc <> 'S/I'
  THEN DELT.Contenedor_Desc
  ELSE COALESCE(HIST.Contenedor_Desc,'S/I')
END  
,CASE WHEN DELT.Cupo_Contenedor <> 0
  THEN DELT.Cupo_Contenedor
  ELSE COALESCE(HIST.Cupo_Contenedor,0)
END  
,CASE WHEN DELT.Cupo_Envase <> 0
  THEN DELT.Cupo_Envase
  ELSE COALESCE(HIST.Cupo_Envase,0)
END  
,CASE WHEN DELT.Tope_Devolucion <> 0
  THEN DELT.Tope_Devolucion
  ELSE COALESCE(HIST.Tope_Devolucion,0)
END   
,COALESCE(DELT.Indicador_EyE,HIST.Indicador_EyE)
,COALESCE(HIST.Origen,DELT.Origen)
,COALESCE(HIST.Fecha_Alta,DELT.Fecha_Alta)
,DELT.Fecha_Mod
,FROM_UNIXTIME(UNIX_TIMESTAMP())
,DELT.EntidadLegal_ID
FROM cp_view.VDW_MF_PRODUCTO_ORGANIZACION DELT
LEFT OUTER JOIN cp_dwh_mf.MF_Producto_Organizacion HIST
   ON DELT.EntidadLegal_ID = HIST.EntidadLegal_ID
   AND DELT.MF_Organizacion_ID = HIST.MF_Organizacion_ID
   AND DELT.MF_Producto_ID=HIST.MF_Producto_ID
WHERE ((HIST.EntidadLegal_ID IS NULL OR HIST.MF_Organizacion_ID IS NULL OR HIST.MF_Producto_ID IS NULL)
OR (COALESCE(HIST.Producto_ID,'-1') <> COALESCE(DELT.Producto_ID,'-1')
OR COALESCE(HIST.Descripcion,'S/I') <> COALESCE(DELT.Descripcion,'S/I')
OR COALESCE(HIST.Tipo_Producto_ID,-1) <> COALESCE(DELT.Tipo_Producto_ID,-1)
OR COALESCE(HIST.Marca_ID,-1) <> COALESCE(DELT.Marca_ID,-1)
OR COALESCE(HIST.Presentacion_ID,-1) <> COALESCE(DELT.Presentacion_ID,-1)
OR COALESCE(HIST.Categoria_ID,-1) <> COALESCE(DELT.Categoria_ID,-1)
OR COALESCE(HIST.Linea_ID,-1) <> COALESCE(DELT.Linea_ID,-1)
OR COALESCE(HIST.SubLinea_ID,-1) <> COALESCE(DELT.SubLinea_ID,-1)
OR COALESCE(HIST.Gramaje,0) <> COALESCE(DELT.Gramaje,0)
OR COALESCE(HIST.MF_UnidadMedida_ID,-1) <> COALESCE(DELT.MF_UnidadMedida_ID,-1)
OR COALESCE(HIST.MF_Envase_ID,-1) <> COALESCE(DELT.MF_Envase_ID,-1)
OR COALESCE(HIST.Vida_Anaquel,0) <> COALESCE(DELT.Vida_Anaquel,0)
OR COALESCE(HIST.Contenedor_Desc,'S/I') <> COALESCE(DELT.Contenedor_Desc,'S/I')
OR COALESCE(HIST.Cupo_Contenedor,0) <> COALESCE(DELT.Cupo_Contenedor,0)      
OR COALESCE(HIST.Cupo_Envase,0) <> COALESCE(DELT.Cupo_Envase,0)
OR COALESCE(HIST.Tope_Devolucion,0) <> COALESCE(DELT.Tope_Devolucion,0)
OR COALESCE(HIST.Indicador_EyE,-1) <> COALESCE(DELT.Indicador_EyE,-1)      
OR COALESCE(HIST.Origen,'MEXICO') <> COALESCE(DELT.Origen,'MEXICO')));



-- delete duplicades rows process

insert overwrite table cp_dwh_mf.mf_producto_organizacion partition(entidadlegal_id)
select tmp.* from cp_dwh_mf.mf_producto_organizacion tmp 
join (select EntidadLegal_ID,mf_organizacion_id,MF_Producto_ID,max(storeday) as first_record 
from cp_dwh_mf.mf_producto_organizacion group by EntidadLegal_ID ,MF_Organizacion_ID ,MF_Producto_ID) sec 
on tmp.EntidadLegal_ID=sec.EntidadLegal_ID 
and tmp.MF_Organizacion_ID=sec.MF_Organizacion_ID
and tmp.MF_Producto_ID=sec.MF_Producto_ID
and tmp.storeday=sec.first_record
where tmp.origen='MEXICO';

-- ================== cp_app_costoproducir.A_RUBROS_FSG ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information for view cp_app_costoproducir.A_RUBROS_FSG
-- Subject Area / Area Sujeto : App 


-- Clean table from all data into cp_app_costoproducir.T_A_RUBROS_FSG
TRUNCATE table cp_app_costoproducir.T_A_RUBROS_FSG;

-- Inserting data from view cp_app_costoproducir.A_RUBROS_FSG
INSERT INTO cp_app_costoproducir.T_A_RUBROS_FSG partition(entidadlegal_id) 
SELECT 
AnioSaldo
,MesSaldo
,Linea_ID
,NombreConcepto
,AreaNegocio_ID
,CuentaNatural_ID
,CentroCostos_ID
,AnalisisLocal_ID
,TOT_mActividaddelPeriodo
,FROM_UNIXTIME(UNIX_TIMESTAMP()) as storeday
,EntidadLegal_ID
FROM cp_app_costoproducir.A_RUBROS_FSG;

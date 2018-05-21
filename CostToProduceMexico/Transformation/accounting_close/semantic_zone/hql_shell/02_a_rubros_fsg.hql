-- ================== gb_smntc_mexico_costoproducir.A_RUBROS_FSG ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information for view gb_smntc_mexico_costoproducir.A_RUBROS_FSG
-- Subject Area / Area Sujeto : App 


-- Clean table from all data into gb_smntc_mexico_costoproducir.T_A_RUBROS_FSG
TRUNCATE table gb_smntc_mexico_costoproducir.T_A_RUBROS_FSG;

-- Inserting data from view gb_smntc_mexico_costoproducir.A_RUBROS_FSG
INSERT INTO gb_smntc_mexico_costoproducir.T_A_RUBROS_FSG partition(entidadlegal_id) 
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
FROM gb_smntc_mexico_costoproducir.A_RUBROS_FSG;



INSERT overwrite table gb_smntc_mexico_costoproducir.rubros_fsg partition(entidadlegal_id) 
SELECT * FROM gb_mdl_mexico_costoproducir_views.view_rubros_fsg;

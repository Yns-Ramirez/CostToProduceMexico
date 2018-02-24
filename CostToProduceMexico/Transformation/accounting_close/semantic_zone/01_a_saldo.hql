-- ================== cp_dwh.A_SALDO HQL ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update information in the table cp_dwh.A_SALDO
-- Subject Area / Area Sujeto : App 

-- Inserting values into cp_dwh.A_SALDO, using view cp_view.VDW_A_SALDO
-- insert into cp_dwh.A_SALDO partition(entidadlegal_id) 
-- select * from cp_view.VDW_A_SALDO;


Insert into cp_dwh.A_SALDO partition(entidadlegal_id) select 
aniosaldo,messaldo,areanegocio_id,cuentanatural_id,analisislocal_id,centrocostos_id
,intercost_id,segment8,segment9,segment10,juegolibros_id,marca_id,presupuesto,balanceinicial
,actividaddelperiodo,balancefinal,creditodelperiodo,debitodelperiodo,FROM_UNIXTIME(UNIX_TIMESTAMP()),entidadlegal_id
from cp_view.VDW_A_SALDO_GL;


-- Deleting duplicades for legal
insert overwrite table cp_dwh.A_SALDO partition(entidadlegal_id) 
select tmp.* from cp_dwh.A_SALDO tmp join 
(select AnioSaldo ,MesSaldo ,EntidadLegal_ID,AreaNegocio_ID ,CuentaNatural_ID 
,AnalisisLocal_ID ,CentroCostos_ID ,Intercost_ID ,JuegoLibros_ID ,Marca_ID ,Presupuesto
,max(storeday) as first_record 
from cp_dwh.A_SALDO 
group by AnioSaldo ,MesSaldo ,EntidadLegal_ID,AreaNegocio_ID ,CuentaNatural_ID 
,AnalisisLocal_ID ,CentroCostos_ID ,Intercost_ID ,JuegoLibros_ID ,Marca_ID ,Presupuesto
) sec on tmp.AnioSaldo=sec.AnioSaldo
and tmp.MesSaldo=sec.MesSaldo
and tmp.EntidadLegal_ID=sec.EntidadLegal_ID
and tmp.AreaNegocio_ID=sec.AreaNegocio_ID
and tmp.CuentaNatural_ID=sec.CuentaNatural_ID
and tmp.AnalisisLocal_ID=sec.AnalisisLocal_ID
and tmp.CentroCostos_ID=sec.CentroCostos_ID
and tmp.Intercost_ID=sec.Intercost_ID
and tmp.JuegoLibros_ID=sec.JuegoLibros_ID
and tmp.Marca_ID=sec.Marca_ID
and tmp.Presupuesto=sec.Presupuesto
and tmp.storeday=sec.first_record
where tmp.entidadlegal_id in ('100','101');

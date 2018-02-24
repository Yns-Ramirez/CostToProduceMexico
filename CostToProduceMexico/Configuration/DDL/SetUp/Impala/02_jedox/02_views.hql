CREATE VIEW IF NOT EXISTS jedox.v_validate_exec AS Select
substr(dt.periodo,1,4) as anio,
substr(dt.periodo,6,2) as mes,
substr(dt.periodo,1,8) as periodo,
dt.execution_date   as execution_date,
dt.organizacion_id   as organizacion_id,
dt.pais_id    as pais_id,
dt.entidadlegal_id as entidadlegal_id,
dt.planta_id  as planta_id,
dt.lineas_id   as lineas_id,
dt.turno_id as turno_id,
dt.producto_id as producto_id,
dt.rubro_id as rubro_id,
dt.value    as value,
dt.tmoneda_id as tmoneda_id,  
pz.concepto  as concepto,
pz.cantidad   as cantidad, 
'Costo Real' as TipoCosto,

case
when dt.rubro_id  like '1%' then '1'
when dt.rubro_id  like '2%' then '2'
when dt.rubro_id  like '3%' then '3'
when dt.rubro_id  like '4%' then '4'
when dt.rubro_id  like '5%' then '5'
end as Chapter_ID
  
from JEDOX.cp_data_detalle dt
left join JEDOX.cp_data_piezas pz on 
pz.periodo= dt.periodo and
pz.execution_date= dt.execution_date  and 
pz.entidadlegal_id=dt.entidadlegal_id and
pz.planta_id = dt.planta_id and
pz.lineas_id = dt.lineas_id  and
pz.turno_id = dt.turno_id   AND
pz.producto_id = dt.producto_id
where dt.rubro_id  not in ('1','2','3','4','5')
  
UNION ALL
Select
substr(dt.periodo,1,4) as anio,
substr(dt.periodo,6,2) as mes,
substr(dt.periodo,1,8) as periodo,
dt.execution_date   as execution_date,
dt.organizacion_id   as organizacion_id,
dt.pais_id as pais_id,
dt.entidadlegal_id as entidadlegal_id,
dt.planta_id  as planta_id,
dt.lineas_id   as lineas_id,
dt.turno_id as turno_id,
dt.producto_id as producto_id,
dt.rubro_id as rubro_id,
dt.value    as value,
dt.tmoneda_id as tmoneda_id,  
pz.concepto  as concepto,
pz.cantidad   as cantidad,
'Costo Estandar'as TipoCosto,
case
when dt.rubro_id  like '1%' then '1'
when dt.rubro_id  like '2%' then '2'
when dt.rubro_id  like '3%' then '3'
when dt.rubro_id  like '4%' then '4'
when dt.rubro_id  like '5%' then '5'
end as Chapter_ID
from JEDOX.cp_data_sumario dt
left join JEDOX.cp_data_piezas pz on 
pz.periodo= dt.periodo and
pz.execution_date= dt.execution_date  and 
pz.entidadlegal_id=dt.entidadlegal_id and
pz.planta_id = dt.planta_id and
pz.lineas_id = dt.lineas_id  and
pz.turno_id = dt.turno_id   AND
pz.producto_id = dt.producto_id

where dt.tipo_costo = 'costo_estandar'
AND dt.rubro_id NOT IN ('margen operativo');
-- ======================================================
--  cp_dwh.mx_depreciacion_equipo_cuenta
truncate table cp_dwh.mx_depreciacion_equipo_cuenta;
insert into cp_dwh.mx_depreciacion_equipo_cuenta partition(storeday)
select 
  a.asset_id as activoequipoerp_id
  ,a.calendar_period_close_date as fecha
  ,cc.segment1
  ,cc.segment2
  ,cc.segment3
  ,cc.segment4
  ,cc.segment5
  ,cc.segment6
  ,cc.segment7
  ,cc.segment8  
  ,cc.segment9
  ,cc.segment10
  ,a.book_type_code
  ,sum(a.deprn_amount)   as montodepreciado
  ,sum(a.deprn_reserve)  as montodepreciadoreserva
  ,sum(a.ytd_deprn)      as montodepreciadoacumulado
  ,sum(a.cost)           as valorrevaluadoactivo
  ,from_unixtime(unix_timestamp()) as storeday
from
 (
  select 
    dp.asset_id
    ,dp.book_type_code
    ,dp.calendar_period_close_date
    ,max(dp.distribution_id)  as distribution_id
    ,sum(dp.deprn_amount)     as deprn_amount
    ,sum(dp.deprn_reserve)    as deprn_reserve
    ,sum(dp.ytd_deprn)        as ytd_deprn
    ,sum(dp.cost)             as cost
    ,dp.storeday
  from
   erp_mexico_sz.fa_deprn_detail_per  dp
  where 
  lower(dp.book_type_code) in ('bimbo corporate','blm corporate', 'glo corporate', 'cbi corporate','bimbo_norma_loc','blm norma local','glo norma local','con ifrs','frp_ifrs','baus ifrs','baus_corporate','baus_ifrs')
  group by dp.asset_id,dp.book_type_code,dp.calendar_period_close_date,dp.storeday
 ) a
 left outer join erp_mexico_sz.fa_distribution_history_brio dh
 on a.asset_id = dh.asset_id
 and a.distribution_id = dh.distribution_id
 left outer join cp_view.gl_code_combinations_fa_brio_tmp cc
   on dh.code_combination_id = cc.code_combination_id
where 
  lower(a.book_type_code) in ('bimbo corporate','blm corporate', 'glo corporate', 'cbi corporate','bimbo_norma_loc','blm norma local','glo norma local','con ifrs','frp_ifrs','baus ifrs','baus_corporate','baus_ifrs')
group by a.asset_id, a.calendar_period_close_date, cc.segment1, cc.segment2, cc.segment3, cc.segment4, cc.segment5, cc.segment6, cc.segment7, cc.segment8, cc.segment9, cc.segment10, a.book_type_code, a.storeday;

-- compactation
insert overwrite table cp_dwh.mx_depreciacion_equipo_cuenta partition(storeday) select tmp.* from  cp_dwh.mx_depreciacion_equipo_cuenta tmp join (select activoequipoerp_id ,fecha, max(storeday) as first_record from  cp_dwh.mx_depreciacion_equipo_cuenta group by activoequipoerp_id ,fecha) sec on tmp.activoequipoerp_id = sec.activoequipoerp_id and tmp.fecha = sec.fecha and tmp.storeday = sec.first_record;

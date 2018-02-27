select orden_turno as row_depth
from gb_mdl_mexico_costoproducir_views.wrkt_mf_turnos_rotativos
     group by orden_turno
     order by orden_turno;
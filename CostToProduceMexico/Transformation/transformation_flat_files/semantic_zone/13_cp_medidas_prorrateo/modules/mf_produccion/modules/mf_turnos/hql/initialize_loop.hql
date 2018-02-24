select orden_turno as row_depth
from cp_view.wrkt_mf_turnos_rotativos
     group by orden_turno
     order by orden_turno;
-- Se inserta uno a uno los turnos rotativos en tabla de trabajo WRKT_MF_Turnos
          INSERT INTO table gb_mdl_mexico_costoproducir_views.wrkt_mf_turnos partition(entidadlegal_id)
               SELECT
               TC2.MF_Organizacion_ID
               ,TC2.Planta_ID
               ,TC2.Linea_Prod_ID
               ,TC2.Turno_ID 
               ,TC2.Periodo
               ,TC2.TurnoFechaIni
               ,TC2.TurnoFechaFin
               ,TC2.FechaInicio_Ant 
               ,TC2.FechaFin_Ant
               ,TC2.FechaInicio_Sig
               ,TC2.FechaFin_Sig
               ,TC2.TurnoHraIni
               ,TC2.TurnoHraFinal
               ,TC2.HoraInicio_Ant 
               ,TC2.HoraFin_Ant
               ,TC2.HoraInicio_Sig
               ,TC2.HoraFin_Sig
               ,TC2.Turno_HrInicio
               ,TC2.Turno_HrFin
               ,2 AS TipoCaso  -- Es dos cuando son turnos rotativos
               ,CASE WHEN lower(TC2.Observaciones) = lower('S/I') AND TC2.TurnoHraFinal <> TC2.Turno_HrFin THEN 'Se corrigio gap de tiempo'
                    ELSE TC2.Observaciones
               END AS Obervaciones
               ,from_unixtime(unix_timestamp())
               ,from_unixtime(unix_timestamp())
               ,TC2.EntidadLegal_ID
               FROM 
               (
                    SELECT 
                    T2.EntidadLegal_ID
                    ,T2.MF_Organizacion_ID
                    ,T2.Planta_ID
                    ,T2.Linea_Prod_ID
                    ,T2.Turno_ID
                    ,T2.Periodo 
                    ,T2.TurnoHraIni
                    ,T2.TurnoHraFinal
                    ,COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) AS cuenta_turnos 
                    ,CASE 
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 3 THEN COALESCE(T2.HrIni_Ant, T2.HrIni_AntP) 
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 2 THEN COALESCE(T2.HrIni_Ant, T2.HrIni_Sig)
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 1 THEN from_unixtime(unix_timestamp(T2.TurnoHraIni,'HH:mm:ss')-1,'HH:mm:ss')
                         ELSE T2.HrIni_Ant
                    END AS  HoraInicio_Ant 
                    ,CASE 
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 3 THEN COALESCE(T2.HrFin_Ant,T2.HrFin_AntP)
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 2 THEN COALESCE(T2.HrFin_Ant, T2.HrFin_Sig)
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 1 THEN from_unixtime(unix_timestamp(T2.TurnoHraFinal,'HH:mm:ss')-1,'HH:mm:ss')
                         ELSE T2.HrFin_Ant
                    END AS HoraFin_Ant
                    ,CASE 
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 3 THEN COALESCE(T2.HrIni_Sig, T2.HrIni_SigP)
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 2 THEN  COALESCE(T2.HrIni_Sig, T2.HrIni_Ant)
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 1 THEN from_unixtime(unix_timestamp(T2.TurnoHraFinal,'HH:mm:ss')-1,'HH:mm:ss')
                         ELSE T2.HrIni_Sig
                    END AS HoraInicio_Sig
                    ,CASE 
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 3 THEN COALESCE(T2.HrFin_Sig, T2.HrFin_SigP)
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 2 THEN COALESCE(T2.HrFin_Sig, T2.HrFin_Ant) 
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 1 THEN from_unixtime(unix_timestamp(T2.TurnoHraIni,'HH:mm:ss')-1,'HH:mm:ss')
                         ELSE T2.HrFin_Sig
                    END AS  HoraFin_Sig
                    ,T2.TurnoHraIni AS Turno_HrInicio  
                    ,CASE 
                         WHEN  COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 1 THEN from_unixtime(unix_timestamp(T2.TurnoHraIni,'HH:mm:ss')-1,'HH:mm:ss')
                         ELSE from_unixtime(unix_timestamp(
                              CASE 
                                   WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 3 THEN COALESCE(T2.HrIni_Sig, T2.HrIni_SigP)
                                   WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 2 THEN  COALESCE(T2.HrIni_Sig, T2.HrIni_Ant)
                                   WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 1 THEN from_unixtime(unix_timestamp(T2.TurnoHraFinal,'HH:mm:ss')-1,'HH:mm:ss')
                                   ELSE T2.HrIni_Sig
                              END
                              ,'HH:mm:ss')-1,'HH:mm:ss')
                    END AS Turno_HrFin 
                    
                    ,T2.TurnoFechaIni
                    ,T2.TurnoFechaFin
                    ,CASE 
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 3 THEN COALESCE(T2.FecIni_Ant, T2.FecIni_AntP) 
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 2 THEN COALESCE(T2.FecIni_Ant, T2.FecIni_Sig )
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 1 THEN T2.TurnoFechaIni
                         ELSE T2.FecIni_Ant
                    END AS FechaInicio_Ant 
                   ,CASE 
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 3 THEN COALESCE(T2.FecFin_Ant,T2.FecFin_AntP)
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 2 THEN COALESCE(T2.FecFin_Ant, T2.FecFin_Sig)
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 1 THEN T2.TurnoFechaFin
                         ELSE T2.FecFin_Ant
                   END AS FechaFin_Ant
                   ,CASE 
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 3 THEN COALESCE(T2.FecIni_Sig, T2.FecIni_SigP)
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 2 THEN COALESCE(T2.FecIni_Sig, T2.FecIni_Ant)
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 1 THEN T2.TurnoFechaFin
                         ELSE T2.FecIni_Sig
                   END AS FechaInicio_Sig
                  ,CASE 
                    WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 3 THEN COALESCE(T2.FecFin_Sig, T2.FecFin_SigP)
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 2 THEN COALESCE(T2.FecFin_Sig, T2.FecFin_Ant) 
                         WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 1 THEN T2.TurnoFechaIni
                         ELSE T2.FecFin_Sig
                   END AS  FechaFin_Sig
                  ,T2.TurnoFechaIni AS Turno_FecInicio
                  ,to_date(date_sub(
                         CASE 
                              WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 3 THEN COALESCE(T2.FecIni_Sig, T2.FecIni_SigP)
                              WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 2 THEN COALESCE(T2.FecIni_Sig, T2.FecIni_Ant)
                              WHEN COUNT(*) OVER (PARTITION BY T2.EntidadLegal_ID, T2.MF_Organizacion_ID, T2.Planta_ID, T2.Linea_Prod_ID ORDER BY T2.Turno_ID) = 1 THEN T2.TurnoFechaFin
                              ELSE T2.FecIni_Sig
                         END
                         ,1)
                  ) AS Turno_FecFin    
                  ,T2.Observaciones AS Observaciones

               FROM 
                         (
                              SELECT 
                              T.EntidadLegal_ID                  AS EntidadLegal_ID
                              ,T.MF_Organizacion_ID              AS  MF_Organizacion_ID
                              ,T.Planta_ID                       AS Planta_ID
                              ,T.Linea_Prod_ID                   AS Linea_Prod_ID
                              ,T.Turno_ID                        AS Turno_ID
                              ,T.Periodo                         AS Periodo
                              ,T.Hora_Inicia                     AS TurnoHraIni
                              ,T.Hora_Fin                        AS TurnoHraFinal
                              ,T.Fecha_Inicia                    AS  TurnoFechaIni
                              ,T.Fecha_Fin                       AS TurnoFechaFin
                              ,T.Observaciones                   AS Observaciones
                              
                              ,MIN(T.Hora_Inicia)      OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)     AS HrIni_Ant
                              ,MIN(T.Hora_Fin)         OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)        AS HrFin_Ant
                              ,MIN(T.Hora_Inicia)      OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID DESC ROWS BETWEEN 2 PRECEDING AND 2 PRECEDING)   AS HrIni_AntP
                              ,MIN(T.Hora_Fin)         OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID DESC ROWS BETWEEN 2 PRECEDING AND 2 PRECEDING)      AS HrFin_AntP
                              
                              ,MIN(T.Hora_Inicia)      OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)     AS HrIni_Sig
                              ,MIN(T.Hora_Fin)         OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)        AS HrFin_Sig
                              ,MIN(T.Hora_Inicia)      OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID DESC ROWS BETWEEN 2 FOLLOWING AND 2 FOLLOWING)   AS HrIni_SigP
                              ,MIN(T.Hora_Fin)         OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID DESC ROWS BETWEEN 2 FOLLOWING AND 2 FOLLOWING)      AS HrFin_SigP
                              
                              ,MIN(T.Fecha_Inicia)     OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)   AS FecIni_Ant
                              ,MIN(T.Fecha_Fin)        OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)      AS FecFin_Ant
                              ,MIN(T.Fecha_Inicia)     OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID DESC ROWS BETWEEN 2 PRECEDING AND 2 PRECEDING) AS FecIni_AntP
                              ,MIN(T.Fecha_Fin)        OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID DESC ROWS BETWEEN 2 PRECEDING AND 2 PRECEDING)    AS FecFin_AntP
                              
                              ,MIN(T.Fecha_Inicia)     OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)   AS FecIni_Sig
                              ,MIN(T.Fecha_Fin)        OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID ASC ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)      AS FecFin_Sig
                              ,MIN(T.Fecha_Inicia)     OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID DESC ROWS BETWEEN 2 FOLLOWING AND 2 FOLLOWING)  AS FecIni_SigP
                              ,MIN(T.Fecha_Fin)        OVER(PARTITION BY  T.EntidadLegal_ID, T.MF_Organizacion_ID, T.Planta_ID,T.Linea_Prod_ID, T.Periodo ORDER BY T.Turno_ID DESC ROWS BETWEEN 2 FOLLOWING AND 2 FOLLOWING)     AS FecFin_SigP
                    
                              FROM
                                   (
                                        SELECT  
                                        T1_.EntidadLegal_ID                    AS EntidadLegal_ID
                                        ,P.MF_Organizacion_ID                AS  MF_Organizacion_ID
                                        ,T1_.Planta_ID                         AS Planta_ID
                                        ,T1_.Linea_Prod_ID                     AS Linea_Prod_ID
                                        ,T1_.Turno_ID                          AS Turno_ID
                                        ,T1_.Periodo                           AS Periodo
                                        ,T1_.Hora_Inicia                       AS Hora_Inicia
                                        ,T1_.Hora_Fin                          AS Hora_Fin
                                        ,T1_.Fecha_Inicia                      AS  Fecha_Inicia
                                        ,T1_.Fecha_Fin                         AS Fecha_Fin
                                        ,'S/I'                               AS Observaciones
                                        FROM gb_mdl_mexico_costoproducir_views.v_mf_turnos_cuenta T1_ 
                                        JOIN gb_mdl_mexico_manufactura.MF_PLANTAS P  ON   TRIM(T1_.EntidadLegal_ID)  = TRIM(P.EntidadLegal_ID) AND TRIM(T1_.Planta_ID) = TRIM(P.Planta_ID) AND lower(Sistema_Fuente) = lower('CP')
                                        JOIN gb_mdl_mexico_manufactura.mf_lineas_prod L ON  TRIM(T1_.EntidadLegal_ID)  = TRIM(L.EntidadLegal_ID) AND  T1_.Linea_Prod_ID =  L.Linea_Prod_ID
                                        join 
                                        (
                                             -- Traemos aquellas EL activas y que solo estan en STG
                                             SELECT eamf1_.EntidadLegal_ID 
                                             FROM gb_mdl_mexico_costoproducir_views.V_ENTIDADESLEGALES_ACTIVAS_MF eamf1_
                                             WHERE eamf1_.EntidadLegal_ID  IN (SELECT EntidadLegal_ID FROM cp_flat_files.MF_TURNOS GROUP BY EntidadLegal_ID)
                                             GROUP BY eamf1_.EntidadLegal_ID
                                        ) eamf_2 on p.EntidadLegal_ID = eamf_2.EntidadLegal_ID
                                        WHERE T1_.Turno_ID IN (1,2,3) AND T1_.cuenta_tt = 1
                         
                                        UNION all
                         
                                        SELECT  
                                        T2_.EntidadLegal_ID                   AS EntidadLegal_ID
                                        ,P2.MF_Organizacion_ID               AS  MF_Organizacion_ID
                                        ,T2_.Planta_ID                        AS Planta_ID
                                        ,T2_.Linea_Prod_ID                    AS Linea_Prod_ID
                                        ,T2_.Turno_ID                         AS Turno_ID
                                        ,T2_.Periodo                          AS Periodo 
                                        ,T2_.Hora_Inicia                      AS Hora_Inicia 
                                        ,T2_.Hora_Fin                         AS Hora_Fin
                                        ,T2_.Fecha_Inicia                     AS  Fecha_Inicia
                                        ,T2_.Fecha_Fin                        AS Fecha_Fin
                                        ,T2_.Observaciones                    AS Observaciones
                                        FROM gb_mdl_mexico_costoproducir_views.wrkt_mf_turnos_rotativos T2_
                                        JOIN gb_mdl_mexico_costoproducir_views.V_MF_Turnos_Cuenta C
                                             ON T2_.EntidadLegal_ID = C.EntidadLegal_ID
                                             AND T2_.Planta_ID = C.Planta_ID
                                             AND T2_.Linea_Prod_ID = C.Linea_Prod_ID
                                             AND T2_.Turno_ID = C.Turno_ID
                                        JOIN gb_mdl_mexico_manufactura.MF_PLANTAS P2
                                             ON   TRIM(T2_.EntidadLegal_ID)  = TRIM(P2.EntidadLegal_ID) AND TRIM(T2_.Planta_ID) = TRIM(P2.Planta_ID)
                                        JOIN gb_mdl_mexico_manufactura.mf_lineas_prod L2
                                             ON  TRIM(T2_.EntidadLegal_ID)  = TRIM(L2.EntidadLegal_ID) AND  T2_.Linea_Prod_ID =  L2.Linea_Prod_ID  
                                        join (
                                             SELECT eamf2_.EntidadLegal_ID 
                                             FROM gb_mdl_mexico_costoproducir_views.V_ENTIDADESLEGALES_ACTIVAS_MF eamf2_
                                             WHERE eamf2_.EntidadLegal_ID  IN (SELECT EntidadLegal_ID FROM cp_flat_files.MF_TURNOS GROUP BY EntidadLegal_ID)
                                             GROUP BY eamf2_.EntidadLegal_ID
                                             )eamf3_ on P2.EntidadLegal_ID = eamf3_.EntidadLegal_ID
                                        WHERE T2_.Procesado IS NULL
                                             AND T2_.Turno_ID IN (1,2,3)
                                             AND T2_.Orden_Turno = ${hiveconf:orden_turno}
                                             AND C.cuenta_tt > 1
                                             AND lower(P2.Sistema_Fuente) = lower('CP')
                                        GROUP BY 
                                             T2_.EntidadLegal_ID
                                             ,P2.MF_Organizacion_ID
                                             ,T2_.Planta_ID
                                             ,T2_.Linea_Prod_ID
                                             ,T2_.Turno_ID
                                             ,T2_.Periodo
                                             ,T2_.Hora_Inicia
                                             ,T2_.Hora_Fin
                                             ,T2_.Fecha_Inicia
                                             ,T2_.Fecha_Fin
                                             ,T2_.Observaciones
                              )  T 
                    ) T2
               ) TC2
          LEFT OUTER JOIN (
               SELECT EntidadLegal_ID, MF_Organizacion_ID, Planta_ID, Linea_Prod_ID, Turno_ID,  Periodo, TurnoFechaIni, TurnoFechaFin, TurnoHraIni,TurnoHraFinal
                    FROM gb_mdl_mexico_costoproducir_views.wrkt_mf_turnos
               )wmft on TC2.EntidadLegal_ID = wmft.EntidadLegal_ID and TC2.MF_Organizacion_ID = wmft.MF_Organizacion_ID and TC2.Planta_ID = wmft.Planta_ID and TC2.Linea_Prod_ID = wmft.Linea_Prod_ID and TC2.Turno_ID = wmft.Turno_ID and TC2.Periodo = wmft.Periodo and TC2.TurnoFechaIni = wmft.TurnoFechaIni and TC2.TurnoFechaFin = wmft.TurnoFechaFin and TC2.TurnoHraIni = wmft.TurnoHraIni and TC2.TurnoHraFinal = wmft.TurnoHraFinal
          WHERE
          wmft.EntidadLegal_ID is null and wmft.MF_Organizacion_ID is null and wmft.Planta_ID is null and wmft.Linea_Prod_ID is null and wmft.Turno_ID is null and wmft.Periodo is null and wmft.TurnoFechaIni is null and wmft.TurnoFechaFin is null and wmft.TurnoHraIni is null and wmft.TurnoHraFinal is null
          and (TC2.TurnoHraFinal <> TC2.HoraInicio_Sig)
          AND TC2.TurnoHraIni <> TC2.HoraFin_Ant;


          -- Se actualiza el status de procesado en la tabla de trabajo WRKT_MF_Turnos_Rotativos
          insert overwrite table gb_mdl_mexico_costoproducir_views.wrkt_mf_turnos_rotativos partition(entidadlegal_id)
               select 
               mf_organizacion_id,
               planta_id,
               linea_prod_id,
               turno_id,
               periodo,
               fecha_inicia,
               fecha_fin,
               hora_inicia,
               hora_fin,
               orden_turno,
               'S',
               observaciones,
               storeday,
               entidadlegal_id
               from gb_mdl_mexico_costoproducir_views.wrkt_mf_turnos_rotativos
               where Orden_Turno = ${hiveconf:orden_turno};
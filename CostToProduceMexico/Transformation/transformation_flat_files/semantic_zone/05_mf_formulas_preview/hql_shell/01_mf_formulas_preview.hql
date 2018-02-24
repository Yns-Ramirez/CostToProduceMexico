-- ================== MF_FROMULAS HQL ==================
-- Author / Autor             : Francisco Martinez
-- Date / Fecha               : December 2016
-- Project /Proyecto          : Costo Producir-Big Data
-- Objective / Objetivo       : Update "MF_Formulas" information on table cp_dwh_mf.MF_Formulas
-- Subject Area / Area Sujeto : Manufacture 


-- FORMULAS PROCESS
-- SET Paso = 1;
TRUNCATE TABLE cp_dwh.FORM_STG_PRODS_TERMINADOS_CP PARTITION (EntidadLegal_ID);


--SET Paso = 2;
-- Obtenemos todos los productos PT producidos del mes
-- Enero 2013. Se modifica el paso para clasificar los insumos en 3 tipos: PT, MASAS y MP  
INSERT INTO cp_dwh.FORM_STG_PRODS_TERMINADOS_CP partition(Entidadlegal_id)
SELECT T.Periodo,  
T.Planta_ID,
T.InvItem_Hijos,  
T.Producto_ID,  
T.TranUOM,  
CASE WHEN P.Tipo_Producto_ID = 1 THEN 'PT'  
 WHEN P.Tipo_Producto_ID = 2 THEN 'SUB'
 WHEN P.Tipo_Producto_ID = 3 THEN 'MP'
 ELSE CodigoSubInv
END  AS CodigoSubInv,
T.CantidadPzasP,  
T.CantidadPzasH,
FROM_UNIXTIME(UNIX_TIMESTAMP()),
T.EntidadLegal_ID
FROM  
(
SELECT
A.Periodo,  
TRIM(B.EntidadLegal_ID) AS EntidadLegal_ID,  
A.Planta_ID,
A.InvItem_Hijos,  
A.InvItem_PT AS Producto_ID,  
A.TranUOM,  
MAX(A.CodigoSubInv) AS CodigoSubInv,  
SUM(A.CantidadPzasP) AS CantidadPzasP,  
SUM(A.CantidadPzas) AS CantidadPzasH
FROM 
(
SELECT    
A.ORGANIZATION_ID AS Planta_ID,  
A.INVENTORY_ITEM_ID AS InvItem_Hijos,  
B.Producto_ID AS InvItem_PT,  
A.TRANSACTION_UOM AS TranUOM,  
A.SUBINVENTORY_CODE AS CodigoSubInv,  
CAST(A.PRIMARY_QUANTITY AS FLOAT) AS CantidadPzas,
B.CantidadPzas AS CantidadPzasP,
A.TRANSACTION_DATE AS TranDate,  
B.Periodo  
FROM cp_view.v_MTL_TRANSACCION_MATERIALES A, 
( 
SELECT
A.TRANSACTION_SET_ID AS TranSet_ID,  
A.ORGANIZATION_ID AS Planta_ID,  
A.INVENTORY_ITEM_ID AS Producto_ID,  
CAST(A.PRIMARY_QUANTITY AS FLOAT)  AS CantidadPzas,  
TRIM(SUBSTRING(A.TRANSACTION_DATE,1,7)) AS Periodo
FROM cp_view.v_MTL_TRANSACCION_MATERIALES A, 
cp_dwh_mf.MF_Plantas B,
cp_dwh.mtl_catalogo_materiales C 
WHERE A.ORGANIZATION_ID=B.MF_Organizacion_ID
AND A.ORGANIZATION_ID=C.ORGANIZATION_ID 
AND A.INVENTORY_ITEM_ID=C.INVENTORY_ITEM_ID
AND (
((A.TRANSACTION_TYPE_ID = 17 AND A.TRANSACTION_SOURCE_TYPE_ID = 5) 
OR (A.TRANSACTION_TYPE_ID = 44 AND A.TRANSACTION_SOURCE_TYPE_ID = 5))
AND A.TRANSACTION_DATE BETWEEN '${hiveconf:V_PRIMER_DIA}' AND '${hiveconf:V_ULTIMO_DIA}') 
AND C.ITEM_TYPE LIKE 'PT%'
AND  B.EntidadLegal_ID IN (SELECT EntidadLegal_ID 
FROM cp_view.v_entidadeslegales_activas_for WHERE TRIM(Aplicacion)='FORMULAS' AND EntidadLegal_ID in ('100','101') GROUP BY EntidadLegal_ID) 
) B
WHERE ((A.TRANSACTION_SET_ID=B.TranSet_ID 
AND A.ORGANIZATION_ID=B.Planta_ID)
AND TRIM(SUBSTRING(A.TRANSACTION_DATE,1,7))=B.Periodo) 
AND (((A.TRANSACTION_TYPE_ID = 35 AND A.TRANSACTION_SOURCE_TYPE_ID = 5) 
 OR (A.TRANSACTION_TYPE_ID = 43 AND A.TRANSACTION_SOURCE_TYPE_ID = 5 AND A.PRIMARY_QUANTITY < 0)) 
 OR ((A.TRANSACTION_TYPE_ID = 17 AND A.TRANSACTION_SOURCE_TYPE_ID = 5) 
 OR (A.TRANSACTION_TYPE_ID = 44 AND A.TRANSACTION_SOURCE_TYPE_ID = 5)))
) A, cp_dwh_mf.mf_plantas B
WHERE A.Planta_ID = B.mf_organizacion_id
AND (B.EntidadLegal_ID IN (SELECT EntidadLegal_ID 
FROM cp_view.v_entidadeslegales_activas_for 
WHERE TRIM(Aplicacion) = 'FORMULAS' AND EntidadLegal_ID IN ('100','101') GROUP BY EntidadLegal_ID)) 
AND A.InvItem_PT <> A.InvItem_Hijos
GROUP BY A.Periodo, B.EntidadLegal_ID,A.Planta_ID,A.InvItem_Hijos,A.InvItem_PT,A.TranUOM
) T
LEFT JOIN cp_dwh_mf.mf_producto_organizacion P
ON T.entidadlegal_id = P.entidadlegal_id
AND T.Planta_ID = P.MF_Organizacion_ID
AND T.InvItem_Hijos = P.MF_Producto_ID;

---DESARROLLO PARA CHINA realizar delete para caracteres chinos


-- SET Paso = 3;
TRUNCATE TABLE cp_dwh.FORM_STG_SUBENSAMBLES_CP;


-- SET Paso = 4;
-- Ontenemos todos los subensambles del mes sin importar en que nivel estan.
-- Enero 2013. Se modifica el paso para clasificar los insumos en 3 tipos: PT, MASAS y MP
INSERT INTO cp_dwh.FORM_STG_SUBENSAMBLES_CP partition(entidadlegal_id)
SELECT T.Periodo,  
T.SubEnsamble_ID,  
CASE WHEN P.Tipo_Producto_ID = 1 THEN 'PT'
WHEN P.Tipo_Producto_ID = 2 THEN 'SUB'
WHEN P.Tipo_Producto_ID = 3 THEN 'MP'
ELSE CodigoSubInv
END AS CodigoSubInv,
T.Planta_ID,  
T.Ingrediente_ID,  
T.TranUOM,
T.Cantidad,  
T.Masa,
FROM_UNIXTIME(UNIX_TIMESTAMP()),
T.EntidadLegal_ID
FROM
( 
SELECT   
C.Periodo AS Periodo,  
TRIM(B.EntidadLegal_ID) AS EntidadLegal_ID,  
C.SubEnsamble_ID AS SubEnsamble_ID,  
TRIM(MAX(C.CodigoSubInv)) AS CodigoSubInv,  
C.Planta_ID,  
C.Ingrediente_ID,  
TRIM(C.TranUOM) AS TranUOM,  
SUM(C.CantidadPzas) AS Cantidad,  
SUM(C.CantidadPzasM) AS Masa
FROM
(
SELECT
A.INVENTORY_ITEM_ID AS Ingrediente_ID,  
A.ORGANIZATION_ID AS Planta_ID,  
A.PRIMARY_QUANTITY AS CantidadPzas,  
TRIM(A.TRANSACTION_UOM) AS TranUOM,  
A.TRANSACTION_DATE AS TranDate,  
B.Producto_ID AS SubEnsamble_ID,  
B.CantidadPzas AS CantidadPzasM,  
TRIM(A.SUBINVENTORY_CODE) AS CodigoSubInv,  
B.Periodo AS Periodo 
FROM cp_view.v_MTL_TRANSACCION_MATERIALES A, 
(
SELECT    
MTL.INVENTORY_ITEM_ID AS Producto_ID,  
MTL.ORGANIZATION_ID AS Planta_ID,  
MTL.PRIMARY_QUANTITY AS CantidadPzas,  
MTL.COMPLETION_TRANSACTION_ID AS Tran_Comp_ID,
TRIM(SUBSTRING(MTL.TRANSACTION_DATE,1,7)) AS Periodo
FROM cp_view.v_MTL_TRANSACCION_MATERIALES MTL  
WHERE 
(
(
(MTL.TRANSACTION_TYPE_ID=17 AND MTL.TRANSACTION_SOURCE_TYPE_ID=5 AND MTL.PRIMARY_QUANTITY > 0)
OR (MTL.TRANSACTION_TYPE_ID=44 AND MTL.TRANSACTION_SOURCE_TYPE_ID=5 AND MTL.PRIMARY_QUANTITY > 0) 
OR (MTL.TRANSACTION_TYPE_ID=35 AND MTL.TRANSACTION_SOURCE_TYPE_ID=5 AND MTL.PRIMARY_QUANTITY > 0) 
OR (MTL.TRANSACTION_TYPE_ID=43 AND MTL.TRANSACTION_SOURCE_TYPE_ID=5 AND MTL.PRIMARY_QUANTITY < 0) 
OR (MTL.TRANSACTION_TYPE_ID=48 AND MTL.TRANSACTION_SOURCE_TYPE_ID=5 AND MTL.PRIMARY_QUANTITY > 0) 
OR (MTL.TRANSACTION_TYPE_ID=38 AND MTL.TRANSACTION_SOURCE_TYPE_ID=5 AND MTL.PRIMARY_QUANTITY > 0)
)
AND  MTL.SUBINVENTORY_CODE LIKE '%SUB%'
) 
AND (MTL.TRANSACTION_DATE BETWEEN '${hiveconf:V_PRIMER_DIA}' AND '${hiveconf:V_ULTIMO_DIA}')
AND MTL.PRIMARY_QUANTITY > 0
) B  
WHERE ((A.COMPLETION_TRANSACTION_ID = B.Tran_Comp_ID 
AND A.ORGANIZATION_ID = B.Planta_ID) 
AND TRIM(SUBSTRING(A.TRANSACTION_DATE,1,7))=B.Periodo)
) C, cp_dwh_mf.MF_Plantas B
WHERE C.Planta_ID=B.MF_Organizacion_ID
AND (B.EntidadLegal_ID IN (SELECT EntidadLegal_ID FROM cp_view.v_entidadeslegales_activas_for WHERE TRIM(Aplicacion) = 'FORMULAS' 
AND EntidadLegal_ID IN ('100','101') GROUP BY EntidadLegal_ID)) 
GROUP BY C.SubEnsamble_ID, TRIM(C.TranUOM),C.Planta_ID, C.Ingrediente_ID, C.Periodo, TRIM(B.EntidadLegal_ID)
) T
LEFT JOIN cp_dwh_mf.mf_producto_organizacion P
ON T.entidadlegal_id = P.entidadlegal_id 
AND T.Planta_ID = P.MF_Organizacion_ID 
AND T.Ingrediente_ID = P.MF_Producto_ID;


--SET Paso = 5;
TRUNCATE TABLE cp_dwh.FORM_ODS_CM_A_01_2;


--SET Paso = 6;
--Insertamos las máximas masas utilizadas dentro de un PT o un Subensamble
INSERT INTO cp_dwh.FORM_ODS_CM_A_01_2 partition(Entidadlegal_id)
SELECT 
Periodo,  
Planta_ID,  
InvItem_Hijos,  
Producto_ID,  
TranUOM,  
CodigoSubInv,  
CantidadPzasP,  
CantidadPzasH,
FROM_UNIXTIME(UNIX_TIMESTAMP()),
EntidadLegal_ID 
FROM cp_view.V_FORM_ODS_CM_A_01_2;


--SET Paso = 7;
-- Se insertan excpciones de nivel 0 que no encontro en el primer calculo de los SUB 
--(Es decir todo lo que es SUB pero que esta marcado como MP)

INSERT INTO cp_dwh.FORM_STG_SUBENSAMBLES_CP partition(entidadlegal_id) 
SELECT 
A.Periodo
,A.Producto_ID
-- Enero 2013. Se modifica el paso para clasificar los insumos en 3 tipos: PT, MASAS y MP.
,CASE WHEN P.Tipo_Producto_ID = 1 THEN 'PT' WHEN P.Tipo_Producto_ID = 2 THEN 'SUB'
WHEN P.Tipo_Producto_ID = 3 THEN 'MP'
ELSE CodigoSubInv
END AS CodigoSubInv
,A.Planta_ID
,A.Ingrediente_ID
,'-kg'
,A.Cantidad*a.factor
,A.Masa
,FROM_UNIXTIME(UNIX_TIMESTAMP())
,A.EntidadLegal_ID
FROM 
(
SELECT DISTINCT 
A.Periodo, A.EntidadLegal_ID, A.Producto_ID, 
'SUB ' AS CodigoSubInv,A.Planta_ID, A.Ingrediente_ID, 'msa' AS TranUOM, A.Factor, A.Cantidad, A.Masa
FROM cp_view.V_FORM_ODS_CM_A_01_3 A
INNER JOIN cp_dwh.FORM_STG_SUBENSAMBLES_CP B
ON A.Periodo = B.Periodo 
AND A.EntidadLegal_ID = B.EntidadLegal_ID 
AND A.Planta_ID = B.Planta_ID 
AND A.Ingrediente_ID = B.SubEnsamble_ID
LEFT OUTER JOIN cp_dwh.FORM_STG_SUBENSAMBLES_CP C
ON A.Periodo = C.Periodo 
AND A.EntidadLegal_ID = C.EntidadLegal_ID 
AND A.Planta_ID = C.Planta_ID 
AND A.Producto_ID = C.SubEnsamble_ID
AND A.Ingrediente_ID = C.Ingrediente_ID 
WHERE C.Periodo is NULL 
AND C.EntidadLegal_ID is NULL 
AND C.Planta_ID is NULL 
AND C.SubEnsamble_ID is NULL
AND C.Ingrediente_ID is NULL
) A
LEFT JOIN cp_dwh_mf.mf_producto_organizacion P
ON A.entidadlegal_id = P.entidadlegal_id 
AND A.Planta_ID = P.MF_Organizacion_ID 
AND A.Ingrediente_ID  = P.MF_Producto_ID; 


--SET Paso = 8;
--Se buscan los ingredientes que presentan problemas por recursion mal generada desde la fuente.
-- **Este proceso trata de filtrar problemas de ingredientes para el primer nivel. 
INSERT INTO cp_dwh.STATUS_SP_LOG  
SELECT DISTINCT 
64,
'MANUFACTURA',
'MF_FORMULAS_PASO_1_TO_7',
0,
CURRENT_TIMESTAMP,
CURRENT_TIMESTAMP,
CURRENT_TIMESTAMP,
0,
CONCAT('Eliminacion de ingredientes por recursividad incorrecta **Problemas de datos en el ERP. Estos ingredientes no se procesaran**: Periodo: ',TRIM(CAST(TBL1.Periodo AS STRING)), '  EntidadLegal: ',TRIM(CAST(TBL1.EntidadLegal_ID AS STRING)),'   Planta: ' , TRIM(CAST(TBL1.Planta_ID AS STRING)), '    Ingrediente: ',TRIM(CAST(TBL1.Ingrediente_ID AS STRING))),
FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM 
(
SELECT 
A.Periodo, 
A.EntidadLegal_ID,
A.CodigoSubInv, 
A.Planta_ID,
A.SubEnsamble_ID, 
A.Ingrediente_ID,
B.Ingrediente_ID Ingrediente_ID_1, 
C.Ingrediente_ID Ingrediente_ID_2
FROM cp_dwh.FORM_STG_SUBENSAMBLES_CP A, 
cp_dwh.FORM_STG_SUBENSAMBLES_CP B, 
cp_dwh.FORM_STG_SUBENSAMBLES_CP C
WHERE A.Ingrediente_ID =C.Ingrediente_ID
AND A.Planta_ID = B.Planta_ID 
AND A.Periodo = B.Periodo 
AND A.EntidadLegal_ID = B.EntidadLegal_ID 
AND A.Ingrediente_ID = B.SubEnsamble_ID 
AND A.Ingrediente_ID <> B.Ingrediente_ID 
AND B.EntidadLegal_ID = C.EntidadLegal_ID 
AND B.Periodo = C.Periodo 
AND B.Planta_ID = C.Planta_ID 
AND B.Ingrediente_ID = C.SubEnsamble_ID 
AND A.SubEnsamble_ID<>A.Ingrediente_ID 
AND B.Ingrediente_ID<>C.Ingrediente_ID
)TBL1;


-- Se eliminan los productos para que no causen problemas en la recursion en una recursion infinita 
INSERT OVERWRITE TABLE cp_dwh.FORM_STG_SUBENSAMBLES_CP partition(EntidadLegal_ID)
SELECT tmp.* from cp_dwh.FORM_STG_SUBENSAMBLES_CP tmp
LEFT OUTER JOIN (
SELECT DISTINCT TBL.Periodo,TBL.EntidadLegal_ID,TBL.Planta_ID,TBL.Ingrediente_ID FROM 
(SELECT 
A.Periodo AS Periodo , 
A.EntidadLegal_ID AS EntidadLegal_ID,
A.CodigoSubInv AS CodigoSubInv, 
A.Planta_ID AS Planta_ID,
A.SubEnsamble_ID AS SubEnsamble_ID, 
A.Ingrediente_ID AS Ingrediente_ID,
B.Ingrediente_ID AS Ingrediente_ID_1, 
C.Ingrediente_ID AS Ingrediente_ID_2,
FROM_UNIXTIME(UNIX_TIMESTAMP())
FROM cp_dwh.FORM_STG_SUBENSAMBLES_CP A, 
cp_dwh.FORM_STG_SUBENSAMBLES_CP B, 
cp_dwh.FORM_STG_SUBENSAMBLES_CP C
WHERE A.Ingrediente_ID =C.Ingrediente_ID
AND A.Planta_ID = B.Planta_ID 
AND A.Periodo = B.Periodo 
AND A.EntidadLegal_ID = B.EntidadLegal_ID 
AND A.Ingrediente_ID = B.SubEnsamble_ID 
AND A.Ingrediente_ID <> B.Ingrediente_ID 
AND B.EntidadLegal_ID = C.EntidadLegal_ID 
AND B.Periodo = C.Periodo 
AND B.Planta_ID = C.Planta_ID 
AND B.Ingrediente_ID = C.SubEnsamble_ID 
AND A.SubEnsamble_ID<>A.Ingrediente_ID 
AND B.Ingrediente_ID<>C.Ingrediente_ID) TBL
) sec 
on tmp.Periodo = sec.Periodo
and tmp.EntidadLegal_ID = sec.EntidadLegal_ID
and tmp.Planta_ID = sec.Planta_ID
and tmp.Ingrediente_ID = sec.Ingrediente_ID
where sec.Periodo is null
and sec.EntidadLegal_ID is null
and sec.Planta_ID is null
and sec.Ingrediente_ID is null;


--SET Paso = 9;
TRUNCATE TABLE cp_dwh.FORM_STG_SUBENSAMBLES_CP_PASO;


--SET Paso = 10;
-- Se necesita aplicar la  maxima masa por subensamble, en algunos caso se tiene diferencia en las masas, por lo que resulta necesario buscar las masas maximas a un inicio del procedimiento  recursivo
-- Noviembre 2012. Se cambio la masa que se va a tomar, ahora se tomara la masa que mas se repite. 
INSERT INTO cp_dwh.FORM_STG_SUBENSAMBLES_CP_PASO partition(entidadlegal_id)
SELECT DISTINCT   
 a.Periodo,
 a.SubEnsamble_ID,
 a.CodigoSubInv,
 a.Planta_ID,
 a.Ingrediente_ID,
 a.TranUOM,
 a.Cantidad,
 COALESCE(b.Masa,a.Masa) Masa,
 FROM_UNIXTIME(UNIX_TIMESTAMP()),
 a.EntidadLegal_ID
FROM cp_dwh.FORM_STG_SUBENSAMBLES_CP a  
LEFT OUTER JOIN (
SELECT A.Periodo, A.EntidadLegal_ID, A.Planta_ID, A.CodigoSubInv, B.SubEnsamble_ID, b.masa   --MAX(b.masa) masa
FROM cp_dwh.FORM_STG_SUBENSAMBLES_CP A , 
-- Se creo la vista V_FORM_STG_SUBS_CP_MASA para traer la masa que mas se repite
cp_view.V_FORM_STG_SUBS_CP_MASA  B
WHERE A.EntidadLegal_ID = B.EntidadLegal_ID 
 AND A.Periodo = B.Periodo 
 AND A.Planta_ID = B.Planta_ID 
 AND A.Ingrediente_ID = B.SubEnsamble_ID
GROUP BY A.Periodo, A.EntidadLegal_ID, A.Planta_ID, A.CodigoSubInv, B.SubEnsamble_ID, b.masa
) B ON a.Periodo = b.Periodo
AND a.SubEnsamble_ID = b.SubEnsamble_ID
AND a.EntidadLegal_ID = b.EntidadLegal_ID
AND a.Planta_ID = b.Planta_ID;


--SET Paso = 11;
TRUNCATE TABLE cp_dwh.FORM_STG_SUBENSAMBLES_CP;
-- Se limpia el dato de masa y se envia ya los datos de masas que deben estar presentes en toda la formula
INSERT INTO cp_dwh.FORM_STG_SUBENSAMBLES_CP partition(entidadlegal_id)
SELECT * 
FROM cp_dwh.FORM_STG_SUBENSAMBLES_CP_PASO; 


--SET Paso = 12;
--Se limpia la tabla que tendra el resultado de la recursion
TRUNCATE TABLE cp_dwh.T_FORM_STG_SUBENSFORM_CP;
TRUNCATE TABLE cp_dwh.T_FORM_STG_SUBENSFORM_CP_TEMP;


--ETAPA 2 
--Materializar subensambles
with Q1 AS (select p.Periodo as Periodo
,p.EntidadLegal_ID as EntidadLegal_ID
,p.Planta_ID as Planta_ID
,p.SubEnsamble_ID as SubEnsamble_ID_Ori
,p.SubEnsamble_ID as SubEnsamble_ID
,p.CodigoSubInv as CodigoSubInv
,p.Ingrediente_ID as Ingrediente_ID, 
p.cantidad as cantidad
,p.Masa as Masa,
(p.cantidad/p.Masa) as fact,
0 AS nivel 
from cp_dwh.FORM_STG_SUBENSAMBLES_CP p),
parents as (
SELECT
a.Periodo as Periodo, 
a.EntidadLegal_ID as EntidadLegal_ID, 
a.Planta_ID as Planta_ID, 
a.SubEnsamble_ID_Ori as SubEnsamble_ID_Ori,
sub.SubEnsamble_ID as SubEnsamble_ID, 
sub.CodigoSubInv as CodigoSubInv, 
sub.Ingrediente_ID as Ingrediente_ID, 
sub.cantidad as cantidad,
sub.Masa as Masa,
(sub.cantidad/sub.Masa) * a.fact as fact,
a.nivel+1 AS nivel   
FROM Q1 a
INNER JOIN cp_dwh.FORM_STG_SUBENSAMBLES_CP sub
ON a.Periodo = sub.Periodo
AND a.EntidadLegal_ID = sub.EntidadLegal_ID
AND a.Planta_ID = sub.Planta_ID
AND a.Ingrediente_ID = sub.SubEnsamble_ID
AND a.nivel < 10
AND a.CodigoSubInv <> 'PT'
 -- Enero 2013. Se agrego linea de codigo, para que no rompa los PT que estan como ingredientes.
),
child1 as (
SELECT
a.Periodo as Periodo, 
a.EntidadLegal_ID as EntidadLegal_ID, 
a.Planta_ID as Planta_ID, 
a.SubEnsamble_ID_Ori as SubEnsamble_ID_Ori,
sub.SubEnsamble_ID as SubEnsamble_ID, 
sub.CodigoSubInv as CodigoSubInv, 
sub.Ingrediente_ID as Ingrediente_ID, 
sub.cantidad as cantidad,
sub.Masa as Masa,
(sub.cantidad/sub.Masa) * a.fact as fact,
a.nivel+1 AS nivel
FROM parents a 
INNER JOIN cp_dwh.FORM_STG_SUBENSAMBLES_CP sub
ON a.Periodo = sub.Periodo
AND a.EntidadLegal_ID = sub.EntidadLegal_ID
AND a.Planta_ID = sub.Planta_ID
AND a.Ingrediente_ID = sub.SubEnsamble_ID
AND a.nivel < 10
AND a.CodigoSubInv <> 'PT'
 -- Enero 2013. Se agrego linea de codigo, para que no rompa los PT que estan como ingredientes.
),
child2 as (
SELECT
a.Periodo as Periodo, 
a.EntidadLegal_ID as EntidadLegal_ID, 
a.Planta_ID as Planta_ID, 
a.SubEnsamble_ID_Ori as SubEnsamble_ID_Ori,
sub.SubEnsamble_ID as SubEnsamble_ID, 
sub.CodigoSubInv as CodigoSubInv, 
sub.Ingrediente_ID as Ingrediente_ID, 
sub.cantidad as cantidad,
sub.Masa as Masa,
(sub.cantidad/sub.Masa) * a.fact as fact,
a.nivel+1 AS nivel
FROM child1 a
INNER JOIN cp_dwh.FORM_STG_SUBENSAMBLES_CP sub
ON a.Periodo = sub.Periodo
AND a.EntidadLegal_ID = sub.EntidadLegal_ID
AND a.Planta_ID = sub.Planta_ID
AND a.Ingrediente_ID = sub.SubEnsamble_ID
AND a.nivel < 10
AND a.CodigoSubInv <> 'PT'
),
child3 as (
SELECT
a.Periodo as Periodo, 
a.EntidadLegal_ID as EntidadLegal_ID, 
a.Planta_ID as Planta_ID, 
a.SubEnsamble_ID_Ori as SubEnsamble_ID_Ori,
sub.SubEnsamble_ID as SubEnsamble_ID, 
sub.CodigoSubInv as CodigoSubInv, 
sub.Ingrediente_ID as Ingrediente_ID, 
sub.cantidad as cantidad,
sub.Masa as Masa,
(sub.cantidad/sub.Masa) * a.fact as fact,
a.nivel+1 AS nivel
FROM child2 a 
INNER JOIN cp_dwh.FORM_STG_SUBENSAMBLES_CP sub
ON a.Periodo = sub.Periodo
AND a.EntidadLegal_ID = sub.EntidadLegal_ID
AND a.Planta_ID = sub.Planta_ID
AND a.Ingrediente_ID = sub.SubEnsamble_ID
AND a.nivel < 10
AND a.CodigoSubInv <> 'PT'
),child4 as (
SELECT
a.Periodo as Periodo, 
a.EntidadLegal_ID as EntidadLegal_ID, 
a.Planta_ID as Planta_ID, 
a.SubEnsamble_ID_Ori as SubEnsamble_ID_Ori,
sub.SubEnsamble_ID as SubEnsamble_ID, 
sub.CodigoSubInv as CodigoSubInv, 
sub.Ingrediente_ID as Ingrediente_ID, 
sub.cantidad as cantidad,
sub.Masa as Masa,
(sub.cantidad/sub.Masa) * a.fact as fact,
a.nivel+1 AS nivel
FROM child3 a
INNER JOIN cp_dwh.FORM_STG_SUBENSAMBLES_CP sub
ON a.Periodo = sub.Periodo
AND a.EntidadLegal_ID = sub.EntidadLegal_ID
AND a.Planta_ID = sub.Planta_ID
AND a.Ingrediente_ID = sub.SubEnsamble_ID
AND a.nivel < 10
AND a.CodigoSubInv <> 'PT'
),child5 as (
SELECT
a.Periodo as Periodo, 
a.EntidadLegal_ID as EntidadLegal_ID, 
a.Planta_ID as Planta_ID, 
a.SubEnsamble_ID_Ori as SubEnsamble_ID_Ori,
sub.SubEnsamble_ID as SubEnsamble_ID, 
sub.CodigoSubInv as CodigoSubInv, 
sub.Ingrediente_ID as Ingrediente_ID, 
sub.cantidad as cantidad,
sub.Masa as Masa,
(sub.cantidad/sub.Masa) * a.fact as fact,
a.nivel+1 AS nivel
FROM child4 a
INNER JOIN cp_dwh.FORM_STG_SUBENSAMBLES_CP sub
ON a.Periodo = sub.Periodo
AND a.EntidadLegal_ID = sub.EntidadLegal_ID
AND a.Planta_ID = sub.Planta_ID
AND a.Ingrediente_ID = sub.SubEnsamble_ID
AND a.nivel < 10
AND a.CodigoSubInv <> 'PT'
),child6 as (
SELECT
a.Periodo as Periodo, 
a.EntidadLegal_ID as EntidadLegal_ID, 
a.Planta_ID as Planta_ID, 
a.SubEnsamble_ID_Ori as SubEnsamble_ID_Ori,
sub.SubEnsamble_ID as SubEnsamble_ID, 
sub.CodigoSubInv as CodigoSubInv, 
sub.Ingrediente_ID as Ingrediente_ID, 
sub.cantidad as cantidad,
sub.Masa as Masa,
(sub.cantidad/sub.Masa) * a.fact as fact,
a.nivel+1 AS nivel
FROM child5 a 
INNER JOIN cp_dwh.FORM_STG_SUBENSAMBLES_CP sub
ON a.Periodo = sub.Periodo
AND a.EntidadLegal_ID = sub.EntidadLegal_ID
AND a.Planta_ID = sub.Planta_ID
AND a.Ingrediente_ID = sub.SubEnsamble_ID
AND a.nivel < 10
AND a.CodigoSubInv <> 'PT'
),child7 as (
SELECT
a.Periodo as Periodo, 
a.EntidadLegal_ID as EntidadLegal_ID, 
a.Planta_ID as Planta_ID, 
a.SubEnsamble_ID_Ori as SubEnsamble_ID_Ori,
sub.SubEnsamble_ID as SubEnsamble_ID, 
sub.CodigoSubInv as CodigoSubInv, 
sub.Ingrediente_ID as Ingrediente_ID, 
sub.cantidad as cantidad,
sub.Masa as Masa,
(sub.cantidad/sub.Masa) * a.fact as fact,
a.nivel+1 AS nivel
FROM child6 a
INNER JOIN cp_dwh.FORM_STG_SUBENSAMBLES_CP sub
ON a.Periodo = sub.Periodo
AND a.EntidadLegal_ID = sub.EntidadLegal_ID
AND a.Planta_ID = sub.Planta_ID
AND a.Ingrediente_ID = sub.SubEnsamble_ID
AND a.nivel < 10
AND a.CodigoSubInv <> 'PT'
),child8 as (
SELECT
a.Periodo as Periodo, 
a.EntidadLegal_ID as EntidadLegal_ID, 
a.Planta_ID as Planta_ID, 
a.SubEnsamble_ID_Ori as SubEnsamble_ID_Ori,
sub.SubEnsamble_ID as SubEnsamble_ID, 
sub.CodigoSubInv as CodigoSubInv, 
sub.Ingrediente_ID as Ingrediente_ID, 
sub.cantidad as cantidad,
sub.Masa as Masa,
(sub.cantidad/sub.Masa) * a.fact as fact,
a.nivel+1 AS nivel
FROM child7 a
INNER JOIN cp_dwh.FORM_STG_SUBENSAMBLES_CP sub
ON a.Periodo = sub.Periodo
AND a.EntidadLegal_ID = sub.EntidadLegal_ID
AND a.Planta_ID = sub.Planta_ID
AND a.Ingrediente_ID = sub.SubEnsamble_ID
AND a.nivel < 10
AND a.CodigoSubInv <> 'PT'
),child9 as (
SELECT
a.Periodo as Periodo, 
a.EntidadLegal_ID as EntidadLegal_ID, 
a.Planta_ID as Planta_ID, 
a.SubEnsamble_ID_Ori as SubEnsamble_ID_Ori,
sub.SubEnsamble_ID as SubEnsamble_ID, 
sub.CodigoSubInv as CodigoSubInv, 
sub.Ingrediente_ID as Ingrediente_ID, 
sub.cantidad as cantidad,
sub.Masa as Masa,
(sub.cantidad/sub.Masa) * a.fact as fact,
a.nivel+1 AS nivel
FROM child8 a
INNER JOIN cp_dwh.FORM_STG_SUBENSAMBLES_CP sub
ON a.Periodo = sub.Periodo
AND a.EntidadLegal_ID = sub.EntidadLegal_ID
AND a.Planta_ID = sub.Planta_ID
AND a.Ingrediente_ID = sub.SubEnsamble_ID
AND a.nivel < 10
AND a.CodigoSubInv <> 'PT'
)

INSERT INTO cp_dwh.T_FORM_STG_SUBENSFORM_CP_TEMP
SELECT *
FROM Q1 p
UNION ALL
SELECT *
FROM parents p
UNION ALL
SELECT *
FROM child1 p
UNION ALL
SELECT *
FROM child2 p
UNION ALL
SELECT *
FROM child3 p
UNION ALL
SELECT *
FROM child4 p
UNION ALL
SELECT *
FROM child5 p
UNION ALL
SELECT *
FROM child6 p
UNION ALL
SELECT *
FROM child7 p
UNION ALL
SELECT *
FROM child8 p
UNION ALL
SELECT *
FROM child9 p;

INSERT INTO cp_dwh.T_FORM_STG_SUBENSFORM_CP partition(entidadlegal_id)
SELECT a.Periodo, a.Planta_ID, a.SubEnsamble_ID_Ori,a.SubEnsamble_ID, a.CodigoSubInv, 
a.Ingrediente_ID,ABS(a.fact) fact,a.delnivel,FROM_UNIXTIME(UNIX_TIMESTAMP()),a.EntidadLegal_ID
FROM cp_view.V_FORM_All_SubEns a, 
(SELECT Periodo, 
EntidadLegal_ID,
Planta_ID, 
SubEnsamble_ID_Ori,
SubEnsamble_ID, 
Ingrediente_ID,
MIN(delnivel) as delnivel
FROM cp_view.V_FORM_All_SubEns
WHERE subensamble_id <> ingrediente_id
GROUP BY Periodo,EntidadLegal_ID,Planta_ID,SubEnsamble_ID_Ori,SubEnsamble_ID,Ingrediente_ID) b
WHERE a.subensamble_id <> a.ingrediente_id
AND a.EntidadLegal_ID IN (SELECT EntidadLegal_ID FROM cp_dwh.gx_control_entidades_app WHERE TRIM(Aplicacion) = 'FORMULAS' AND EntidadLegal_ID IN ('100','101') GROUP BY EntidadLegal_ID)
AND a.Periodo=b.Periodo
AND a.EntidadLegal_ID=b.EntidadLegal_ID
AND a.Planta_ID=b.Planta_ID
AND a.SubEnsamble_ID_Ori=b.SubEnsamble_ID_Ori
AND a.SubEnsamble_ID=b.SubEnsamble_ID
AND a.Ingrediente_ID=b.Ingrediente_ID
AND a.delnivel=b.delnivel;


 -- SET Paso = 1
 -- Eliminamos de la tabla que se materializo aquellos subnsambles 
 -- que son hijos pero que no necesitamos llevar a la formula final porque no son insumos de ultimo nivel
 -- Para que al final solo queden items de tipo insumos MP

INSERT OVERWRITE table cp_dwh.T_FORM_STG_SUBENSFORM_CP partition(entidadlegal_id) 
select tmp.* from cp_dwh.T_FORM_STG_SUBENSFORM_CP tmp
join (
SELECT c.Periodo, c.EntidadLegal_ID, c.Planta_ID, c.SubEnsamble_ID_Ori, c.SubEnsamble_ID, c.Ingrediente_ID
FROM cp_dwh.T_FORM_STG_SUBENSFORM_CP c,
( SELECT a.Periodo,a.EntidadLegal_ID,a.Planta_ID,a.SubEnsamble_ID_Ori,a.SubEnsamble_ID,a.Ingrediente_ID
FROM cp_dwh.T_FORM_STG_SUBENSFORM_CP a,
(SELECT b.Periodo,b.EntidadLegal_ID,b.Planta_ID,b.SubEnsamble_ID_Ori,b.SubEnsamble_ID,b.CodigoSubInv,b.Ingrediente_ID
FROM cp_dwh.T_FORM_STG_SUBENSFORM_CP b,
(SELECT d.Periodo,d.EntidadLegal_ID,d.Planta_ID,d.SubEnsamble_ID_ori
FROM cp_dwh.T_FORM_STG_SUBENSFORM_CP d WHERE UPPER(CodigoSubInv) NOT LIKE '%SUB%') c
WHERE UPPER(b.CodigoSubInv) NOT LIKE '%SUB%'
AND b.Periodo=c.Periodo
AND b.EntidadLegal_ID=c.EntidadLegal_ID
AND b.Planta_ID=c.Planta_ID
AND b.Ingrediente_ID=c.SubEnsamble_ID_ori
) sub
WHERE a.Periodo=sub.Periodo
AND a.Entidadlegal_id=sub.entidadlegal_id
AND a.Planta_ID=sub.Planta_ID
AND a.SubEnsamble_ID_Ori=sub.SubEnsamble_ID_Ori
AND a.SubEnsamble_ID=sub.SubEnsamble_ID
AND a.ingrediente_id=sub.ingrediente_id
AND UPPER(a.CodigoSubInv) NOT LIKE 'PT'
) e 
WHERE C.Periodo=E.Periodo
AND C.Entidadlegal_id=E.entidadlegal_id
AND C.Planta_ID=E.Planta_ID
AND C.SubEnsamble_ID_Ori=E.SubEnsamble_ID_Ori
AND C.SubEnsamble_ID=E.SubEnsamble_ID
AND C.ingrediente_id=E.ingrediente_id 
) sec 
on tmp.Periodo=sec.Periodo 
and tmp.EntidadLegal_ID=sec.EntidadLegal_ID
and tmp.Planta_ID=sec.Planta_ID 
and tmp.SubEnsamble_ID_Ori=sec.SubEnsamble_ID_Ori 
and tmp.SubEnsamble_ID=sec.SubEnsamble_ID
and tmp.Ingrediente_ID=sec.Ingrediente_ID;


--SET Paso = 2;
-- Esta tabla es el espejo de la tabla que tiene el CP del proveedor MARSYSTEMS. 
-- Se hizo así para poder comparar set de resultados entre una versión y otra

TRUNCATE TABLE cp_dwh.T_F_FORMULAS;

-- Insertamos lo calculado en la recursividad y las excepciones encontradas
-- SET Paso = 3;

INSERT INTO cp_dwh.T_F_FORMULAS partition(entidadlegal_id)
SELECT A.Periodo
,A.Planta_ID
,A.SubEnsamble_ID_Ori
,A.Ingrediente_ID
,SUM(A.fact)
,NULL
,NULL
,NULL
,FROM_UNIXTIME(UNIX_TIMESTAMP())
,A.EntidadLegal_ID
FROM cp_view.V_FORM_SUBFORMULAS_CP A, 
cp_dwh.mtl_catalogo_materiales B
WHERE B.ORGANIZATION_ID = a.Planta_ID  
AND a.SubEnsamble_ID_Ori = B.INVENTORY_ITEM_ID  
AND B.ITEM_TYPE='PT'
GROUP BY A.Periodo,A.EntidadLegal_ID,A.Planta_ID,A.SubEnsamble_ID_Ori,A.Ingrediente_ID;


--SET Paso = 4
--Se actualiza costo real
INSERT OVERWRITE TABLE cp_dwh.T_F_FORMULAS partition(EntidadLegal_ID)
select tmp.periodo,tmp.planta_id,tmp.producto_id,tmp.ingrediente_id,tmp.cantidad,COALESCE(sec.costo,tmp.CostoReal),
tmp.CostoEstandar,tmp.TMoneda_ID,FROM_UNIXTIME(UNIX_TIMESTAMP()),tmp.entidadlegal_id
from cp_dwh.T_F_FORMULAS tmp 
left OUTER join (
SELECT a.Periodo,a.Planta_ID,a.Producto_ID,a.Ingrediente_ID,a.Cantidad,tbl_comp.costo,
a.CostoEstandar,a.TMoneda_ID,a.storeday,a.EntidadLegal_ID
from cp_dwh.T_F_FORMULAS a
join (SELECT regexp_replace(SUBSTRING(Fecha,1,7),"/","-") AS Periodo, 
EntidadLegal_ID,MF_Organizacion_ID,MF_Producto_ID,
SUM(Cantidad*Costo)/SUM(Cantidad) AS Costo
FROM cp_dwh_mf.MF_COMPRAS
WHERE regexp_replace(Fecha,"/","-")='${hiveconf:V_PRIMER_DIA}' ---=========================sPeriodo ===========> PARAMETRO
AND Cantidad<>0 AND (Cantidad NOT BETWEEN -1 AND 1)
GROUP BY Fecha,EntidadLegal_ID, MF_Organizacion_ID,MF_Producto_ID
HAVING SUM(Cantidad)<>0) tbl_comp
where a.Periodo = tbl_comp.Periodo
AND a.Planta_ID = tbl_comp.MF_Organizacion_ID
AND a.Ingrediente_ID = tbl_comp.MF_Producto_ID
AND a.EntidadLegal_ID = tbl_comp.EntidadLegal_ID) sec
on tmp.Periodo=sec.Periodo
and tmp.Planta_ID = sec.Planta_ID
and tmp.Producto_ID = sec.Producto_ID
and tmp.Ingrediente_ID = sec.Ingrediente_ID
and tmp.EntidadLegal_ID = sec.EntidadLegal_ID;


-- Actualizamos el costo estandar de los productos, se toma en cuenta el costo estandar de las transferecias
--SET Paso = 5;
INSERT OVERWRITE table cp_dwh.T_F_FORMULAS partition(EntidadLegal_ID) 
select tmp.periodo,tmp.planta_id,tmp.producto_id,tmp.ingrediente_id,tmp.cantidad,tmp.CostoReal,
COALESCE(sec.Precio,0),tmp.TMoneda_ID,FROM_UNIXTIME(UNIX_TIMESTAMP()),tmp.entidadlegal_id 
from cp_dwh.T_F_FORMULAS tmp 
left outer join (
select 
a.Periodo,a.Planta_ID,a.Producto_ID,a.Ingrediente_ID,a.Cantidad,a.CostoReal,
tbl_upd.Precio as precio,a.TMoneda_ID,a.storeday,a.EntidadLegal_ID from cp_dwh.T_F_FORMULAS a 
JOIN (
SELECT DISTINCT A.Periodo,
A.EntidadLegal_ID,  
A.Planta_ID,
A.Producto_ID,  
A.Ingrediente_ID,
C.Precio
FROM
(SELECT A.EntidadLegal_ID,A.Planta_ID,A.Producto_ID,A.Ingrediente_ID,A.Periodo
FROM cp_dwh.T_F_FORMULAS A
LEFT OUTER JOIN cp_dwh_mf.mf_transferencias B 
ON A.EntidadLegal_ID=B.EntidadLegal_ID 
AND A.Periodo=SUBSTRING(B.Fecha,1,7) --- para las pruebas de datos de oct estaba comentado
AND A.Planta_ID=B.Recibe_MF_Organizacion_ID
AND A.Ingrediente_ID=B.MF_Producto_ID
WHERE b.entidadlegal_id IS NULL
) A
FULL JOIN 
(
SELECT a.Periodo,
a.EntidadLegal_ID,
a.MF_Organizacion_ID,
a.Planta_ID,
a.MF_Producto_ID,
SUM(a.Costo) Precio
FROM cp_dwh_mf.MF_Costo_Prod a
WHERE a.EntidadLegal_ID IN (SELECT EntidadLegal_ID 
FROM cp_view.V_ENTIDADESLEGALES_ACTIVAS_FOR WHERE TRIM(Aplicacion)='FORMULAS' GROUP BY EntidadLegal_ID)
AND a.Tipo_Costo_ID = 1 -- Costo estandard de Materiales
AND a.Periodo='${hiveconf:V_PERIODO}'
GROUP BY a.Periodo,a.EntidadLegal_ID,a.MF_Organizacion_ID,a.Planta_ID,a.MF_Producto_ID
) C
ON A.Planta_ID=C.MF_Organizacion_ID
AND A.Ingrediente_ID=C.MF_Producto_ID
WHERE (A.Planta_ID IS NOT NULL)
AND A.periodo = C.Periodo

UNION ALL

SELECT A.Periodo,  
A.EntidadLegal_ID,  
A.Planta_ID,
A.Producto_ID,  
A.Ingrediente_ID,
MAX(C.Precio) Precio
FROM  
(
SELECT A.EntidadLegal_ID,A.Planta_ID,A.Producto_ID,A.Ingrediente_ID,A.Periodo,B.MF_Organizacion_ID
FROM cp_dwh.T_F_FORMULAS A
INNER JOIN cp_dwh_mf.mf_transferencias B 
ON A.EntidadLegal_ID=B.EntidadLegal_ID
AND A.Planta_ID = B.Recibe_MF_Organizacion_ID
AND A.Ingrediente_ID = B.MF_Producto_ID
WHERE A.Periodo=SUBSTRING(B.Fecha,1,7) 
) A
INNER JOIN 
(
SELECT A.Periodo,A.EntidadLegal_ID,A.MF_Organizacion_ID,A.Planta_ID,A.MF_Producto_ID,SUM(A.Costo) AS Precio
FROM  cp_dwh_mf.MF_Costo_Prod A
WHERE A.Tipo_Costo_ID IN (1,2,3,4) -- Costo estandard de Materiales
AND A.Periodo='${hiveconf:V_PERIODO}'
AND a.Entidadlegal_id IN (SELECT EntidadLegal_ID 
FROM cp_view.V_ENTIDADESLEGALES_ACTIVAS_FOR 
WHERE TRIM(Aplicacion) = 'FORMULAS' GROUP BY EntidadLegal_ID)  
GROUP BY Periodo, EntidadLegal_ID,  MF_Organizacion_ID, Planta_ID, MF_Producto_ID
) C
ON A.MF_Organizacion_ID=C.MF_Organizacion_ID
AND A.Ingrediente_ID=C.MF_Producto_ID 
WHERE A.Planta_ID IS NOT NULL
AND a.periodo = C.Periodo 
GROUP BY A.Periodo,A.EntidadLegal_ID,A.Planta_ID,A.Producto_ID,A.Ingrediente_ID
) tbl_upd
ON a.EntidadLegal_ID = tbl_upd.EntidadLegal_ID
AND a.Periodo = tbl_upd.Periodo 
AND a.Planta_ID  = tbl_upd.Planta_ID
AND a.Producto_ID = tbl_upd.Producto_ID
AND a.Ingrediente_ID = tbl_upd.Ingrediente_ID
) sec
on tmp.Periodo=sec.Periodo
and tmp.Planta_ID = sec.Planta_ID
and tmp.Producto_ID = sec.Producto_ID
and tmp.Ingrediente_ID = sec.Ingrediente_ID
and tmp.EntidadLegal_ID = sec.EntidadLegal_ID;


--Si no existe el costo Real se aplica lo que tenemos en el costo estandar
--SET Paso = 6;
INSERT OVERWRITE table cp_dwh.T_F_FORMULAS partition(EntidadLegal_ID)
SELECT tmp.periodo,tmp.planta_id,tmp.producto_id,tmp.ingrediente_id,tmp.cantidad,COALESCE(tmp.costoReal,sec.costoReal),
tmp.CostoEstandar,tmp.TMoneda_ID,FROM_UNIXTIME(UNIX_TIMESTAMP()),tmp.entidadlegal_id
from cp_dwh.T_F_FORMULAS tmp 
left outer join ( SELECT tmp.Periodo,
tmp.Planta_ID,tmp.Producto_ID,tmp.Ingrediente_ID,tmp.Cantidad,SEC0.CostoEstandar as costoReal,tmp.CostoEstandar
,tmp.TMoneda_ID,tmp.storeday,tmp.EntidadLegal_ID
FROM cp_dwh.T_F_FORMULAS tmp join (select Periodo,EntidadLegal_ID,
Planta_ID,Producto_ID,Ingrediente_ID,Cantidad,CostoReal,CostoEstandar,TMoneda_ID
FROM cp_dwh.T_F_FORMULAS) sec0 
on tmp.Periodo=sec0.Periodo
AND tmp.Planta_ID=sec0.Planta_ID
AND tmp.Ingrediente_ID=sec0.Ingrediente_ID
AND tmp.Producto_ID=sec0.Producto_ID
AND tmp.EntidadLegal_ID=sec0.EntidadLegal_ID
WHERE sec0.periodo = '${hiveconf:V_PERIODO}'
AND sec0.CostoReal IS NULL 
AND sec0.costoestandar IS NOT NULL) sec
on tmp.Periodo=sec.Periodo
and tmp.Planta_ID = sec.Planta_ID
and tmp.Producto_ID = sec.Producto_ID
and tmp.Ingrediente_ID = sec.Ingrediente_ID
and tmp.EntidadLegal_ID = sec.EntidadLegal_ID;


--SET Paso = 7;
--Si el costo real es nulo o costo estandar es nulo se establecen con valor de 0
INSERT OVERWRITE table cp_dwh.T_F_FORMULAS partition(EntidadLegal_ID)
SELECT tmp.periodo,tmp.planta_id,tmp.producto_id,tmp.ingrediente_id,tmp.cantidad,COALESCE(tmp.costoReal,sec.costoreal),
COALESCE(tmp.CostoEstandar,sec.costoestandar),tmp.TMoneda_ID,FROM_UNIXTIME(UNIX_TIMESTAMP()),tmp.entidadlegal_id
from cp_dwh.T_F_FORMULAS tmp 
left outer join (
SELECT tmp.Periodo,tmp.EntidadLegal_ID,
tmp.Planta_ID,tmp.Producto_ID,tmp.Ingrediente_ID,tmp.Cantidad,0 as costoreal,0 as costoestandar,tmp.TMoneda_ID,tmp.storeday
FROM cp_dwh.T_F_FORMULAS tmp JOIN (select Periodo,EntidadLegal_ID,
Planta_ID,Producto_ID,Ingrediente_ID,Cantidad,CostoReal,CostoEstandar,TMoneda_ID
FROM cp_dwh.T_F_FORMULAS) sec 
on tmp.Periodo=sec.Periodo
AND tmp.Planta_ID=sec.Planta_ID
AND tmp.Ingrediente_ID=sec.Ingrediente_ID
AND tmp.EntidadLegal_ID=sec.EntidadLegal_ID
WHERE sec.periodo = '${hiveconf:V_PERIODO}' 
AND (tmp.costoestandar is null OR tmp.costoReal is null)) sec 
on tmp.Periodo=sec.Periodo
and tmp.Planta_ID = sec.Planta_ID
and tmp.Producto_ID = sec.Producto_ID
and tmp.Ingrediente_ID = sec.Ingrediente_ID
and tmp.EntidadLegal_ID = sec.EntidadLegal_ID;



INSERT OVERWRITE table cp_dwh.T_F_FORMULAS partition(EntidadLegal_ID)
SELECT tmp.Periodo,tmp.Planta_ID,tmp.Producto_ID,tmp.Ingrediente_ID,tmp.Cantidad,COALESCE(tmp.costoReal,sec.CostoReal),tmp.CostoEstandar
,tmp.TMoneda_ID,FROM_UNIXTIME(UNIX_TIMESTAMP()),tmp.EntidadLegal_ID
FROM cp_dwh.T_F_FORMULAS TMP
LEFT OUTER JOIN
(SELECT tmp.Periodo,tmp.Planta_ID,tmp.Producto_ID,tmp.Ingrediente_ID,tmp.Cantidad,tbl_upd.CostoReal,tmp.CostoEstandar
,tmp.TMoneda_ID,tmp.storeday,tmp.EntidadLegal_ID
FROM cp_dwh.T_F_FORMULAS tmp JOIN (
SELECT  tf.Periodo, tf.EntidadLegal_ID, tf.Planta_ID, MAX(tf.Producto_ID) Producto_ID, MAX(tf.Ingrediente_ID) Ingrediente_ID, 
0 CostoReal
FROM cp_dwh_mf.mf_transferencias MF_Transferencias_0,cp_dwh.T_F_FORMULAS tf
WHERE MF_Transferencias_0.Recibe_EntidadLegal_ID = tf.EntidadLegal_ID 
AND tf.EntidadLegal_ID IN (SELECT EntidadLegal_ID FROM cp_view.v_entidadeslegales_activas_for
 WHERE TRIM(Aplicacion) = 'FORMULAS' GROUP BY EntidadLegal_ID) 
AND tf.Periodo = '${hiveconf:V_PERIODO}' 
AND MF_Transferencias_0.Recibe_MF_Organizacion_ID = tf.Planta_ID 
AND MF_Transferencias_0.MF_Producto_ID = tf.Ingrediente_ID 
AND tf.Periodo = CAST(MF_Transferencias_0.Fecha AS STRING) 
GROUP BY tf.Periodo,tf.EntidadLegal_ID,tf.Planta_ID
) tbl_upd 
ON tmp.Periodo = tbl_upd.Periodo
AND tmp.EntidadLegal_ID = tbl_upd.EntidadLegal_ID
AND tmp.Planta_ID  = tbl_upd.Planta_ID
AND tmp.Producto_ID = tbl_upd.Producto_ID
AND tmp.Ingrediente_ID = tbl_upd.Ingrediente_ID)sec
on tmp.Periodo=sec.Periodo
AND tmp.Planta_ID=sec.Planta_ID
AND tmp.Producto_ID = sec.Producto_ID
AND tmp.Ingrediente_ID=sec.Ingrediente_ID
AND tmp.EntidadLegal_ID=sec.EntidadLegal_ID
WHERE sec.periodo = '${hiveconf:V_PERIODO}';


--SET Paso = 8;
 --============================================ 
-- Inicia deflactacion
--============================================ 
--Se comienza el analisis que pretende suavizar las series (formulas). En el procedimiento de la ejecucion de las formulas se encuentran factores que estan totalmente desviadas y estas causan problemas en los costos asociados pues se LE imputan mas producto DEL que en realidad ocupo, 
--estos problemas estan mas relacionados con los productos terminados de pan molido, por lo que esta funcion busca suavizar esta desviacion dentro de un rango ya preestablecido
-- Enero 2013. La Deflactacion se podra aplicar a cualquier EL que lo requiera siempre y cuando se de de alta STG_OPERERP_OLA.GX_CONTROL_ENTIDADES_APP .

--SET Paso = 9;
TRUNCATE TABLE cp_dwh.T_FACT_DESV;


--Se analizan los productos molido y tostado, en los cuales buscamos su desviacion porcentual, aqui solo ponemos atencion a los que tengan una desviacion mayor al 10%
--SET Paso = 10;
INSERT INTO cp_dwh.T_FACT_DESV partition(Entidadlegal_id)
SELECT a.Periodo, 
   a.Planta_ID, 
   a.Producto_ID, 
   a.sumcosto - COALESCE(b.sumunos, 0) as CostoCP, 
   c.Costo,
   c.Costo-COALESCE(b.sumunos, 0) as CostoSunos,  
   COALESCE(CAST(CASE WHEN c.Costo/z.totsumcosto -1<0 
    THEN z.totsumcosto /c.Costo-1 ELSE c.Costo/z.totsumcosto-1 
    END * 100 AS DECIMAL(18,5)),0) as Desviacion, 
   (c.Costo-COALESCE(b.sumunos, 0))/a.sumcosto as FactDefla,
   1,
   FROM_UNIXTIME(UNIX_TIMESTAMP()),
  a.EntidadLegal_ID
FROM (SELECT A.Periodo, A.EntidadLegal_ID, A.Planta_ID, A.Producto_ID, SUM(Cantidad* CostoEstandar) as totsumcosto
 FROM cp_dwh.T_F_FORMULAS A 
 --INNER JOIN VDWH.MF_Producto B ON B.MF_Producto_ID=A.Producto_ID
 INNER JOIN cp_dwh_mf.MF_Producto_Organizacion B 
 ON B.MF_Producto_ID=A.Producto_ID 
 AND B.EntidadLegal_ID = A.EntidadLegal_ID 
 AND B.MF_Organizacion_ID = A.Planta_ID 
 INNER JOIN cp_dwh.GX_CONTROL_ENTIDADES_APP c
 ON A.EntidadLegal_ID=C.EntidadLegal_ID
 AND a.Periodo= '${hiveconf:V_PERIODO}' 
 AND TRIM(c.Cadena) = 'MF' 
-- AND C.EntidadLegal_ID IN ('100','101')
 AND c.Aplicacion = 'FORMULAS' 
 AND c.Objeto = 'Deflactacion' 
 AND c.Campo = 'Deflactacion' 
 AND c.Condicion = 'A'
 WHERE ((B.Descripcion LIKE '%MOLIDO%') OR (B.Descripcion LIKE '%TOSTAD%'))
AND C.EntidadLegal_ID IN ('100','101')
GROUP BY A.Periodo, A.EntidadLegal_ID, A.Planta_ID, A.Producto_ID) Z 
LEFT OUTER JOIN
(SELECT Periodo, EntidadLegal_ID, Planta_ID, Producto_ID, SUM(Cantidad* CostoEstandar) sumcosto
 FROM cp_dwh.T_F_FORMULAS 
 WHERE cantidad<>1
 GROUP BY Periodo, EntidadLegal_ID, Planta_ID, Producto_ID) A 
  ON a.Periodo = z.Periodo
  AND a.EntidadLegal_ID = z.EntidadLegal_ID
  AND a.Planta_ID = z.Planta_ID
  AND a.Producto_ID = z.Producto_ID 
LEFT OUTER JOIN 
(SELECT Periodo, EntidadLegal_ID, Planta_ID, Producto_ID, SUM(Cantidad* CostoEstandar) sumunos
FROM cp_dwh.T_F_FORMULAS 
WHERE cantidad=1
GROUP BY Periodo, EntidadLegal_ID, Planta_ID, Producto_ID) B 
ON a.Periodo = b.Periodo
AND a.EntidadLegal_ID = b.EntidadLegal_ID
AND a.Planta_ID = b.Planta_ID
AND a.Producto_ID = b.Producto_ID 
LEFT OUTER JOIN
(SELECT EntidadLegal_ID, MF_Organizacion_ID Planta_ID, MF_Producto_ID Producto_ID  , Periodo, Costo
 FROM cp_dwh_mf.MF_Costo_Prod 
 WHERE (Tipo_Costo_ID=1)
) C 
ON c.Periodo = a.Periodo
AND c.EntidadLegal_ID = a.EntidadLegal_ID
AND c.Planta_ID = a.Planta_ID
AND c.Producto_ID = a.Producto_ID
WHERE COALESCE(CAST(CASE WHEN c.Costo/z.totsumcosto -1<0 THEN z.totsumcosto /c.Costo-1 ELSE c.Costo/z.totsumcosto-1 END * 100 AS DECIMAL(18,5)),0)>10 
AND a.sumcosto<>0
AND a.EntidadLegal_ID IN (SELECT d.EntidadLegal_ID FROM cp_dwh.GX_CONTROL_ENTIDADES_APP d
WHERE TRIM(d.Cadena) = 'MF' AND d.Aplicacion = 'FORMULAS' 
AND d.Objeto = 'Deflactacion' AND d.Campo = 'Deflactacion' AND d.Condicion = 'A' GROUP BY d.EntidadLegal_ID);


--SET Paso = 11;
INSERT OVERWRITE table cp_dwh.T_F_FORMULAS partition(EntidadLegal_ID) select tmp.Periodo,tmp.Planta_ID,tmp.Producto_ID
,tmp.Ingrediente_ID,tmp.Cantidad,tmp.CostoReal,
tmp.CostoEstandar,sec.TMoneda_ID,FROM_UNIXTIME(UNIX_TIMESTAMP()),tmp.EntidadLegal_ID from cp_dwh.T_F_FORMULAS tmp
LEFT OUTER JOIN
(select 
tmp.Periodo,tmp.Planta_ID,tmp.Producto_ID,tmp.Ingrediente_ID,tmp.Cantidad,tmp.CostoReal,
tmp.CostoEstandar,tblupd.TMoneda_ID,tmp.storeday,tmp.EntidadLegal_ID from cp_dwh.T_F_FORMULAS tmp 
JOIN (
SELECT A.Periodo, 
A.EntidadLegal_ID, 
A.Planta_ID, 
A.Producto_ID, 
A.Ingrediente_ID, 
A.Cantidad, 
A.TMoneda_ID,  
A.desvx,
A.Cantidada,
A.cantidadb
FROM (
SELECT A.Periodo, 
A.EntidadLegal_ID, 
A.Planta_ID, 
A.Producto_ID, 
A.Ingrediente_ID, 
0 Cantidad, 
-1 AS TMoneda_ID,  
COALESCE(CASE WHEN a.Cantidada/B.cantidadb-1<0 
THEN B.cantidadb/a.Cantidada-1 
ELSE a.Cantidada/B.cantidadb-1 END,20)* 100 desvx
,a.Cantidada
,B.cantidadb
FROM (SELECT A.Periodo, A.EntidadLegal_ID, A.Planta_ID, A.Producto_ID, A.Ingrediente_ID, Cantidad AS cantidada
FROM cp_dwh.T_F_FORMULAS A 
INNER JOIN cp_dwh.T_FACT_DESV B
ON A.Periodo = B.Periodo 
AND A.EntidadLegal_ID = B.EntidadLegal_ID
AND A.Producto_ID = B.Producto_ID
AND A.Planta_id = B.Planta_id ) A 
LEFT OUTER JOIN (
SELECT a.periodo, a.EntidadLegal_ID,a.Planta_ID, a.producto_id, 
a.ingrediente_id, COALESCE(STDDEV_SAMP(a.cantidad)/4,0) + AVG( a.cantidad) as cantidadb,AVG( a.cantidad) promedio
FROM 
(SELECT X.periodo,X.EntidadLegal_ID,X.Planta_ID,X.producto_id,X.ingrediente_id,X.cantidad,X.desv
FROM 
(SELECT a.periodo, a.EntidadLegal_ID, 
b.Planta_ID, a.producto_id, 
a.ingrediente_id, a.cantidad, 
CASE WHEN (a.costo/b.costo-1)<0  THEN (b.costo/a.costo-1)  ELSE (a.costo/b.costo-1) END*100 AS Desv,
a.costo/b.costo
FROM (
SELECT a.periodo, a.EntidadLegal_ID, 
a.Planta_ID, a.producto_id, 
a.ingrediente_id, a.cantidad cantidad, 
b.costo
FROM cp_dwh.T_F_FORMULAS A 
INNER JOIN cp_dwh_mf.MF_Costo_Prod B
ON A.Periodo = B.Periodo 
AND A.EntidadLegal_ID = B.EntidadLegal_ID 
AND A.Producto_ID = B.mf_Producto_ID 
AND A.planta_id = B.mf_organizacion_id
WHERE (b.Tipo_Costo_ID=1)
AND A.periodo = '${hiveconf:V_PERIODO}'
AND B.entidadlegal_id IN (SELECT d.EntidadLegal_ID FROM cp_dwh.GX_CONTROL_ENTIDADES_APP d WHERE TRIM(d.Cadena)='MF' AND d.Aplicacion='FORMULAS'
AND d.Objeto='Deflactacion' AND d.Campo='Deflactacion' AND d.Condicion='A' GROUP BY d.EntidadLegal_ID)
) A FULL OUTER JOIN cp_dwh.T_FACT_DESV B 
ON A.Periodo=B.Periodo 
AND A.EntidadLegal_ID=B.EntidadLegal_ID 
AND A.Producto_ID=B.Producto_ID
WHERE a.costo/b.costo<>1
) X 
WHERE X.DESV < 10
) A
GROUP BY a.periodo,a.EntidadLegal_ID,a.Planta_ID, a.producto_id,a.ingrediente_id
) B 
ON A.Periodo = B.Periodo 
AND A.EntidadLegal_ID = B.EntidadLegal_ID 
AND A.Producto_ID = B.Producto_ID 
AND A.Planta_ID = B.Planta_ID 
AND A.ingrediente_id = B.ingrediente_id 
) A
WHERE A.desvx>10 
AND A.Cantidada <>1 
AND A.cantidadb <> 0 
AND A.cantidada <> 0
AND A.EntidadLegal_ID IN (SELECT d.EntidadLegal_ID FROM cp_dwh.GX_CONTROL_ENTIDADES_APP AS d WHERE TRIM(d.Cadena) = 'MF' AND d.Aplicacion = 'FORMULAS' AND d.Objeto = 'Deflactacion' AND d.Campo='Deflactacion' AND d.Condicion = 'A' GROUP BY d.EntidadLegal_ID)
) tblupd
WHERE tblupd.Periodo=tmp.Periodo
AND tblupd.EntidadLegal_ID=tmp.EntidadLegal_ID
AND tblupd.Planta_ID=tmp.Planta_ID
AND tblupd.Ingrediente_ID=tmp.Ingrediente_ID
AND tblupd.Producto_ID=tmp.Producto_ID
AND tblupd.EntidadLegal_ID IN (SELECT d.EntidadLegal_ID FROM cp_dwh.GX_CONTROL_ENTIDADES_APP d WHERE TRIM(d.Cadena) = 'MF' 
AND d.Aplicacion = 'FORMULAS' AND d.Objeto = 'Deflactacion' 
AND d.Campo = 'Deflactacion' AND d.Condicion = 'A' GROUP BY d.EntidadLegal_ID)
) sec
on tmp.Periodo=sec.Periodo
and tmp.Planta_ID = sec.Planta_ID
and tmp.Producto_ID = sec.Producto_ID
and tmp.Ingrediente_ID = sec.Ingrediente_ID
and tmp.EntidadLegal_ID = sec.EntidadLegal_ID;


--Tenemos ya los productos que estan sujetos a deflactar, entonces para estos productos 
--exclusivamente se analizan por separado y se les calcula el factor que se debera de 
--utilizar para suavizar sus factores dentro de su formula
--SET Paso = 12;
INSERT INTO cp_dwh.T_FACT_DESV partition(Entidadlegal_id)
SELECT a.Periodo, 
 a.Planta_ID, 
 a.Producto_ID, 
 a.sumcosto - COALESCE(b.sumunos, 0) CostoCP, 
 c.Costo,
 c.Costo-COALESCE(b.sumunos,0) CostoSunos,      
 c.Costo - (z.totsumcosto - a.sumcosto) as MtoDeflact,
 ( c.Costo - (z.totsumcosto - a.sumcosto) )/a.sumcosto FactDefla,
 2,
 FROM_UNIXTIME(UNIX_TIMESTAMP()),
 a.EntidadLegal_ID
FROM  cp_dwh.T_FACT_DESV Y 
INNER JOIN 
(SELECT Periodo, EntidadLegal_ID, Planta_ID, Producto_ID, SUM(Cantidad*  CostoEstandar) totsumcosto
FROM cp_dwh.T_F_FORMULAS 
GROUP BY  Periodo, EntidadLegal_ID, Planta_ID, Producto_ID) Z 
ON y.Periodo = z.Periodo
 AND y.EntidadLegal_ID = z.EntidadLegal_ID
 AND y.Planta_ID = z.Planta_ID
 AND y.Producto_ID = z.Producto_ID  
LEFT OUTER JOIN (SELECT Periodo, EntidadLegal_ID, Planta_ID, Producto_ID, SUM(Cantidad*  CostoEstandar) sumcosto
         FROM cp_dwh.T_F_FORMULAS 
         WHERE ( cantidad<>1 
          AND tmoneda_id = -1) 
         GROUP BY Periodo, EntidadLegal_ID, Planta_ID, Producto_ID) A 
                  ON a.Periodo = z.Periodo
                  AND a.EntidadLegal_ID = z.EntidadLegal_ID
                  AND a.Planta_ID = z.Planta_ID
                  AND a.Producto_ID = z.Producto_ID 
LEFT OUTER JOIN (SELECT Periodo, EntidadLegal_ID, Planta_ID, Producto_ID, SUM(Cantidad*  CostoEstandar) sumunos
          FROM cp_dwh.T_F_FORMULAS 
          WHERE cantidad=1
          GROUP BY Periodo, EntidadLegal_ID, Planta_ID, Producto_ID) B 
                    ON a.Periodo = b.Periodo
                    AND a.EntidadLegal_ID = b.EntidadLegal_ID
                    AND a.Planta_ID = b.Planta_ID
                    AND a.Producto_ID = b.Producto_ID 
LEFT OUTER JOIN (SELECT EntidadLegal_ID, MF_Organizacion_ID Planta_ID, MF_Producto_ID Producto_ID,Periodo, Costo
               FROM cp_dwh_mf.MF_Costo_Prod 
               WHERE (Tipo_Costo_ID=1)
              ) C ON c.Periodo = a.Periodo
                AND c.EntidadLegal_ID = a.EntidadLegal_ID
                AND c.Planta_ID = a.Planta_ID
                AND c.Producto_ID = a.Producto_ID
WHERE a.Periodo IS NOT NULL 
AND a.sumcosto<>0 
AND (( c.Costo - (z.totsumcosto - a.sumcosto)) /a.sumcosto)>0
AND z.EntidadLegal_ID IN (SELECT d.EntidadLegal_ID FROM cp_dwh.GX_CONTROL_ENTIDADES_APP d
WHERE TRIM(d.Cadena)='MF' AND d.Aplicacion='FORMULAS' AND d.Objeto='Deflactacion' 
AND d.Campo='Deflactacion' AND d.Condicion='A' GROUP BY d.EntidadLegal_ID);


--SET Paso = 13;
INSERT OVERWRITE table cp_dwh.T_F_FORMULAS partition(EntidadLegal_ID) SELECT tmp.Periodo,tmp.Planta_ID,tmp.Producto_ID
,tmp.Ingrediente_ID,COALESCE(tmp.cantidad,sec.Cantidad),tmp.CostoReal,
tmp.CostoEstandar,sec.TMoneda_ID,FROM_UNIXTIME(UNIX_TIMESTAMP()),tmp.EntidadLegal_ID 
from cp_dwh.T_F_FORMULAS tmp
LEFT OUTER JOIN (
select 
tmp.Periodo,tmp.Planta_ID,tmp.Producto_ID,tmp.Ingrediente_ID,tblupd.cantidad,tmp.CostoReal,
tmp.CostoEstandar,tmp.TMoneda_ID,tmp.storeday,tmp.EntidadLegal_ID 
from cp_dwh.T_F_FORMULAS tmp 
JOIN ( 
SELECT A.Periodo, A.EntidadLegal_ID, A.Planta_ID, A.Producto_ID, A.Ingrediente_ID,A.Cantidad* B.FactDefla Cantidad
FROM cp_dwh.T_F_FORMULAS A 
 INNER JOIN cp_dwh.T_FACT_DESV B
    ON A.Periodo = B.Periodo
    AND A.EntidadLegal_ID = B.EntidadLegal_ID
    AND A.Producto_ID = B.Producto_ID
    AND A.Planta_ID = B.Planta_ID
WHERE A.Cantidad<>1 
AND B.paso=2 
AND a.tmoneda_id = -1
AND A.EntidadLegal_ID IN (SELECT EntidadLegal_ID 
    FROM cp_dwh.GX_CONTROL_ENTIDADES_APP 
    WHERE TRIM(Cadena) = 'MF' AND Aplicacion = 'FORMULAS' 
    AND Objeto = 'Deflactacion' AND Campo = 'Deflactacion' 
    AND Condicion = 'A' GROUP BY EntidadLegal_ID)
 ) tblupd
WHERE tblupd.Periodo = tmp.Periodo
AND tblupd.EntidadLegal_ID = tmp.EntidadLegal_ID
AND tblupd.Planta_ID = tmp.Planta_ID
AND tblupd.Ingrediente_ID = tmp.Ingrediente_ID
AND tblupd.Producto_ID = tmp.Producto_ID
AND tblupd.EntidadLegal_ID IN (SELECT EntidadLegal_ID 
  FROM cp_dwh.GX_CONTROL_ENTIDADES_APP d
  WHERE TRIM(d.Cadena) = 'MF' AND d.Aplicacion = 'FORMULAS' 
  AND d.Objeto = 'Deflactacion' AND d.Campo = 'Deflactacion' 
  AND d.Condicion = 'A' GROUP BY d.EntidadLegal_ID)
) sec
on tmp.Periodo=sec.Periodo
and tmp.Planta_ID = sec.Planta_ID
and tmp.Producto_ID = sec.Producto_ID
and tmp.Ingrediente_ID = sec.Ingrediente_ID
and tmp.EntidadLegal_ID = sec.EntidadLegal_ID;


--SET Paso = 14;
INSERT INTO cp_dwh.T_FACT_DESV partition(Entidadlegal_id)
SELECT 
B.Periodo,
B.Planta_ID, 
B.Producto_ID, 
B.CostoCP, 
B.Costo,
B.CostoSunos,
B.Desviacion,
B.FactDefla,
B.CTE,
FROM_UNIXTIME(UNIX_TIMESTAMP()),
B.EntidadLegal_ID
FROM (
SELECT a.Periodo,
a.Planta_ID, 
a.Producto_ID, 
a.sumcosto - COALESCE(b.sumunos, 0) CostoCP, 
c.Costo,
c.Costo-COALESCE(b.sumunos, 0) CostoSunos,  
COALESCE(CAST(CASE WHEN (c.Costo/z.totsumcosto) -1<0 THEN (z.totsumcosto/c.Costo)-1 ELSE (c.Costo/z.totsumcosto)-1 END * 100 AS DECIMAL(18,5)),0) Desviacion, 
(c.Costo-COALESCE(b.sumunos, 0))/a.sumcosto FactDefla,
4 AS CTE,
a.EntidadLegal_ID
FROM (SELECT a.Periodo, a.EntidadLegal_ID, a.Planta_ID, a.Producto_ID, SUM(a.Cantidad*  a.CostoEstandar) totsumcosto
FROM cp_dwh.T_F_FORMULAS a 
WHERE a.Periodo= '${hiveconf:V_PERIODO}'
AND a.EntidadLegal_ID IN (SELECT d.EntidadLegal_ID FROM cp_dwh.GX_CONTROL_ENTIDADES_APP d WHERE TRIM(d.Cadena) = 'MF' 
AND d.Aplicacion = 'FORMULAS' AND d.Objeto = 'Deflactacion' AND d.Campo = 'Deflactacion' 
AND d.Condicion = 'A' GROUP BY d.EntidadLegal_ID) 
GROUP BY a.Periodo, a.EntidadLegal_ID, a.Planta_ID, a.Producto_ID) Z 
LEFT OUTER JOIN
(SELECT a.Periodo, a.EntidadLegal_ID, a.Planta_ID, a.Producto_ID, SUM(a.Cantidad*  a.CostoEstandar) sumcosto
FROM cp_dwh.T_F_FORMULAS a
WHERE a.cantidad<>1
GROUP BY a.Periodo, a.EntidadLegal_ID, a.Planta_ID, a.Producto_ID) A 
ON a.Periodo = z.Periodo
AND a.EntidadLegal_ID = z.EntidadLegal_ID
AND a.Planta_ID = z.Planta_ID
AND a.Producto_ID = z.Producto_ID 
LEFT OUTER JOIN 
(SELECT a.Periodo, a.EntidadLegal_ID, a.Planta_ID, a.Producto_ID, SUM(a.Cantidad*  a.CostoEstandar) sumunos
FROM cp_dwh.T_F_FORMULAS a
WHERE a.cantidad=1
GROUP BY a.Periodo, a.EntidadLegal_ID, a.Planta_ID, a.Producto_ID) B 
ON a.Periodo = b.Periodo
AND a.EntidadLegal_ID = b.EntidadLegal_ID
AND a.Planta_ID = b.Planta_ID
AND a.Producto_ID = b.Producto_ID 
LEFT OUTER JOIN
(SELECT p.EntidadLegal_ID, p.MF_Organizacion_ID as Planta_ID, p.MF_Producto_ID as Producto_ID, p.Periodo, p.Costo
FROM cp_dwh_mf.MF_Costo_Prod p
WHERE (p.Tipo_Costo_ID=1)) C 
ON c.EntidadLegal_ID = a.EntidadLegal_ID
AND c.Periodo = a.Periodo
AND c.Planta_ID = a.Planta_ID
AND c.Producto_ID = a.Producto_ID
WHERE a.sumcosto<>0 
AND a.sumcosto<>0 
AND (c.Costo-COALESCE(b.sumunos, 0))/a.sumcosto>0
AND a.EntidadLegal_ID IN (SELECT d.EntidadLegal_ID FROM cp_dwh.GX_CONTROL_ENTIDADES_APP d WHERE TRIM(d.Cadena) = 'MF' 
AND d.Aplicacion = 'FORMULAS' AND d.Objeto = 'Deflactacion' 
AND d.Campo = 'Deflactacion' AND d.Condicion = 'A' GROUP BY d.EntidadLegal_ID) 
) B
WHERE B.Desviacion>30;


-- SET Paso = 15;
INSERT OVERWRITE TABLE cp_dwh.T_F_FORMULAS partition(Entidadlegal_id) 
SELECT tmp.Periodo,tmp.Planta_ID,tmp.Producto_ID,tmp.Ingrediente_ID,COALESCE(sec.Cantidad,tmp.cantidad),tmp.CostoReal,
tmp.CostoEstandar,COALESCE(sec.TMoneda_ID,tmp.tmoneda_id),FROM_UNIXTIME(UNIX_TIMESTAMP()),tmp.EntidadLegal_ID
from cp_dwh.T_F_FORMULAS tmp
left outer join(
select tmp.Periodo,tmp.Planta_ID,tmp.Producto_ID,tmp.Ingrediente_ID,tblupd.Cantidad,tmp.CostoReal,
tmp.CostoEstandar,tblupd.TMoneda_ID,tmp.storeday,tmp.EntidadLegal_ID from cp_dwh.T_F_FORMULAS tmp 
JOIN ( 
SELECT A.Periodo,
A.EntidadLegal_ID,
A.Planta_ID,
A.Producto_ID,
A.Ingrediente_ID,
A.Cantidad* B.FactDefla as Cantidad, 
-1 AS tmoneda_id
FROM cp_dwh.T_F_FORMULAS A 
INNER JOIN cp_dwh.T_FACT_DESV B
ON A.Periodo = B.Periodo
AND A.EntidadLegal_ID = B.EntidadLegal_ID
AND A.Producto_ID = B.Producto_ID
AND A.Planta_ID = B.Planta_ID
WHERE A.Cantidad<>1 
AND B.paso=4
) tblupd
WHERE tblupd.Periodo=tmp.Periodo
AND tblupd.EntidadLegal_ID=tmp.EntidadLegal_ID
AND tblupd.Planta_ID=tmp.Planta_ID
AND tblupd.Ingrediente_ID=tmp.Ingrediente_ID
AND tblupd.Producto_ID=tmp.Producto_ID
AND tblupd.EntidadLegal_ID IN (SELECT d.EntidadLegal_ID FROM cp_dwh.GX_CONTROL_ENTIDADES_APP d
WHERE TRIM(d.Cadena) = 'MF' AND d.Aplicacion = 'FORMULAS' AND d.Objeto = 'Deflactacion' 
AND d.Campo = 'Deflactacion' AND d.Condicion = 'A' GROUP BY d.EntidadLegal_ID)
) sec
on tmp.Periodo=sec.Periodo
and tmp.Planta_ID = sec.Planta_ID
and tmp.Producto_ID = sec.Producto_ID
and tmp.Ingrediente_ID = sec.Ingrediente_ID
and tmp.EntidadLegal_ID = sec.EntidadLegal_ID;

WITH DATES1 AS ( select rownum AS TEST1,DATES from (
SELECT (ROW_NUMBER()OVER(ORDER BY MAX_DT desc )) AS rownum,MAX_DT AS DATES FROM
(SELECT T1.Current_Day,MAX(T1.Current_Day) MAX_DT FROM db_reporting.oc_reporting T1
WHERE T1.Current_Day >= TRUNC(SYSDATE)
GROUP BY T1.Current_Day
ORDER BY T1.Current_day DESC)
)
where rownum<3)select



(SELECT TRUNC(T.current_day) AS R_DATE,
T.current_day,
'ECOMM' AS ECOMM,
(CASE WHEN T.NODE_TYPE IN ('DC','LFC') THEN T.SHIP_NODE
WHEN T.FULFILLMENT_TYPE IN ('SFS','SDD') THEN 'SFS'
WHEN T.FULFILLMENT_TYPE = 'BPS' THEN 'BPS'
WHEN T.NODE_TYPE IS NULL THEN 'SYSTEMS/OTHER' ELSE T.NODE_TYPE END) AS SHIP_NODE,
COALESCE(TRIM(T.NODE_TYPE),'SYSTEMS/OTHER') AS NODE_TYPE,
TRIM(CASE WHEN fulfillment_type IN ('SFS','SDD') THEN 'SFS'
WHEN T.SHIP_NODE = '873' THEN 'EFC1'
WHEN T.SHIP_NODE = '809' THEN 'EFC2'
WHEN T.SHIP_NODE = '819' THEN 'EFC3'
WHEN T.SHIP_NODE = '829' THEN 'EFC4'
WHEN T.NODE_TYPE IN ('LFC','RDC','DSV') THEN T.NODE_TYPE ELSE fulfillment_type END) AS fulfillment_type,
TRIM(CASE WHEN T.NODE_TYPE IN ('DC','RDC','LFC') THEN 'ERL'
WHEN T.NODE_TYPE IS NULL THEN 'SYSTEMS/OTHER' ELSE T.NODE_TYPE END) AS ERL,
SUM(CASE WHEN T.current_day = (SELECT DATES FROM DATES1 WHERE TEST1 = 1) THEN T.NW_DMND_UNT_CNT ELSE 0 END) AS LH_NW_DMND_UNT,
SUM(CASE WHEN T.current_day = (SELECT DATES FROM DATES1 WHERE TEST1 = 1) THEN T.CNCLD_UNT_CNT ELSE 0 END) AS LH_CNCLD_UNT_CNT,
SUM(CASE WHEN T.current_day = (SELECT DATES FROM DATES1 WHERE TEST1 = 1) THEN T.PRCD_UNT_CNT ELSE 0 END) AS LH_PRCD_UNT_CNT,
SUM(CASE WHEN T.current_day = (SELECT DATES FROM DATES1 WHERE TEST1 = 1) THEN T.RT_BCKLG_UNT_CNT ELSE 0 END) AS LH_RT_BCKLG_UNT_CNT,
--SUM(CASE WHEN T.current_day = (SELECT DATES FROM DATES1 WHERE TEST1 = 1) THEN T.NW_DMND_ORD_CNT ELSE 0 END) AS LH_NW_DMND_ORD_UNT,
--SUM(CASE WHEN T.current_day = (SELECT DATES FROM DATES1 WHERE TEST1 = 1) THEN T.CNCLD_ORD_CNT ELSE 0 END) AS LH_CNCLD_ORD_CNT,
SUM(CASE WHEN T.current_day = (SELECT DATES FROM DATES1 WHERE TEST1 = 1) THEN T.PRCD_ORD_CNT ELSE 0 END) AS LH_PRCD_ORD_CNT,
--SUM(CASE WHEN T.DATES = (SELECT DATES FROM DATES1 WHERE TEST1 = 1) THEN T.RT_BCKLG_ORD_CNT ELSE 0 END) AS LH_RT_BCKLG_ORD_CNT,
SUM(CASE WHEN T.current_day = (SELECT DATES FROM DATES1 WHERE TEST1 = 1) THEN T.NW_DMND_SLS_AMT ELSE 0 END) AS LH_NW_DMND_SLS_AMT,
SUM(CASE WHEN T.current_day = (SELECT DATES FROM DATES1 WHERE TEST1 = 1) THEN T.CNCLD_SLS_AMT ELSE 0 END) AS LH_CNCLD_SLS_AMT,
SUM(CASE WHEN T.current_day = (SELECT DATES FROM DATES1 WHERE TEST1 = 1) THEN T.PRCD_SLS_AMT ELSE 0 END) AS LH_PRCD_SLS_AMT,
SUM(CASE WHEN T.current_day = (SELECT DATES FROM DATES1 WHERE TEST1 = 1) THEN T.RT_BCKLG_SLS_AMT ELSE 0 END) AS LH_RT_BCKLG_SLS_AMT,
SUM(CASE WHEN T.current_day = (SELECT DATES FROM DATES1 WHERE TEST1 = 2) THEN T.NW_DMND_UNT_CNT ELSE 0 END) AS L2H_NW_DMND_UNT,
SUM(CASE WHEN T.current_day = (SELECT DATES FROM DATES1 WHERE TEST1 = 2) THEN T.CNCLD_UNT_CNT ELSE 0 END) AS L2H_CNCLD_UNT_CNT,
--SUM(T.RT_BCKLG_ORD_CNT) AS RT_BCKLG_ORD_CNT,
SUM(T.RT_BCKLG_RELS_CNT) AS RT_BCKLG_RELS_CNT,
SUM(T.RT_BCKLG_UNT_CNT) AS RT_BCKLG_UNT_CNT,
SUM(T.RT_BCKLG_SLS_AMT) AS RT_BCKLG_SLS_AMT,
--SUM(T.RSRC_ORD_CNT) AS RSRC_ORD_CNT,
SUM(T.RSRC_RELS_CNT) AS RSRC_RELS_CNT,
SUM(T.RSRC_UNT_CNT) AS RSRC_UNT_CNT,
SUM(T.RSRC_SLS_AMT) AS RSRC_SLS_AMT,
--SUM(T.CNCLD_ORD_CNT) AS CNCLD_ORD_CNT,
SUM(T.CNCLD_RELS_CNT) AS CNCLD_RELS_CNT,
SUM(T.CNCLD_UNT_CNT) AS CNCLD_UNT_CNT,
SUM(T.CNCLD_SLS_AMT) AS CNCLD_SLS_AMT,
--SUM(T.NW_DMND_ORD_CNT) AS NW_DMND_ORD_CNT,
SUM(T.NW_DMND_UNT_CNT) AS NW_DMND_UNT_CNT,
SUM(T.NW_DMND_SLS_AMT) AS NW_DMND_SLS_AMT,
--SUM(T.RCVD_DMND_ORD_CNT) AS RCVD_DMND_ORD_CNT,
SUM(T.RCVD_DMND_RELS_CNT) AS RCVD_DMND_RELS_CNT,
SUM(T.RCVD_DMND_UNT_CNT) AS RCVD_DMND_UNT_CNT,
SUM(T.RCVD_DMND_SLS_AMT) AS RCVD_DMND_SLS_AMT,
SUM(T.PRCD_ORD_CNT) AS PRCD_ORD_CNT,
SUM(T.PRCD_RELS_CNT) AS PRCD_RELS_CNT,
SUM(T.PRCD_UNT_CNT) AS PRCD_UNT_CNT,
SUM(T.PRCD_SLS_AMT) AS PRCD_SLS_AMT
FROM db_reporting.oc_reporting T
WHERE T.current_day >= (case when extract(hour from sysdate)>0 then (extract(year from sysdate) || extract(month from sysdate) || extract(day from sysdate)) ELSE
(extract(year from sysdate-1) || extract(month from sysdate-1) || extract(day from sysdate-1)) END)
--AND T.DATES < TRUNC(SYSDATE,'HH24')
AND SUBSTRING(T.CURRENT_HOUR,9,10) = '23'
AND T.TIME_GRAIN = 'HOURLY'
AND T.FULFILLMENT_TYPE <> 'ECOMM'
GROUP BY TRUNC(T.current_day),
T.current_day,
(CASE WHEN T.NODE_TYPE IN ('DC','LFC') THEN T.SHIP_NODE
WHEN T.FULFILLMENT_TYPE IN ('SFS','SDD') THEN 'SFS'
WHEN T.FULFILLMENT_TYPE = 'BPS' THEN 'BPS'
WHEN T.NODE_TYPE IS NULL THEN 'SYSTEMS/OTHER' ELSE T.NODE_TYPE END),
COALESCE(TRIM(T.NODE_TYPE),'SYSTEMS/OTHER'),
TRIM(CASE WHEN T.NODE_TYPE IN ('DC','RDC','LFC') THEN 'ERL'
WHEN T.NODE_TYPE IS NULL THEN 'SYSTEMS/OTHER' ELSE T.NODE_TYPE END),
TRIM(CASE WHEN fulfillment_type IN ('SFS','SDD') THEN 'SFS'
WHEN T.SHIP_NODE = '873' THEN 'EFC1'
WHEN T.SHIP_NODE = '809' THEN 'EFC2'
WHEN T.SHIP_NODE = '819' THEN 'EFC3'
WHEN T.SHIP_NODE = '829' THEN 'EFC4'
WHEN T.NODE_TYPE IN ('LFC','RDC','DSV') THEN T.NODE_TYPE ELSE FULFILLMENT_TYPE END)
)
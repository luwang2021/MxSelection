query_da_acnp_train = """
WITH merchant_dates_dedup AS (
    WITH distinct_rcd AS (
        SELECT DISTINCT 
              store_id
            , last_np_live_day
            , activation_date
            , level_verification
        from    
            public.fact_selection_intel_merchant_dates 
    )
    , md_unique AS (
        SELECT 
              store_id
            , count(1)
        FROM 
            distinct_rcd    
        GROUP BY store_id
        HAVING count(1)=1
    ) 
    SELECT 
        md.* 
    FROM 
        md_unique
    LEFT JOIN  
        distinct_rcd md
    ON 
        md_unique.store_id = md.store_id
    WHERE 
        level_verification=2
    ORDER BY 
        store_id
)
-- get all the current partner stores
--DROP TABLE DEACTIVATED_NONPARTNER_STORES_RAW;
-- in the future, when fact_selection_intel_mx_raw is in good quality, should consider to replace FACT_STORE_ACTIVE_STATUS by  fact_selection_intel_mx_raw
, deactivated_nonpartner_stores_raw AS (
    SELECT 
          D.STORE_ID -- this store ID is nimda store ID
        , mx.id match_id
        , D.STORE_NAME AS NAME
        , D.BUSINESS_ID
        , D.SP_ID AS STARTING_POINT_ID
        , D.SUB_ID AS SUBMARKET_ID
    --        COALESCE(md.last_np_live_day, D.MOST_RECENT_DEACTIVATION_TIMESTAMP) AS MOST_RECENT_DEACTIVATION_TIMESTAMP,
        , md.last_np_live_day AS MOST_RECENT_DEACTIVATION_TIMESTAMP
        , md.ACTIVATION_DATE AS activation_date
    FROM 
        PUBLIC.FACT_STORE_ACTIVE_STATUS D 
    LEFT OUTER JOIN 
        merchant_dates_dedup md
    ON 
        D.store_id = md.store_id
    LEFT JOIN 
        PUBLIC.fact_selection_intel_mx_raw mx
    ON 
        d.store_id=mx.nimda_id
    WHERE 1
    --    The merchants in the training data should be live and a partner 7 days ago
        AND D.DATE_STAMP  = dateadd('DAY', -7, CURRENT_DATE())
        AND D.ACTIVE_BOP = 1
        AND D.PARTNER_BOP = 1
        AND match_id IS NOT NULL 
        AND md.activation_date IS NOT NULL 
    --        need to verify if the store has ever been activated 
)
-- add the info on first/last devliery and total # of deliveries
, DEACTIVATED_NONPARTNER_STORES_INFO AS(
    SELECT
          DS.*
        , COALESCE(SUM(
            CASE
            WHEN DD.IS_PARTNER IS NOT DISTINCT FROM false THEN 1
            ELSE 0
            END), 0) AS TOTAL_NONPARTNER_ORDERS_LIFETIME
        --        COALESCE(COUNT(DISTINCT DD.DELIVERY_ID), 0) AS TOTAL_ORDERS_LIFETIME,
        , MIN(to_timestamp_ntz(DD.CREATED_AT)) AS FIRST_LIFETIME_ORDER
        , MAX(CASE
              WHEN DD.IS_PARTNER IS NOT DISTINCT FROM false
              THEN to_timestamp_ntz(DD.CREATED_AT)
              ELSE NULL
              END) AS LAST_NP_ORDER
        , MIN(CASE
              WHEN DD.IS_PARTNER IS NOT DISTINCT FROM true
              THEN to_timestamp_ntz(DD.CREATED_AT)
              ELSE NULL
              END) AS FIRST_PARTNER_ORDER
    FROM
        DEACTIVATED_NONPARTNER_STORES_RAW AS DS
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD
    ON 
        DS.STORE_ID = DD.STORE_ID
        AND DD.IS_FILTERED_CORE IS NOT DISTINCT FROM true
        AND DD.ACTIVE_DATE >= '2014-01-01'::TIMESTAMP
    --          AND DD.ACTIVE_DATE < '2019-02-01'::TIMESTAMP
    GROUP BY 
        1, 2, 3, 4, 5, 6, 7, 8
)
--select count(*) from DEACTIVATED_NONPARTNER_STORES_INFO;

-- get store name, cuisine type, and number of reviews from foursquare. ?
-- And then attach these attribute to the current partner stores
, STORES_MOST_RECENT_DEACTIVATION AS (
    SELECT
          DNP.STORE_ID
        , DNP.MATCH_ID
        , DNP.NAME AS NIMDA_STORE_NAME
        , DNP.BUSINESS_ID
        , DNP.STARTING_POINT_ID
        , DNP.SUBMARKET_ID
        , DNP.FIRST_LIFETIME_ORDER
        , DNP.TOTAL_NONPARTNER_ORDERS_LIFETIME
        , DNP.LAST_NP_ORDER
        , DNP.FIRST_PARTNER_ORDER
        , DNP.ACTIVATION_DATE AS REACTIVATION_TIMESTAMP
        , DNP.MOST_RECENT_DEACTIVATION_TIMESTAMP
--        added
        , EXTRACT(MONTH FROM DNP.ACTIVATION_DATE) AS REACTIVATION_MONTH
        , EXTRACT(YEAR FROM DNP.ACTIVATION_DATE) AS REACTIVATION_YEAR
        , EXTRACT(MONTH FROM DNP.MOST_RECENT_DEACTIVATION_TIMESTAMP) AS DEACTIVATION_MONTH
        , EXTRACT(YEAR FROM DNP.MOST_RECENT_DEACTIVATION_TIMESTAMP) AS DEACTIVATION_YEAR
--        MAX(DD_2.ACTUAL_DELIVERY_TIME) AS MOST_RECENT_DELIVERY_TIMESTAMP
        , datediff('DAY', DNP.MOST_RECENT_DEACTIVATION_TIMESTAMP, DNP.ACTIVATION_DATE) AS NUM_DAYS_DEACTIVE
        , datediff('DAY', DNP.ACTIVATION_DATE, dateadd('DAY', -1, dateadd('DAY', -7, CURRENT_DATE()))) AS NUM_DAYS_SINCE_REACTIVATION
--        stop here
    FROM
        DEACTIVATED_NONPARTNER_STORES_INFO AS DNP
    WHERE  
        NUM_DAYS_SINCE_REACTIVATION > 28
)
--  statistics at store level before deactivation
, STORE_STATS_BEFORE_DEACTIVATION AS (
    SELECT
        MRD.STORE_ID,
        MRD.MATCH_ID,
        MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP,
        mrd.reactivation_timestamp,
        COUNT(DISTINCT DD_0.DELIVERY_ID) AS NUM_ORDERS_BEFORE,
        COUNT(DISTINCT DD_0.ACTIVE_DATE) AS NUM_DAYS_ACTIVE_BEFORE
    FROM
        STORES_MOST_RECENT_DEACTIVATION AS MRD
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_0
          ON MRD.STORE_ID = DD_0.STORE_ID
          AND DD_0.ACTIVE_DATE <= MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP
          AND DD_0.IS_FILTERED_CORE = TRUE
    GROUP BY (1, 2, 3, 4)
)
--information around stores reactivation
, STORE_REACTIVATION AS (
    SELECT
        STORE_ID,
        STARTING_POINT_ID,
        MOST_RECENT_DEACTIVATION_TIMESTAMP,
        REACTIVATION_TIMESTAMP
    FROM
        STORES_MOST_RECENT_DEACTIVATION 
)

--  statics of stores after becoming a partner. Note, none of these variables should be used as features for training. ?
, STORE_STATS_AFTER_REACTIVATION AS (
    SELECT
        SR_0.STORE_ID,
        SR_0.MOST_RECENT_DEACTIVATION_TIMESTAMP,
        SR_0.REACTIVATION_TIMESTAMP,
        SUM(CASE
            WHEN DD_3.ACTIVE_DATE
              BETWEEN dateadd('DAY', 120, DATE_TRUNC('DAY', SR_0.REACTIVATION_TIMESTAMP))
              AND dateadd('DAY', 148, DATE_TRUNC('DAY', SR_0.REACTIVATION_TIMESTAMP))
            THEN DD_3.SUBTOTAL / 100.0
            ELSE NULL
            END) AS TOTAL_SUBTOTAL_FIRST_120_148_DAYS
    FROM
        STORE_REACTIVATION AS SR_0
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_3
    ON 
        SR_0.STORE_ID = DD_3.STORE_ID
        AND DD_3.ACTIVE_DATE >= SR_0.REACTIVATION_TIMESTAMP
        AND DD_3.IS_FILTERED_CORE IS NOT DISTINCT FROM true
    GROUP BY 
        1, 2, 3
)
-- CREATE TABLE chendong.deactivated_activeNP AS 
SELECT
    S_0.STORE_ID,
    S_0.match_id,
    mx.vendor_1_id,
    S_0.NIMDA_STORE_NAME,
    mx.RESTAURANT_NAME AS EXTERNAL_STORE_NAME,
    S_0.STARTING_POINT_ID,
    s_0.BUSINESS_ID,
    SP.SUBMARKET_ID,
    case
      when mx.vendor_1_id IS NOT null  then  'requestable'
      ELSE 'not_requestable' 
    end requestable,
    S_0.MOST_RECENT_DEACTIVATION_TIMESTAMP,
    SR.REACTIVATION_TIMESTAMP,
--    SR.MOST_RECENT_DELIVERY_TIMESTAMP,
    CASE
--      WHEN datediff('DAY', S_0.LAST_NP_ORDER, S_0.FIRST_PARTNER_ORDER) BETWEEN 0 AND 28 THEN 'nonpartner_partner_conversion'
      WHEN datediff('DAY', S_0.MOST_RECENT_DEACTIVATION_TIMESTAMP, S_0.reactivation_timestamp) BETWEEN 0 AND 28 THEN 'active_np'
      WHEN datediff('DAY', S_0.MOST_RECENT_DEACTIVATION_TIMESTAMP, S_0.reactivation_timestamp) > 28 AND
          S_0.TOTAL_NONPARTNER_ORDERS_LIFETIME != 0 THEN 'deactivated_np'
--      WHEN S_0.TOTAL_NONPARTNER_ORDERS_LIFETIME > 0 THEN 'nonpartner_deactivated_partner'
      WHEN S_0.TOTAL_NONPARTNER_ORDERS_LIFETIME = 0 THEN 'net_new'
    END AS MERCHANT_TYPE,
    S_0.REACTIVATION_MONTH,
    S_0.REACTIVATION_YEAR,
    S_0.DEACTIVATION_MONTH,
    S_0.DEACTIVATION_YEAR,
    S_0.NUM_DAYS_DEACTIVE,
    S_0.NUM_DAYS_SINCE_REACTIVATION,
    SAR.TOTAL_SUBTOTAL_FIRST_120_148_DAYS,
    CURRENT_DATE AS CREATE_DATE
FROM
    STORES_MOST_RECENT_DEACTIVATION AS S_0
LEFT OUTER JOIN 
    PUBLIC.fact_selection_intel_mx_raw mx 
ON 
    s_0.store_id=mx.nimda_id
LEFT OUTER JOIN 
    STORE_STATS_BEFORE_DEACTIVATION AS SBD 
ON 
    S_0.STORE_ID = SBD.STORE_ID 
    AND S_0.MOST_RECENT_DEACTIVATION_TIMESTAMP = SBD.MOST_RECENT_DEACTIVATION_TIMESTAMP
LEFT OUTER JOIN 
    STORE_REACTIVATION AS SR 
ON 
    S_0.STORE_ID = SR.STORE_ID 
    AND S_0.MOST_RECENT_DEACTIVATION_TIMESTAMP = SR.MOST_RECENT_DEACTIVATION_TIMESTAMP
LEFT OUTER JOIN 
    STORE_STATS_AFTER_REACTIVATION AS SAR 
ON 
    S_0.STORE_ID = SAR.STORE_ID 
    AND S_0.MOST_RECENT_DEACTIVATION_TIMESTAMP = SAR.MOST_RECENT_DEACTIVATION_TIMESTAMP
LEFT OUTER JOIN 
    geo_intelligence.PUBLIC.MAINDB_STARTING_POINT AS SP 
ON 
    SP.ID = S_0.STARTING_POINT_ID
-- remove fradulent stores   
WHERE 
    (s_0.store_id NOT IN  (
     SELECT store_id FROM  PUBLIC.DIMENSION_STORE WHERE LAST_DEACTIVATION_REASON = 'Fraudulent Store'
    ) or s_0.store_id is null)
    AND NUM_DAYS_ACTIVE_BEFORE > 0
"""
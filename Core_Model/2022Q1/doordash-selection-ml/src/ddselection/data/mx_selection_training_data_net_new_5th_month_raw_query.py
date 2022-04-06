query_net_new_train = """
WITH STORE_FIRST_ORDER AS (
    SELECT
        DD.STORE_ID,
        DD.STORE_STARTING_POINT_ID,
        MIN(DATE_TRUNC('DAY', CONVERT_TIMEZONE('UTC', DD.TIMEZONE, DD.CREATED_AT))) AS FIRST_ORDER_DATE,
        MIN(CASE
            WHEN DD.IS_PARTNER = 1 
            THEN DATE_TRUNC('DAY', CONVERT_TIMEZONE('UTC', DD.TIMEZONE, DD.CREATED_AT))
            ELSE NULL
            END) AS FIRST_PARTNER_ORDER_DATE
    FROM
        PUBLIC.DIMENSION_DELIVERIES AS DD
    WHERE 1 
        AND DD.CREATED_AT > '2014-01-01' :: TIMESTAMP 
        AND DD.IS_FILTERED_CORE = 1
    GROUP BY 
        1, 2
)

--  get the partner stores from Mx db.
, ALL_PARTNER_STORES AS (
    SELECT
          DISTINCT S.STORE_ID
        , MX.ID MATCH_ID
        , COALESCE (MX.RESTAURANT_NAME, S.BUSINESS_NAME) AS RESTAURANT_NAME
        , S.SP_ID AS STARTING_POINT_ID
        , S.SUB_ID AS SUBMARKET_ID
    FROM
        PUBLIC.FACT_STORE_ACTIVE_STATUS S
    LEFT JOIN 
        PUBLIC.FACT_SELECTION_INTEL_MX_RAW MX 
    ON 
        S.STORE_ID=MX.NIMDA_ID
    WHERE 1
        AND S.DATE_STAMP = dateadd('DAY', -7, CURRENT_DATE()) 
        AND S.ACTIVE_BOP = 1
        AND S.PARTNER_BOP = 1
        AND MATCH_ID IS NOT NULL 
)

, merchant_dates_dedup AS (
    WITH distinct_rcd AS (
        SELECT
              DISTINCT store_id
            , last_np_live_day
            , activation_date
            , level_verification
        FROM
            public.fact_selection_intel_merchant_dates 
    )
    , md_unique AS (
        SELECT
            store_id,
            count(1)
        FROM
            distinct_rcd    
        GROUP BY
            store_id
        HAVING
            count(1)=1
    ) 
    SELECT 
        md.* 
    FROM 
        md_unique
    LEFT JOIN  
        distinct_rcd AS md
    ON 
        md_unique.store_id = md.store_id
    WHERE 
        level_verification=2
    ORDER BY 
        store_id
)

, STORE_PROXY_ACTIVATION AS (
    SELECT
        DS.STORE_ID,
        DS.MATCH_ID,
        DS.RESTAURANT_NAME,
        DS.STARTING_POINT_ID,
        DS.SUBMARKET_ID,
        SF.FIRST_ORDER_DATE,
        SF.FIRST_PARTNER_ORDER_DATE,
--        SS.FIRST_SALESFORCE_ACTIVATION_DATE,
        coalesce(md.activation_date,SF.FIRST_PARTNER_ORDER_DATE) AS STORE_ACTIVATION_DATE
    FROM
        ALL_PARTNER_STORES AS DS
       -- this inner join makes sure all the mx in store proxy activation are net new stores
    LEFT JOIN 
        merchant_dates_dedup as md
    ON 
        ds.store_id = md.store_id
    INNER JOIN
        STORE_FIRST_ORDER AS SF ON DS.STORE_ID = SF.STORE_ID
--      LEFT OUTER JOIN
--          STORE_SALESFORCE_ACTIVATION AS SS ON SF.STORE_ID = SS.STORE_ID
    WHERE 1
        AND SF.FIRST_ORDER_DATE = SF.FIRST_PARTNER_ORDER_DATE
        AND store_activation_date IS NOT NULL 
          -- this is so the store have 120-148 day sales
--        AND DATEDIFF('DAYS', STORE_ACTIVATION_DATE, CURRENT_TIMESTAMP) >= 148
)

, STORE_TARGET_SALES AS (
    SELECT
        FDSC_0.store_id AS NIMDA_STORE_ID,
        FDSC_0.RESTAURANT_NAME,
        FDSC_0.MATCH_ID,
        SUM(
            CASE
            WHEN DD_8.ACTIVE_DATE <= dateadd('DAY', 27, FDSC_0.STORE_ACTIVATION_DATE) THEN DD_8.SUBTOTAL / 100.0
            ELSE 0
            END) AS TOTAL_SUBTOTAL_FIRST_28_DAYS,
        SUM(
            CASE
            WHEN DD_8.ACTIVE_DATE <= dateadd('DAY', 29, FDSC_0.STORE_ACTIVATION_DATE) THEN DD_8.SUBTOTAL / 100.0
            ELSE 0
            END) AS TOTAL_SUBTOTAL_FIRST_30_DAYS,
        SUM(CASE
            WHEN DD_8.ACTIVE_DATE
              BETWEEN dateadd('DAY', 120, DATE_TRUNC('DAY', FDSC_0.STORE_ACTIVATION_DATE))
              AND dateadd('DAY', 148, DATE_TRUNC('DAY', FDSC_0.STORE_ACTIVATION_DATE))
            THEN DD_8.SUBTOTAL / 100.0
            ELSE 0
            END) AS TOTAL_SUBTOTAL_FIRST_120_148_DAYS
    FROM
        STORE_PROXY_ACTIVATION AS FDSC_0
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_8
    ON 
        FDSC_0.STORE_ID = DD_8.STORE_ID
        AND DD_8.IS_FILTERED_CORE = 1
        AND DD_8.ACTIVE_DATE >= FDSC_0.STORE_ACTIVATION_DATE
    GROUP BY 
        1, 2, 3
)

-- CREATE OR REPLACE TABLE chendong.net_new_training AS 
SELECT
    FDSC.MATCH_ID,
    MX.VENDOR_1_ID,
    FDSC.STORE_ID,
    FDSC.RESTAURANT_NAME,
    FDSC.STARTING_POINT_ID,
    FDSC.SUBMARKET_ID,
    CASE
      WHEN MX.VENDOR_1_ID IS NOT null THEN 'requestable'
      ELSE 'not_requestable' 
    END AS requestable,
    FDSC.FIRST_PARTNER_ORDER_DATE,
    FDSC.STORE_ACTIVATION_DATE,
    EXTRACT(MONTH FROM FDSC.STORE_ACTIVATION_DATE) AS ACTIVATION_MONTH,
    EXTRACT(YEAR FROM FDSC.STORE_ACTIVATION_DATE) AS ACTIVATION_YEAR, 
    -- target
    STS.TOTAL_SUBTOTAL_FIRST_28_DAYS,
    STS.TOTAL_SUBTOTAL_FIRST_30_DAYS,
    STS.TOTAL_SUBTOTAL_FIRST_120_148_DAYS,
    current_date AS CREATE_DATE
FROM
    STORE_PROXY_ACTIVATION AS FDSC
LEFT OUTER JOIN 
    PUBLIC.fact_selection_intel_mx_raw AS mx  
ON 
    FDSC.match_id=mx.id
JOIN
    STORE_TARGET_SALES AS STS 
ON 
    FDSC.STORE_ID = STS.NIMDA_STORE_ID
WHERE 1
  -- remove fradulent stores   
    AND(fdsc.store_id NOT IN  (
        SELECT store_id FROM  PUBLIC.DIMENSION_STORE WHERE LAST_DEACTIVATION_REASON = 'Fraudulent Store'
      ) or fdsc.store_id is null)
"""
from ddselection.data.query_config import *


query_aov_danp = f"""
WITH  mx_filtered_mx_db_w_fraudulent as (
    with active_stores as (
        select *
        from
            dimension_store
        where
            is_active = 1 and is_partner = 1
    )
    SELECT
        r.*
    FROM
        fact_selection_intel_mx_filtered_raw r
    LEFT JOIN
        active_stores act on r.nimda_id = act.store_id
    WHERE
        act.store_id is null --hide active nimda
)
, mx_filtered_mx_db as (
  select * from mx_filtered_mx_db_w_fraudulent
  WHERE 1
    AND (nimda_id NOT in  (
      SELECT store_id FROM  PUBLIC.DIMENSION_STORE WHERE LAST_DEACTIVATION_REASON = 'Fraudulent Store'
    ) OR nimda_id IS NULL)
)

--TODO look into the joins.
-- get the deactivated merchants.
, mx_DEACTIVATED_MERCHANTS AS (
      SELECT
          S.ID AS MATCH_ID
          , COALESCE(s.NIMDA_ID, DS.STORE_ID) AS NIMDA_STORE_ID
          , COALESCE(S.VENDOR_1_ID, es.external_id) as VENDOR_1_ID
          , S.VENDOR_2_ID
          , COALESCE(S.RESTAURANT_NAME, DS.NAME) AS NAME
          , DS.BUSINESS_ID
          , COALESCE(
                   S.SP_ID,
           DS.STARTING_POINT_ID,
                     ES.STARTING_POINT_ID,
                     dd.STORE_STARTING_POINT_ID )
          AS STARTING_POINT_ID
          , coalesce(s.sub_id, DS.SUBMARKET_ID) as SUBMARKET_ID
          , date_trunc('DAY',
             CONVERT_TIMEZONE('UTC', COALESCE(MAX(MM.TIMEZONE), 'US/Pacific'),
                           COALESCE(max(dd.created_at), MAX(DE.CREATED_AT), MAX(DD.ACTIVE_DATE), MAX(DR.MOST_RECENT_DEACT))
              )) AS MOST_RECENT_DEACTIVATION_DATE
          -- , dateadd('DAY', 28, date_trunc('DAY', TO_TIMESTAMP_NTZ(LOCALTIMESTAMP))) AS PROJECTED_PARTNER_LIVE_DATE
      FROM
          mx_filtered_mx_db AS S
      LEFT JOIN
          PUBLIC.DIMENSION_STORE DS
      ON S.NIMDA_ID = DS.STORE_ID
      LEFT JOIN
          doordash_merchant.public.maindb_external_store AS ES ON S.NIMDA_ID = es.store_id
      LEFT JOIN
          PUBLIC.FACT_DEACT_REACT AS DR ON S.NIMDA_ID = DR.STORE_ID
      LEFT JOIN
          doordash_merchant.public.maindb_store_deactivation AS DE ON S.NIMDA_ID = DE.STORE_ID
      LEFT JOIN
          GEO_INTELLIGENCE.PUBLIC.MAINDB_MARKET MM ON MM.ID = DS.MARKET_ID
      inner JOIN
          PUBLIC.DIMENSION_DELIVERIES AS DD
            ON S.NIMDA_ID = DD.STORE_ID
            AND DD.IS_FILTERED_CORE = 1
      WHERE 1
      AND (s.IS_ACTIVE=false OR s.is_active IS NULL)
      GROUP BY
          1, 2, 3, 4, 5, 6, 7, 8
)

-- get the active nonpartner stores. set the store deactivation date as yesterday for the active stores.
, mx_ACTIVE_NONPARTNERS AS (
  select
    mx.id AS match_id
    , COALESCE(mx.nimda_id, ds.store_id) AS nimda_store_id
    , COALESCE(mx.VENDOR_1_ID, ES.EXTERNAL_ID) as VENDOR_1_ID
    , mx.VENDOR_2_ID
    , COALESCE(mx.RESTAURANT_NAME, DS.NAME) AS NAME
    , DS.BUSINESS_ID
    , COALESCE(
        mx.SP_ID,
        DS.STARTING_POINT_ID,
      ES.STARTING_POINT_ID)
    AS STARTING_POINT_ID
    , COALESCE(mx.SUB_ID, DS.SUBMARKET_ID) AS SUBMARKET_ID
    , dateadd('DAY', -1, '{{test_date}}') AS MOST_RECENT_DEACTIVATION_DATE
    from
        mx_filtered_mx_db mx
    LEFT JOIN
    PUBLIC.dimension_store AS ds
    ON mx.NIMDA_ID = DS.STORE_ID
  LEFT JOIN
    doordash_merchant.public.maindb_external_store AS ES ON mx.nimda_id = es.store_id 
    WHERE mx.is_active=TRUE
    AND (mx.is_partner=FALSE or mx.is_partner is null)
)

-- the following five cte's are used to get the deactivation dates for the deactivated stores
-- get the stores that are not live
, mx_not_live as (
    select *
    from
        public.dimension_store st
    where
        st.is_active = 0
)

-- get the precise deactivation dates. '2018-10-15' is the date since when Anna Seidler started collecting the snapshots of store's active/deactive status every day.
, mx_precise_dates as (
    select
        store_id
        ,max(date_stamp) as last_np_live_date
    from
        public.fact_store_active_status
    where
        active_eop = 1
        AND partner_eop = 0
        AND date_stamp > '2018-10-15'
    group by
        1
    having
        last_np_live_date < dateadd('days', -7, '{{test_date}}')
)

, mx_most_recent_deact as (
    SELECT
          MD.CREATED_AT::date as most_recent_deactivation_time,
          MD.STORE_ID,
          MD.ID AS DEACTIVATION_ID,
          MD.REASON_ID,
          MD.NOTES,
          ROW_NUMBER() OVER (PARTITION BY MD.STORE_ID ORDER BY MD.CREATED_AT DESC) AS DEACTIVATED_RANK
        FROM
          doordash_merchant.public.maindb_store_deactivation MD
        LEFT JOIN PUBLIC.DIMENSION_STORE AS S ON S.STORE_ID = MD.STORE_ID
        WHERE
            S.IS_ACTIVE = 0
)

, mx_deact_reason as (
    select mr.*
    from
        mx_most_recent_deact mr
    where
        mr.DEACTIVATED_RANK = 1
)

, mx_merchant_dates_dedup AS (
    select
        nl.store_id
    ,coalesce( dr.most_recent_deactivation_time, mr.last_np_live_date) as last_np_live_day
    from
        mx_not_live nl
    left join
        mx_precise_dates mr ON mr.store_id = nl.store_id
    left join
        mx_deact_reason dr ON dr.store_id = nl.store_id
    where
        ((dr.reason_id <> 1) or (dr.reason_id is null))
        AND ( (coalesce(mr.last_np_live_date, dr.most_recent_deactivation_time) < dateadd('days', -7, '{{test_date}}') ) or (coalesce(mr.last_np_live_date, dr.most_recent_deactivation_time) is null ))  
)
, mx_activated_after AS (
  select
        match_id
      , store_id AS nimda_store_id
      , VENDOR_1_ID
      , NULL AS VENDOR_2_ID
      , EXTERNAL_STORE_NAME AS NAME
      , BUSINESS_ID
      , STARTING_POINT_ID
      , SUBMARKET_ID
      , most_recent_deactivation_timestamp
      , dateadd('day', -1, '{{test_date}}') as activation_date
      , CASE WHEN MERCHANT_TYPE = 'active_np' THEN 'active' WHEN MERCHANT_TYPE = 'deactivated_np' THEN 'deactivated' ELSE NULL END AS MERCHANT_TYPE_PROD
  from
      {DANP_TRAIN_5MONTH_TABLE_NAME}
  WHERE
      REACTIVATION_TIMESTAMP > '{{test_date}}'
      AND create_date IN (select max(create_date) from {DANP_TRAIN_5MONTH_TABLE_NAME})
)
-- combine the active np and deactivated stores
, mx_DEACTIVATED_NONPARTNER_COMBINED AS (
    SELECT
          D.match_id
        , D.nimda_store_id
        , D.VENDOR_1_ID
        , D.VENDOR_2_ID
        , D.NAME
        , D.BUSINESS_ID
        , D.STARTING_POINT_ID
        , D.SUBMARKET_ID
        , COALESCE( D.most_recent_deactivation_date, md.last_np_live_day) AS most_recent_deactivation_timestamp
        , dateadd('day', -1, '{{test_date}}') as activation_date
        , 'deactivated' as MERCHANT_TYPE_PROD
    FROM
        mx_DEACTIVATED_MERCHANTS AS D
    LEFT JOIN
        mx_merchant_dates_dedup AS md
    ON
        D.nimda_store_id = md.store_id

    UNION

    SELECT
          match_id
        , nimda_store_id
        , VENDOR_1_ID
        , VENDOR_2_ID
        , NAME
        , BUSINESS_ID
        , STARTING_POINT_ID
        , SUBMARKET_ID
        , most_recent_deactivation_date AS most_recent_deactivation_timestamp
        , dateadd('day', -1, '{{test_date}}') as activation_date
        , 'active' as MERCHANT_TYPE_PROD
    FROM
        mx_ACTIVE_NONPARTNERS

    UNION

    SELECT
          match_id
        , nimda_store_id
        , VENDOR_1_ID
        , VENDOR_2_ID
        , NAME
        , BUSINESS_ID
        , STARTING_POINT_ID
        , SUBMARKET_ID
        , most_recent_deactivation_timestamp
        , activation_date
        , MERCHANT_TYPE_PROD
    FROM
        mx_activated_after
)

-- TODO this is to replace the table above. Note when left join to _DEACTIVATED_NONPARTNER_COMBINED, I need to make sure if null values should be 0
, mx_STORES_NONPARTNER_INFO AS (
  SELECT
    store_id,
    COALESCE(SUM(
        CASE
        WHEN DD.IS_PARTNER IS NOT DISTINCT FROM false THEN 1
        ELSE 0
        END), 0) AS TOTAL_NONPARTNER_ORDERS_LIFETIME,
  --        COALESCE(COUNT(DISTINCT DD.DELIVERY_ID), 0) AS TOTAL_ORDERS_LIFETIME,
    MIN(to_timestamp_ntz(DD.CREATED_AT)) AS FIRST_LIFETIME_ORDER,
    MAX(CASE
            WHEN DD.IS_PARTNER IS NOT DISTINCT FROM false
              THEN to_timestamp_ntz(DD.CREATED_AT)
            ELSE NULL
            END) AS LAST_NP_ORDER,
    MIN(CASE
            WHEN DD.IS_PARTNER IS NOT DISTINCT FROM true
              THEN to_timestamp_ntz(DD.CREATED_AT)
            ELSE NULL
            END) AS FIRST_PARTNER_ORDER
    FROM PUBLIC.dimension_deliveries dd
    WHERE 1
    AND DD.IS_FILTERED_CORE= true
    AND DD.ACTIVE_DATE >= '2014-01-01'::TIMESTAMP
    GROUP BY dd.store_id
)



-- calculate the stats before a store was deactivated.
-- NUM_FIRST_ORDERCARTS_BEFORE indicate the number users came to DoorDash because of this store.
-- TODO check if the dateadd -9 and -30 is the same as training data 
, mx_STORE_STATS_BEFORE_DEACTIVATION AS (
     SELECT
            MRD.nimda_STORE_ID,
            MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP,
            COUNT(DISTINCT DD_0.DELIVERY_ID) AS NUM_ORDERS_BEFORE,
            COUNT(DISTINCT DD_0.ACTIVE_DATE) AS NUM_DAYS_ACTIVE_BEFORE,
            AVG(DD_0.FEE / 100.0) AS AVG_FEE_BEFORE,
            COALESCE(AVG(DD_0.SUBTOTAL / 100.0),0) AS AVG_SUBTOTAL_BEFORE,
            COALESCE(SUM(DD_0.SUBTOTAL / 100.0),0) AS TOTAL_SUBTOTAL_BEFORE,
            COALESCE(TOTAL_SUBTOTAL_BEFORE / NULLIF(NUM_ORDERS_BEFORE, 0), 0) AS AOV_BEFORE,
            coalesce(MEDIAN(DD_0.SUBTOTAL / 100.0),0) AS MEDIAN_SUBTOTAL_BEFORE,
            coalesce(max(DD_0.SUBTOTAL / 100.0),0) AS max_SUBTOTAL_BEFORE,
            SUM(CASE
                WHEN DD_0.IS_FIRST_ORDERCART IS NOT DISTINCT FROM true THEN 1
                ELSE 0
                END) AS NUM_FIRST_ORDERCARTS_BEFORE,
            SUM(CASE
                WHEN DD_0.IS_PARTNER IS NOT DISTINCT FROM true THEN 1.0
                ELSE 0.0
                END) / NULLIF(COUNT(1), 0) AS PCT_PARTNER_DELIVS_BEFORE,
      COALESCE(COUNT(DISTINCT
                CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.ACTIVE_DATE
                ELSE NULL
                END), 0) AS NUM_DAYS_ACTIVE_7DAYS_BEFORE,
      COALESCE(AVG(CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.SUBTOTAL / 100.0
                ELSE NULL
        END), 0) AS AVG_SUBTOTAL_7DAYS_BEFORE,
      COALESCE(COUNT(DISTINCT
                CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.DELIVERY_ID
                ELSE NULL
                END) , 0)AS NUM_ORDERS_7DAYS_BEFORE,
      COALESCE(AVG(CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.FEE / 100.0
                ELSE NULL
        END) , 0)AS AVG_FEE_7DAYS_BEFORE,
      COALESCE(MEDIAN(CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.SUBTOTAL / 100.0
                ELSE NULL
        END) , 0)AS MEDIAN_SUBTOTAL_7DAYS_BEFORE,
      COALESCE(SUM(CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.SUBTOTAL
                ELSE NULL
        END), 0) AS TOTAL_SUBTOTAL_7DAYS_BEFORE,
      AVG(CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.COMMISSION_RATE
                ELSE NULL
        END) AS AVG_COMMISSION_RATE_7DAYS_BEFORE,
      MEDIAN(CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.COMMISSION_RATE
                ELSE NULL
        END) AS MEDIAN_COMMISSION_RATE_7DAYS_BEFORE,
      COALESCE(COUNT(DISTINCT
                CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) AND DD_0.IS_FIRST_ORDERCART IS NOT DISTINCT FROM true THEN DD_0.DELIVERY_ID
                ELSE NULL
                END), 0) AS NUM_FIRST_ORDERCARTS_7DAYS_BEFORE,
      COALESCE(COUNT(DISTINCT
                CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) AND DD_0.IS_PARTNER IS NOT DISTINCT FROM true THEN DD_0.DELIVERY_ID
                ELSE NULL
                END), 0) AS NUM_PARTNER_DELIVS_7DAYS_BEFORE,
      COALESCE(COUNT(DISTINCT
                CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.ACTIVE_DATE
                ELSE NULL
                END), 0) AS NUM_DAYS_ACTIVE_28DAYS_BEFORE,
      COALESCE(COUNT(DISTINCT
                CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.DELIVERY_ID
                ELSE NULL
                END), 0) AS NUM_ORDERS_28DAYS_BEFORE,
      COALESCE(AVG(CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.SUBTOTAL / 100.0
                ELSE NULL
        END), 0) AS AVG_SUBTOTAL_28DAYS_BEFORE,
      COALESCE(SUM(CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.SUBTOTAL/100.0
                ELSE NULL
        END), 0) AS TOTAL_SUBTOTAL_28DAYS_BEFORE,
      COALESCE(AVG(CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.FEE / 100.0
                ELSE NULL
        END) , 0)AS AVG_FEE_28DAYS_BEFORE,
      COALESCE(MEDIAN(CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.SUBTOTAL / 100.0
                ELSE NULL
                              END) , 0)AS MEDIAN_SUBTOTAL_28DAYS_BEFORE,
      COALESCE(AVG(CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.COMMISSION_RATE
                ELSE NULL
                           END) , 0)AS AVG_COMMISSION_RATE_28DAYS_BEFORE,
      COALESCE(MEDIAN(CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.COMMISSION_RATE
                ELSE NULL
                              END) , 0)AS MEDIAN_COMMISSION_RATE_28DAYS_BEFORE,
      COALESCE(COUNT(DISTINCT
                CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) AND DD_0.IS_FIRST_ORDERCART IS NOT DISTINCT FROM true THEN DD_0.DELIVERY_ID
                ELSE NULL
                END) , 0)AS NUM_FIRST_ORDERCARTS_28DAYS_BEFORE,
      COALESCE(COUNT(DISTINCT
                CASE
                WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) AND DD_0.IS_PARTNER IS NOT DISTINCT FROM true THEN DD_0.DELIVERY_ID
                ELSE NULL
                END) , 0)AS NUM_PARTNER_DELIVS_28DAYS_BEFORE
          FROM
              mx_DEACTIVATED_NONPARTNER_COMBINED AS MRD
          LEFT OUTER JOIN
              PUBLIC.DIMENSION_DELIVERIES AS DD_0
                ON MRD.nimda_STORE_ID = DD_0.STORE_ID
                AND DD_0.ACTIVE_DATE <= MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP
                AND DD_0.IS_FILTERED_CORE IS NOT DISTINCT FROM true
          GROUP BY (1, 2)
)

-- This table compute has some statistics before store deactivation of the business a restaurant belongs to. these statistics are derived from dimension deliveries
, MX_BUSINESS_STATS_BEFORE_DEACTIVATION AS (
        SELECT
      MRD_0.BUSINESS_ID,
      MRD_0.MOST_RECENT_DEACTIVATION_TIMESTAMP,
            COUNT(DISTINCT DD_1.STORE_ID) AS NUM_STORES_BUSINESS_BEFORE,
            COALESCE(COUNT(DISTINCT DD_1.DELIVERY_ID), 0) AS NUM_ORDERS_BUSINESS_BEFORE,
            CAST(COUNT(DISTINCT DD_1.DELIVERY_ID) as FLOAT) / CAST(NULLIF(COUNT(DISTINCT DD_1.STORE_ID), 0) as FLOAT) AS NUM_ORDERS_PER_STORE_BUSINESS_BEFORE,
            COALESCE(SUM(DD_1.SUBTOTAL / 100.0) / NULLIF(COUNT(DISTINCT DD_1.STORE_ID), 0), 0) AS AVG_SUBTOTAL_PER_STORE_BUSINESS_BEFORE,
            SUM(DD_1.SUBTOTAL / 100.0) AS TOTAL_SUBTOTAL_BUSINESS_BEFORE,
            COALESCE(SUM(DD_1.SUBTOTAL / 100.0) / NULLIF(COUNT(DISTINCT DD_1.ACTIVE_DATE), 0), 0) AS TOTAL_SUBTOTAL_PER_ACTIVE_DATE_BUSINESS_BEFORE
    FROM
   (
        SELECT DISTINCT
            mx_DEACTIVATED_NONPARTNER_COMBINED.BUSINESS_ID,
            mx_DEACTIVATED_NONPARTNER_COMBINED.MOST_RECENT_DEACTIVATION_TIMESTAMP
            FROM
                  mx_DEACTIVATED_NONPARTNER_COMBINED
    ) AS MRD_0
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_1
    ON MRD_0.BUSINESS_ID = DD_1.BUSINESS_ID
      AND DD_1.ACTIVE_DATE <= MRD_0.MOST_RECENT_DEACTIVATION_TIMESTAMP
      AND DD_1.IS_FILTERED_CORE=true
    GROUP BY (
      MRD_0.BUSINESS_ID,
      MRD_0.MOST_RECENT_DEACTIVATION_TIMESTAMP)
)

-- This table compute has some statistics before store reactivation of the business a restaurant belongs to. these statistics are derived from dimension deliveries
, mx_BUSINESS_STATS_BEFORE_REACTIVATION AS (
        SELECT
      MRD_0.BUSINESS_ID,
      MRD_0.ACTIVATION_DATE,
            COUNT(DISTINCT DD_1.STORE_ID) AS NUM_STORES_BUSINESS_BEFORE,
            COALESCE(COUNT(DISTINCT DD_1.DELIVERY_ID), 0) AS NUM_ORDERS_BUSINESS_BEFORE,
            CAST(COUNT(DISTINCT DD_1.DELIVERY_ID) as FLOAT) / CAST(NULLIF(COUNT(DISTINCT DD_1.STORE_ID), 0) as FLOAT) AS NUM_ORDERS_PER_STORE_BUSINESS_BEFORE,
            COALESCE(SUM(DD_1.SUBTOTAL / 100.0) / NULLIF(COUNT(DISTINCT DD_1.STORE_ID), 0), 0) AS AVG_SUBTOTAL_PER_STORE_BUSINESS_BEFORE,
            SUM(DD_1.SUBTOTAL / 100.0) AS TOTAL_SUBTOTAL_BUSINESS_BEFORE,
            COALESCE(SUM(DD_1.SUBTOTAL / 100.0) / NULLIF(COUNT(DISTINCT DD_1.ACTIVE_DATE), 0), 0) AS TOTAL_SUBTOTAL_PER_ACTIVE_DATE_BUSINESS_BEFORE
    FROM
      (
        SELECT DISTINCT
          mx_DEACTIVATED_NONPARTNER_COMBINED.BUSINESS_ID,
          mx_DEACTIVATED_NONPARTNER_COMBINED.ACTIVATION_DATE
          FROM
              mx_DEACTIVATED_NONPARTNER_COMBINED
      ) AS MRD_0
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_1
    ON MRD_0.BUSINESS_ID = DD_1.BUSINESS_ID
      AND DD_1.ACTIVE_DATE <= MRD_0.activation_date
      AND DD_1.IS_FILTERED_CORE=true
    GROUP BY (
        MRD_0.BUSINESS_ID,
        MRD_0.ACTIVATION_DATE)
)

, mx_SP_STATS_BEFORE AS (
        SELECT
            SR_1.nimda_STORE_ID,
            SR_1.STARTING_POINT_ID,
            SR_1.MOST_RECENT_DEACTIVATION_TIMESTAMP,
            SR_1.ACTIVATION_date,
            COUNT(DISTINCT DD_4.DELIVERY_ID) AS NUM_ORDERS_SP_7DAYS_BEFORE,
            SUM(DD_4.SUBTOTAL / 100.0) AS TOTAL_SUBTOTAL_SP_7DAYS_BEFORE,
            AVG(DD_4.FEE / 100.0) AS AVG_FEE_SP_7DAYS_BEFORE,
            COALESCE(
                SUM(CASE
                  WHEN DD_4.IS_PARTNER IS NOT DISTINCT FROM true THEN 1
                  ELSE 0
                END) * 1.0 / NULLIF(COUNT(DD_4.DELIVERY_ID), 0),
            0) AS PCT_PARTNER_SP_7DAYS_BEFORE,
            COUNT(DISTINCT DD_4.STORE_ID) AS NUM_STORES_SP_7DAYS_BEFORE,
            COUNT(DISTINCT DD_4.CREATOR_ID) AS NUM_CONSUMERS_SP_7DAYS_BEFORE
        FROM
            mx_DEACTIVATED_NONPARTNER_COMBINED AS SR_1
        LEFT OUTER JOIN
            PUBLIC.DIMENSION_DELIVERIES AS DD_4
              ON SR_1.STARTING_POINT_ID = DD_4.STORE_STARTING_POINT_ID
        AND DD_4.ACTIVE_DATE <= SR_1.MOST_RECENT_DEACTIVATION_TIMESTAMP
        AND DD_4.ACTIVE_DATE > dateadd('DAY', -7, SR_1.MOST_RECENT_DEACTIVATION_TIMESTAMP)
              AND DD_4.IS_FILTERED_CORE=True
        GROUP BY (1, 2, 3, 4)
)

, mx_SP_STATS_AFTER AS (
        SELECT
            SR_2.nimda_STORE_ID,
            SR_2.STARTING_POINT_ID,
            SR_2.ACTIVATION_DATE,
            COUNT(DISTINCT DD_5.DELIVERY_ID) AS NUM_ORDERS_SP_7DAYS_AFTER,
            SUM(DD_5.SUBTOTAL / 100.0) AS TOTAL_SUBTOTAL_SP_7DAYS_AFTER,
            -- AVG(DD_5.FEE / 100.0) AS AVG_FEE_SP_7DAYS_AFTER,
            2.25 AS AVG_FEE_SP_7DAYS_AFTER,
            SUM(CASE
              WHEN DD_5.IS_PARTNER IS NOT DISTINCT FROM true THEN 1
              ELSE 0
            END) * COALESCE(1.0 / NULLIF(COUNT(DD_5.DELIVERY_ID), 0), 0) AS PCT_PARTNER_SP_7DAYS_AFTER,
            COUNT(DISTINCT DD_5.STORE_ID) AS NUM_STORES_SP_7DAYS_AFTER,
            COUNT(DISTINCT DD_5.CREATOR_ID) AS NUM_CONSUMERS_SP_7DAYS_AFTER
    FROM
        mx_DEACTIVATED_NONPARTNER_COMBINED AS SR_2
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_5
    ON SR_2.STARTING_POINT_ID = DD_5.STORE_STARTING_POINT_ID
    --          ML: change the reactivation_timestamp to current date to create prod data
              AND DD_5.ACTIVE_DATE <= SR_2.ACTIVATION_DATE
    --          ML: change the reactivation_timestamp to current date to create prod data
              AND DD_5.ACTIVE_DATE > dateadd('DAY', -7, SR_2.ACTIVATION_DATE)
              AND DD_5.IS_FILTERED_CORE=true
    GROUP BY (1, 2, 3)
)

, mx_SP_STATS_COMBINED AS (
        SELECT
            SPB.nimda_STORE_ID,
            SPB.STARTING_POINT_ID,
            SPB.MOST_RECENT_DEACTIVATION_TIMESTAMP,
            SPB.ACTIVATION_DATE,
            SPB.NUM_ORDERS_SP_7DAYS_BEFORE,
            SPB.TOTAL_SUBTOTAL_SP_7DAYS_BEFORE,
            SPB.NUM_STORES_SP_7DAYS_BEFORE,
            SPB.NUM_CONSUMERS_SP_7DAYS_BEFORE,
            SPB.PCT_PARTNER_SP_7DAYS_BEFORE,
            SPB.AVG_FEE_SP_7DAYS_BEFORE,
            SPA.NUM_ORDERS_SP_7DAYS_AFTER,
            SPA.TOTAL_SUBTOTAL_SP_7DAYS_AFTER,
            SPA.NUM_STORES_SP_7DAYS_AFTER,
            SPA.NUM_CONSUMERS_SP_7DAYS_AFTER,
            SPA.PCT_PARTNER_SP_7DAYS_AFTER,
            SPA.AVG_FEE_SP_7DAYS_AFTER,
            1.0 +
            COALESCE(
                (SPA.TOTAL_SUBTOTAL_SP_7DAYS_AFTER - SPB.TOTAL_SUBTOTAL_SP_7DAYS_BEFORE) / NULLIF((SPB.TOTAL_SUBTOTAL_SP_7DAYS_BEFORE * 1.0), 0),
                0) AS TOTAL_SUBTOTAL_SCALER_SP,
            COALESCE(
                (SPA.NUM_ORDERS_SP_7DAYS_AFTER - SPB.NUM_ORDERS_SP_7DAYS_BEFORE) / NULLIF((SPB.NUM_ORDERS_SP_7DAYS_BEFORE * 1.0), 0),
                0) AS NUM_ORDERS_GROWTH_SP,
            COALESCE(
                (SPA.NUM_STORES_SP_7DAYS_AFTER - SPB.NUM_STORES_SP_7DAYS_BEFORE) / NULLIF((SPB.NUM_STORES_SP_7DAYS_BEFORE * 1.0), 0),
                0) AS NUM_STORES_GROWTH_SP,
            COALESCE(
                   (SPA.NUM_CONSUMERS_SP_7DAYS_AFTER - SPB.NUM_CONSUMERS_SP_7DAYS_BEFORE) / NULLIF((SPB.NUM_CONSUMERS_SP_7DAYS_BEFORE * 1.0), 0),
                0) AS NUM_CONSUMERS_GROWTH_SP,
            SPA.PCT_PARTNER_SP_7DAYS_AFTER - SPB.PCT_PARTNER_SP_7DAYS_BEFORE AS PCT_PARTNER_CHANGE_SP,
            SPA.AVG_FEE_SP_7DAYS_AFTER - SPB.AVG_FEE_SP_7DAYS_BEFORE AS AVG_FEE_CHANGE_SP
           FROM
        mx_SP_STATS_BEFORE AS SPB
        JOIN mx_SP_STATS_AFTER AS SPA ON SPB.nimda_STORE_ID = SPA.nimda_STORE_ID AND SPB.STARTING_POINT_ID = SPA.STARTING_POINT_ID AND SPB.MOST_RECENT_DEACTIVATION_TIMESTAMP < SPA.ACTIVATION_DATE
)

, mx_STORE_REQUESTS AS (
    SELECT
        SA_0.MATCH_ID,
        SA_0.activation_date,
        MIN(R.REQUESTED_AT) AS FIRST_REQUEST_REQUESTED_AT,
        MAX(R.REQUESTED_AT) AS LAST_REQUEST_REQUESTED_AT,
        DATEDIFF('DAY', MIN(R.REQUESTED_AT), MAX(R.REQUESTED_AT)) AS DAYS_BTW_FIRST_AND_LAST_REQUEST,
        -- TODO should this be distinct
        COUNT(DISTINCT r.consumer_id) AS NUM_REQUESTS_ALL_TIME,
        TRUNC(AVG(CAST(DD_0.SUBTOTAL as FLOAT))) AS AVG_SUBTOTAL_FROM_CONSUMER_REQUEST,
        COALESCE(
            CAST(COUNT(DISTINCT DD_0.DELIVERY_ID) as FLOAT) / CAST(NULLIF(COUNT(DISTINCT r.consumer_id), 0) as FLOAT),
            0) AS NUM_DELIVERY_TO_REQUESTS_RATIO,
        COUNT(
          DISTINCT
          CASE
          WHEN r.requested_at BETWEEN dateadd('day', -(28*3+1), '{{test_date}}') AND dateadd('DAY', -1, '{{test_date}}') THEN r.consumer_id
          ELSE NULL
--          NUM_REQUESTS_L14DAYS is a legacy misnomer, it is actually 7 days of requests.
          END) AS NUM_REQUESTS_L84DAYS,
        COUNT(
          DISTINCT
          CASE
          WHEN r.requested_at BETWEEN dateadd('day', -(28+1), '{{test_date}}') AND dateadd('DAY', -1, '{{test_date}}') THEN r.consumer_id
          ELSE NULL
          END) AS NUM_REQUESTS_L28DAYS
    FROM
        mx_DEACTIVATED_NONPARTNER_COMBINED SA_0
    LEFT OUTER JOIN
        public.fact_selection_intel_mx_external_store_requests r
        ON sa_0.match_id = r.mx_id
        AND R.REQUESTED_AT<dateadd('DAY', -1, '{{test_date}}')
    LEFT OUTER JOIN
          PUBLIC.DIMENSION_DELIVERIES AS DD_0
          ON R.CONSUMER_ID = DD_0.CREATOR_ID
          AND R.REQUESTED_AT > DD_0.CREATED_AT
          AND DD_0.IS_FILTERED_CORE = 1
          AND DD_0.SUBTOTAL > 0
     GROUP BY 1,2
)

, mx_SEARCH_COUNT AS (
    SELECT
        store_id,
        COALESCE(avg(search_count), 0) as avg_search_count_84_days_before
    FROM
    (
        SELECT
            STORE_ID,
            SUM(QUERY_COUNT/STORE_COUNT) AS SEARCH_COUNT
        FROM
            luwang.FACT_STORE_SEARCH_COUNTS_TFIDF_MATCH_NEW
        WHERE
            SEARCH_DATE BETWEEN DATEADD('DAYS', -(28*3), '{{test_date}}') AND DATEADD('DAYS', -1, '{{test_date}}') 
        GROUP BY
            STORE_ID, search_date
    )
    GROUP BY store_id
)


    SELECT
        S_0.nimda_STORE_ID,
        S_0.NAME,
    COALESCE(S_0.MATCH_ID, RX.ID) as MATCH_ID,
    S_0.VENDOR_1_ID,
    -- S_0.VENDOR_2_ID,
        S_0.STARTING_POINT_ID,
        SP.SUBMARKET_ID,
    case
    when s_0.VENDOR_1_ID IS NOT null  then  'requestable'
    ELSE 'not_requestable'
    end requestable,
        S_0.MOST_RECENT_DEACTIVATION_TIMESTAMP,
    --    SR.MOST_RECENT_DELIVERY_TIMESTAMP,
        CASE
      WHEN datediff('DAY', SNI.LAST_NP_ORDER, SNI.FIRST_PARTNER_ORDER) BETWEEN 0 AND 28 THEN 'nonpartner_partner_conversion'
      WHEN SNI.TOTAL_NONPARTNER_ORDERS_LIFETIME > 0 THEN 'nonpartner_deactivated_partner'
      WHEN SNI.TOTAL_NONPARTNER_ORDERS_LIFETIME = 0 THEN 'partner_deactivated_partner'
    ELSE 'other'
        END AS MERCHANT_TYPE,
        SBD.NUM_ORDERS_BEFORE,
        SBD.NUM_DAYS_ACTIVE_BEFORE,
        SBD.AVG_FEE_BEFORE,
        SBD.AVG_SUBTOTAL_BEFORE,
        SBD.AOV_BEFORE,
        SBD.MEDIAN_SUBTOTAL_BEFORE,
        SBD.MAX_SUBTOTAL_BEFORE,
        SBD.TOTAL_SUBTOTAL_BEFORE,
        COALESCE(SBD.TOTAL_SUBTOTAL_BEFORE/SBD.NUM_DAYS_ACTIVE_BEFORE, 0) AS TOTAL_SUBTOTAL_PER_ACTIVE_DATE_BEFORE,
        COALESCE(SBD.NUM_ORDERS_BEFORE::FLOAT/SBD.NUM_DAYS_ACTIVE_BEFORE, 0) AS NUM_ORDERS_PER_ACTIVE_DATE_BEFORE,
        SBD.NUM_FIRST_ORDERCARTS_BEFORE,
        SBD.PCT_PARTNER_DELIVS_BEFORE,
        SBD.NUM_ORDERS_7DAYS_BEFORE,
        SBD.NUM_DAYS_ACTIVE_7DAYS_BEFORE,
        SBD.AVG_FEE_7DAYS_BEFORE,
        SBD.AVG_SUBTOTAL_7DAYS_BEFORE,
        SBD.MEDIAN_SUBTOTAL_7DAYS_BEFORE,
        SBD.TOTAL_SUBTOTAL_7DAYS_BEFORE,
        SBD.NUM_FIRST_ORDERCARTS_7DAYS_BEFORE,
        SBD.NUM_DAYS_ACTIVE_28DAYS_BEFORE,
        SBD.TOTAL_SUBTOTAL_28DAYS_BEFORE,
        SBD.NUM_ORDERS_28DAYS_BEFORE,
    --    SBD.TOTAL_SUBTOTAL_7DAYS_BEFORE * 4.8 AS TOTAL_SUBTOTAL_PROJECTED_30DAYS_BEFORE,
    --    SBD.NUM_ORDERS_7DAYS_BEFORE * 4.8 AS NUM_ORDERS_PROJECTED_30DAYS_BEFORE,
    --    SBD.TOTAL_SUBTOTAL_7DAYS_BEFORE * 4.8 * SPC.TOTAL_SUBTOTAL_SCALER_SP AS TOTAL_SUBTOTAL_PROJECTED_SCALED_30DAYS_BEFORE,
    --    SBD.NUM_ORDERS_7DAYS_BEFORE * 4.8 * SPC.TOTAL_SUBTOTAL_SCALER_SP AS NUM_ORDERS_PROJECTED_SCALED_30DAYS_BEFORE,
        BSD.NUM_STORES_BUSINESS_BEFORE,
        BSD.NUM_ORDERS_BUSINESS_BEFORE,
        BSD.NUM_ORDERS_PER_STORE_BUSINESS_BEFORE,
        BSD.AVG_SUBTOTAL_PER_STORE_BUSINESS_BEFORE,
        BSD.TOTAL_SUBTOTAL_BUSINESS_BEFORE,
        BSD.TOTAL_SUBTOTAL_PER_ACTIVE_DATE_BUSINESS_BEFORE,
        SPC.TOTAL_SUBTOTAL_SCALER_SP,
        SPC.NUM_ORDERS_GROWTH_SP,
        SPC.NUM_STORES_GROWTH_SP,
        SPC.NUM_CONSUMERS_GROWTH_SP,
        SPC.PCT_PARTNER_CHANGE_SP,
        SPC.AVG_FEE_CHANGE_SP,
        SBD.AVG_COMMISSION_RATE_7DAYS_BEFORE,
        SBD.MEDIAN_COMMISSION_RATE_7DAYS_BEFORE,
        COALESCE(
            CAST(SBD.NUM_PARTNER_DELIVS_7DAYS_BEFORE as FLOAT) / NULLIF(CAST(SBD.NUM_ORDERS_7DAYS_BEFORE as FLOAT), 0),
            0
            ) AS PCT_PARTNER_DELIVS_7DAYS_BEFORE,
    sr.NUM_REQUESTS_L84DAYS,
    sr.NUM_REQUESTS_L28DAYS,
    sr.NUM_REQUESTS_ALL_TIME,
    COALESCE(CAST(SR.NUM_REQUESTS_ALL_TIME AS float)/NULLIF(SR.DAYS_BTW_FIRST_AND_LAST_REQUEST,0),0) AS num_request_per_day,
    sr.AVG_SUBTOTAL_FROM_CONSUMER_REQUEST,
    SR.NUM_DELIVERY_TO_REQUESTS_RATIO,
    coalesce(sc.avg_search_count_84_days_before,0) as avg_search_count_84_days_before, 
    EXTRACT(MONTH FROM S_0.ACTIVATION_DATE) AS REACTIVATION_MONTH,
    EXTRACT(YEAR FROM S_0.ACTIVATION_DATE) AS REACTIVATION_YEAR,
    EXTRACT(MONTH FROM S_0.MOST_RECENT_DEACTIVATION_TIMESTAMP) AS DEACTIVATION_MONTH,
    EXTRACT(YEAR FROM S_0.MOST_RECENT_DEACTIVATION_TIMESTAMP) AS DEACTIVATION_YEAR,
        S_0.MERCHANT_TYPE_PROD,
        '{{test_date}}' AS create_date
    FROM
        mx_DEACTIVATED_NONPARTNER_COMBINED AS S_0
    left outer join mx_STORES_NONPARTNER_INFO sni on S_0.NIMDA_STORE_ID = SNI.STORE_ID
    left outer join mx_search_count sc on s_0.match_id=sc.store_id
    LEFT OUTER JOIN mx_STORE_STATS_BEFORE_DEACTIVATION AS SBD ON S_0.NIMDA_STORE_ID = SBD.NIMDA_STORE_ID AND S_0.MOST_RECENT_DEACTIVATION_TIMESTAMP = SBD.MOST_RECENT_DEACTIVATION_TIMESTAMP
        LEFT OUTER JOIN mx_BUSINESS_STATS_BEFORE_REACTIVATION AS BSD ON S_0.BUSINESS_ID = BSD.BUSINESS_ID AND S_0.activation_date = BSD.activation_date
    LEFT OUTER JOIN mx_SP_STATS_COMBINED AS SPC ON SPC.NIMDA_STORE_ID = S_0.NIMDA_STORE_ID AND SPC.STARTING_POINT_ID = S_0.STARTING_POINT_ID AND S_0.activation_date = SPC.activation_date
        LEFT OUTER JOIN geo_intelligence.public.maindb_starting_point AS SP ON SP.ID = S_0.STARTING_POINT_ID
    LEFT OUTER JOIN mx_store_requests AS sr ON s_0.match_id=sr.match_id
    LEFT OUTER JOIN PUBLIC.fact_selection_intel_mx_raw AS RX ON rx.VENDOR_1_ID = S_0.VENDOR_1_ID
    WHERE 1 <> 0 
            AND NUM_DAYS_ACTIVE_BEFORE > 0 
      AND datediff('DAY', S_0.MOST_RECENT_DEACTIVATION_TIMESTAMP, S_0.ACTIVATION_DATE)<=364
    --      AND (NUM_DAYS_SINCE_REACTIVATION > 148)
;


"""

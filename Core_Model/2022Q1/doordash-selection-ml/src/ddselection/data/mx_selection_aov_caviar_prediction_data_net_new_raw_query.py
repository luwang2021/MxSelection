from ddselection.data.query_config import *

query_aov_nn = f"""
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

, mx_activated_after AS (
  select
      match_id AS ID
      ,store_activation_date::date AS activation_date
      ,starting_point_id AS sp_id
      ,submarket_id AS sub_id
      ,vendor_1_id
      ,RESTAURANT_NAME
  from
      {NN_TRAIN_5MONTH_TABLE_NAME}
  WHERE
      store_activation_date >='{{test_date}}'
      AND create_date IN (select max(create_date) from {NN_TRAIN_5MONTH_TABLE_NAME})
)

-- PUBLIC.FACT_SELECTION_INTEL_MX_RAW is the current snapshot
, mx_NET_NEW_MERCHANTS AS (
    SELECT
      ID match_id
      ,RESTAURANT_NAME store_name
      ,vendor_1_id
      ,sp_id AS STARTING_POINT_ID
      ,sub_id AS submarket_id
    FROM
      mx_filtered_mx_db_w_fraudulent AS mx
    WHERE
      (is_active = FALSE or is_active is null)
      OR (is_partner=false or is_partner is null)
-- TOFIX why need this dependence?
      and match_ID not IN (
        SELECT DISTINCT match_ID FROM PUBLIC.FACT_SELECTION_INTEL_DEACTIVATED_ACTIVE_PREDICTION_INPUTS_new_mx_db
          where match_id is not null
      )

    UNION

    SELECT
        ID match_id
        ,RESTAURANT_NAME store_name
        ,vendor_1_id
        ,sp_id AS STARTING_POINT_ID
        ,sub_id AS submarket_id
    FROM
        mx_activated_after
)

, mx_STORE_REQUESTS AS (
    SELECT
        SA_0.MATCH_ID,
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
        mx_NET_NEW_MERCHANTS SA_0
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
     GROUP BY 1
)


-- compute the starting point statistics
-- SP_DELIVERY_PARTNER_PERCENT: the percentage of partner deliveries in each starting point.
-- SP_SALES_PER_Partner: average sales per partner store for each starting point.
, mx_SP_STATS AS (
    SELECT
        DD_1.STORE_STARTING_POINT_ID as STARTING_POINT_ID,
        -- dateadd('DAY', -28, TO_TIMESTAMP_NTZ(LOCALTIMESTAMP)) AS MAX_FEATURE_DATE,
        SUM(DD_1.SUBTOTAL / 100.0) AS SP_TOTAL_SALES,
        COUNT(DD_1.DELIVERY_ID) AS SP_TOTAL_ORDERS,
        COALESCE(SUM(DD_1.SUBTOTAL / 100.0) / NULLIF(COUNT(DD_1.DELIVERY_ID), 0), 0) AS SP_AOV,
        COUNT(DISTINCT DD_1.STORE_ID) AS SP_LIVE_STORES,
        COUNT(DISTINCT DD_1.CREATOR_ID) AS SP_NUM_CONSUMERS,
        COALESCE(
        SUM(CASE
          WHEN DD_1.IS_PARTNER = 1 THEN 1.0
          ELSE 0.0
          END) / NULLIF(COUNT(DD_1.DELIVERY_ID), 0),
          0) AS SP_DELIVERY_PARTNER_PERCENT,
      COALESCE(
        COUNT(
          DISTINCT
          CASE
          WHEN DD_1.IS_PARTNER = 1
          THEN DD_1.STORE_ID
          ELSE NULL
          END) / NULLIF(COUNT(DISTINCT DD_1.STORE_ID), 0),
          0) AS SP_STORE_PARTNER_PERCENT,
      COALESCE(
        SUM(
          CASE
          WHEN DD_1.IS_PARTNER = 1
            THEN DD_1.SUBTOTAL / 100.0
          ELSE NULL
          end ) / NULLIF(COUNT(
                          distinct(
                            case
                            when DD_1.IS_PARTNER = 1 then DD_1.STORE_ID
                            else null
                            end)
                          ),
              0),
        0) AS SP_SALES_PER_partner
    FROM
        PUBLIC.DIMENSION_DELIVERIES AS DD_1
    WHERE 1 
        AND DD_1.ACTIVE_DATE BETWEEN dateadd('day', -(28+1), '{{test_date}}') AND dateadd('DAY', -1, '{{test_date}}')
        AND DD_1.IS_FILTERED_CORE = 1
    GROUP BY 1
)

-- calculate the stats for each submarket
-- Note, SUBMARKET_LIVE_STORES is defined as the number of stores that have deliveries in the past month.
-- TODO in the future, it might be worth using the average per store instead of the total of the entire submarket, since the prediction is at the store level
, mx_SUBMARKET_STATS AS (
    SELECT
        DD_2.SUBMARKET_ID,
        -- MFD_0.MAX_FEATURE_DATE,
        SUM(DD_2.SUBTOTAL / 100.0) AS SUBMARKET_TOTAL_SALES,
        COUNT(DD_2.DELIVERY_ID) AS SUBMARKET_TOTAL_ORDERS,
        COALESCE(SUM(DD_2.SUBTOTAL / 100.0) /  NULLIF(COUNT(DD_2.DELIVERY_ID),0), 0) AS SUBMARKET_AOV,
        COUNT(DISTINCT DD_2.STORE_ID) AS SUBMARKET_LIVE_STORES,
        COUNT(DISTINCT DD_2.CREATOR_ID) AS SUBMARKET_NUM_CONSUMERS,
        COALESCE(
      SUM(CASE
        WHEN DD_2.IS_PARTNER IS NOT DISTINCT FROM true THEN 1.0
        ELSE 0.0
        END) / NULLIF(COUNT(DD_2.DELIVERY_ID), 0),
        0) AS SUBMARKET_DELIVERY_PARTNER_PERCENT,
    COALESCE(
      COUNT(DISTINCT
        CASE
        WHEN DD_2.IS_PARTNER = 1 THEN DD_2.STORE_ID
        ELSE NULL
        END) / CAST(NULLIF(COUNT(DISTINCT DD_2.STORE_ID), 0) as FLOAT),
          0) AS SUBMARKET_STORE_PARTNER_PERCENT,
    COALESCE(
      SUM(
        CASE
        WHEN DD_2.IS_PARTNER = 1
          THEN DD_2.SUBTOTAL / 100.0
        ELSE NULL
        end ) / NULLIF(COUNT(
          distinct(
            case
            when DD_2.IS_PARTNER = 1 then DD_2.STORE_ID
            else null
            end)
        ),
        0),
        0) AS SUBMARKET_SALES_PER_partner
      from  PUBLIC.DIMENSION_DELIVERIES AS DD_2
      where 1
        AND DD_2.ACTIVE_DATE BETWEEN dateadd('day', -(28+1), '{{test_date}}') AND dateadd('DAY', -1, '{{test_date}}')
        AND DD_2.IS_FILTERED_CORE = 1
    GROUP BY
      1
)

-- how old is a submarket by looking at the first delivery in each submarket
, mx_SUBMARKET_AGE AS (
  SELECT
    SUBMARKET_ID,
    datediff('DAY', CAST(MIN(ACTIVE_DATE) as TIMESTAMP), dateadd('DAY', -1, dateadd('DAY', -1, '{{test_date}}'))) AS SUBMARKET_DAYS_SINCE_FIRST_DELIVERY
  FROM
  PUBLIC.DIMENSION_DELIVERIES 
  WHERE IS_FILTERED_CORE = 1
  GROUP BY SUBMARKET_ID
)


, mx_SEARCH_COUNT AS (
  SELECT
        store_id,
        COALESCE(avg(search_count), 0) as avg_search_count_84_days_before
  FROM
  (
      SELECT
          STORE_ID,
          SUM(QUERY_COUNT/STORE_COUNT) SEARCH_COUNT
      FROM
          luwang.FACT_STORE_SEARCH_COUNTS_TFIDF_MATCH_NEW
      WHERE
          SEARCH_DATE BETWEEN DATEADD('DAYS', -(28*3+2), '{{test_date}}') AND DATEADD('DAYS', -2, '{{test_date}}') 
      GROUP BY
          STORE_ID, SEARCH_DATE
  )
  GROUP BY store_id
)

SELECT
    dnn.MATCH_ID
  ,dnn.STORE_NAME
  ,dnn.VENDOR_1_ID
  ,dnn.STARTING_POINT_ID
  ,dnn.SUBMARKET_ID
  ,case
     when dnn.VENDOR_1_ID IS NOT null  then  'requestable'
     ELSE 'not_requestable'
     end requestable
    ,EXTRACT(month from dateadd('DAY', -1, '{{test_date}}')) AS ACTIVATION_MONTH
    ,EXTRACT(year from dateadd('DAY', -1, '{{test_date}}')) AS ACTIVATION_YEAR
    ,sr.FIRST_REQUEST_REQUESTED_AT
    ,sr.LAST_REQUEST_REQUESTED_AT
  ,sr.NUM_REQUESTS_L84DAYS
  ,sr.NUM_REQUESTS_L28DAYS
  ,sr.NUM_REQUESTS_ALL_TIME
  ,COALESCE(CAST(SR.NUM_REQUESTS_ALL_TIME AS float)/NULLIF(SR.DAYS_BTW_FIRST_AND_LAST_REQUEST,0),0) AS num_request_per_day
  ,SR.AVG_SUBTOTAL_FROM_CONSUMER_REQUEST
  ,SR.NUM_DELIVERY_TO_REQUESTS_RATIO
  ,sps.SP_TOTAL_SALES
  ,sps.SP_TOTAL_ORDERS
    ,sps.SP_AOV
  ,sps.SP_LIVE_STORES
  ,sps.SP_NUM_CONSUMERS
  ,sps.SP_DELIVERY_PARTNER_PERCENT
  ,sps.SP_STORE_PARTNER_PERCENT
  ,sps.SP_SALES_PER_PARTNER
  ,sbs.SUBMARKET_TOTAL_SALES
  ,sbs.SUBMARKET_TOTAL_ORDERS
    ,sbs.SUBMARKET_AOV
  ,sbs.SUBMARKET_LIVE_STORES
  ,sbs.SUBMARKET_NUM_CONSUMERS
  ,sbs.SUBMARKET_DELIVERY_PARTNER_PERCENT
  ,sbs.SUBMARKET_STORE_PARTNER_PERCENT
  ,sbs.SUBMARKET_SALES_PER_PARTNER
    ,coalesce(sc.AVG_SEARCH_COUNT_84_DAYS_BEFORE, 0) as AVG_SEARCH_COUNT_84_DAYS_BEFORE
    ,'{{test_date}}' as create_date
FROM
  mx_NET_NEW_MERCHANTS dnn
left join mx_SEARCH_COUNT sc
on dnn.match_id=sc.store_id
LEFT JOIN
  mx_store_requests sr
ON dnn.match_id = sr.match_id
LEFT JOIN
  mx_sp_stats sps
ON dnn.starting_point_id=sps.starting_point_id
LEFT JOIN
  mx_submarket_stats sbs
ON dnn.submarket_id=sbs.submarket_id
"""

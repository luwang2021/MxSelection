from ddselection.data.query_config import *

query_nn_train = f"""
With STORE_PROXY_ACTIVATION AS (
    SELECT
        *
    FROM
        {NN_TRAIN_5MONTH_TABLE_NAME}
    WHERE
        create_date in (select max(create_date) from {NN_TRAIN_5MONTH_TABLE_NAME})
)
-- calculate the number of requests for each store -- 
, STORE_REQUESTS AS (
    SELECT
        SA_0.MATCH_ID,
		SA_0.store_ID nimda_store_ID,
        SA_0.STORE_ACTIVATION_DATE,
        MIN(R.REQUESTED_AT) AS FIRST_REQUEST_REQUESTED_AT,
        MAX(R.REQUESTED_AT) AS LAST_REQUEST_REQUESTED_AT,
        DATEDIFF('DAY', MIN(R.REQUESTED_AT), MAX(R.REQUESTED_AT)) AS DAYS_BTW_FIRST_AND_LAST_REQUEST,
        COUNT(DISTINCT r.consumer_id) AS NUM_REQUESTS_ALL_TIME,
        TRUNC(AVG(CAST(DD_0.SUBTOTAL as FLOAT))) AS AVG_SUBTOTAL_FROM_CONSUMER_REQUEST,
        COALESCE(
			CAST(COUNT(DISTINCT DD_0.DELIVERY_ID) as FLOAT) / CAST(NULLIF(COUNT(DISTINCT r.consumer_id), 0) as FLOAT),
			0) AS NUM_DELIVERY_TO_REQUESTS_RATIO,
        COUNT(
          DISTINCT
          CASE
          WHEN R.REQUESTED_AT between dateadd('DAY', -84, SA_0.STORE_ACTIVATION_DATE) AND dateadd('DAY', -1, SA_0.STORE_ACTIVATION_DATE) THEN r.consumer_id 
          ELSE NULL
          END) AS NUM_REQUESTS_L84DAYS,
        COUNT(
          DISTINCT
          CASE
          WHEN R.REQUESTED_AT between dateadd('DAY', -28, SA_0.STORE_ACTIVATION_DATE) AND dateadd('DAY', -1, SA_0.STORE_ACTIVATION_DATE) THEN r.consumer_id 
          ELSE NULL
          END) AS NUM_REQUESTS_L28DAYS
    FROM
    	STORE_PROXY_ACTIVATION SA_0
    LEFT OUTER JOIN 
        PUBLIC.fact_selection_intel_mx_external_store_requests r
    ON 
        sa_0.match_id = r.mx_id
	    AND R.REQUESTED_AT<SA_0.STORE_ACTIVATION_DATE
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_0
    ON 
        R.CONSUMER_ID = DD_0.CREATOR_ID
        AND R.REQUESTED_AT > DD_0.CREATED_AT
        AND DD_0.IS_FILTERED_CORE = 1
        AND DD_0.SUBTOTAL > 0
    GROUP BY 
        1,2,3
)

--  stats of each starting point. --48,991
, SP_STATS AS (
    SELECT
        MFD.STARTING_POINT_ID,
        MFD.STORE_ACTIVATION_DATE,
        SUM(DD_1.SUBTOTAL / 100.0) AS SP_TOTAL_SALES,
        COUNT(DD_1.DELIVERY_ID) AS SP_TOTAL_ORDERS,
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
			  END) / CAST(NULLIF(COUNT(DISTINCT DD_1.STORE_ID), 0) as FLOAT),
			  0) AS SP_STORE_PARTNER_PERCENT,
        COALESCE(
            SUM(
                CASE
                    WHEN DD_1.IS_PARTNER = 1 THEN DD_1.SUBTOTAL / 100.0
                    ELSE NULL
                END
            ) / NULLIF(
                  COUNT(distinct(
                          case
                          when DD_1.IS_PARTNER = 1 then DD_1.STORE_ID
                          else null
                          end
                          )
                        ), 0
            ), 0) AS SP_SALES_PER_partner
    FROM
        (
          SELECT 
              DISTINCT STORE_PROXY_ACTIVATION.STARTING_POINT_ID,
              STORE_PROXY_ACTIVATION.STORE_ACTIVATION_DATE
          FROM
              STORE_PROXY_ACTIVATION
        ) AS MFD
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_1 
    ON 
        DD_1.STORE_STARTING_POINT_ID = MFD.STARTING_POINT_ID
        AND DD_1.ACTIVE_DATE <= MFD.STORE_ACTIVATION_DATE
        AND DD_1.ACTIVE_DATE > dateadd('DAY', -28, MFD.STORE_ACTIVATION_DATE)
        AND DD_1.IS_FILTERED_CORE = 1
    GROUP BY
        1, 2
)

, SUBMARKET_STATS AS (
    SELECT
        MFD_0.SUBMARKET_ID,
        MFD_0.STORE_ACTIVATION_DATE,
        SUM(DD_2.SUBTOTAL / 100.0) AS SUBMARKET_TOTAL_SALES,
        COUNT(DD_2.DELIVERY_ID) AS SUBMARKET_TOTAL_ORDERS,
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
                END 
                ) / NULLIF(COUNT(
              distinct(
                  case
                  when DD_2.IS_PARTNER = 1 then DD_2.STORE_ID
                  else null
                  end)
            ), 
            0),
            0) AS SUBMARKET_SALES_PER_partner
    FROM
        (
          SELECT 
              DISTINCT STORE_PROXY_ACTIVATION.SUBMARKET_ID,
              STORE_PROXY_ACTIVATION.STORE_ACTIVATION_DATE
          FROM
              STORE_PROXY_ACTIVATION
        ) AS MFD_0
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_2
    ON 
        DD_2.SUBMARKET_ID = MFD_0.SUBMARKET_ID
        AND DD_2.ACTIVE_DATE <= MFD_0.STORE_ACTIVATION_DATE 
        AND DD_2.ACTIVE_DATE > dateadd('DAY', -28, MFD_0.STORE_ACTIVATION_DATE)
        AND DD_2.IS_FILTERED_CORE = 1
    GROUP BY
        1, 2
)

, SUBMARKET_AGE AS (
    SELECT
        SS_1.SUBMARKET_ID,
        SS_1.STORE_ACTIVATION_DATE,
        datediff('DAY', CAST(MIN(DD_3.ACTIVE_DATE) as TIMESTAMP), SS_1.STORE_ACTIVATION_DATE) AS SUBMARKET_DAYS_SINCE_FIRST_DELIVERY
    FROM
        SUBMARKET_STATS AS SS_1
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_3
    ON 
        SS_1.SUBMARKET_ID = DD_3.SUBMARKET_ID
        AND DD_3.IS_FILTERED_CORE = 1
    GROUP BY 
        1, 2
)

, STORE_TARGET_SALES AS (
    SELECT
          FDSC_0.store_id AS NIMDA_STORE_ID
        , FDSC_0.RESTAURANT_NAME
        , FDSC_0.MATCH_ID
        , datediff(month, FDSC_0.STORE_ACTIVATION_DATE, DD_8.ACTIVE_DATE) AS age_month
        , MAX(date(FDSC_0.STORE_ACTIVATION_DATE)) AS STORE_ACTIVATION_DATE
        , MIN(DD_8.ACTIVE_DATE) AS MIN_ACTIVE_DATE
        , SUM(DD_8.SUBTOTAL) / 100.0 AS TOTAL_SUBTOTAL_MONTH
    FROM
        STORE_PROXY_ACTIVATION AS FDSC_0
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_8
    ON 
        FDSC_0.STORE_ID = DD_8.STORE_ID
        AND DD_8.IS_FILTERED_CORE = 1
        AND DD_8.ACTIVE_DATE BETWEEN '{{test_date}}' AND '{{cur_date}}'
        AND FDSC_0.STORE_ACTIVATION_DATE >= '2018-10-01'
    GROUP BY 
        1, 2, 3, 4
    HAVING 
        age_month >= 1 
)

SELECT 
      fdsc.match_id
    , fdsc.store_id
    , fdsc.restaurant_name
    , fdsc.starting_point_id
    , fdsc.submarket_id
    , sts.age_month
    , sts.store_activation_date
    , sts.min_active_date
    , sts.total_subtotal_month
    , CASE
        WHEN MX.VENDOR_1_ID IS NOT null THEN 'requestable'
        ELSE 'not_requestable' 
	  END AS requestable
    , fdsc.first_partner_order_date 
    , EXTRACT(MONTH FROM fdsc.store_activation_date) AS activation_month
    , EXTRACT(YEAR FROM fdsc.store_activation_date) AS activation_year
    -- request features
    , sr.num_requests_l84days 
    , sr.num_requests_l28days 
    , sr.num_requests_all_time
    , COALESCE(CAST(sr.num_requests_all_time AS float)/NULLIF(sr.days_btw_first_and_last_request,0),0) AS num_request_per_day
    , sr.avg_subtotal_from_consumer_request 
    , sr.num_delivery_to_requests_ratio 
    , COALESCE(sr.num_requests_l28days * 1.0 / NULLIF(ds_0.sp_num_consumers, 0), 0) AS requests_to_sp_consumers_ratio
    , COALESCE(sr.num_requests_l28days * 1.0 / NULLIF(ds_0.sp_total_sales, 0), 0) AS requests_to_sp_sales_ratio 
    -- SP / submarket features
    , ds_0.sp_total_sales
    , ds_0.sp_total_orders 
    , ds_0.sp_live_stores 
    , ds_0.sp_num_consumers 
    , ds_0.sp_delivery_partner_percent 
    , ds_0.sp_store_partner_percent 
    , ds_0.sp_sales_per_partner 
    , sa_1.submarket_days_since_first_delivery 
    , ss_0.submarket_total_sales 
    , ss_0.submarket_total_orders 
    , ss_0.submarket_live_stores 
    , ss_0.submarket_num_consumers 
    , ss_0.submarket_delivery_partner_percent 
    , ss_0.submarket_store_partner_percent 
    , ss_0.submarket_sales_per_partner 
    , '{{cur_date}}' AS CREATE_DATE
FROM 
    store_target_sales AS sts 
LEFT JOIN 
    store_proxy_activation AS fdsc
ON 
    sts.match_id = fdsc.match_id 
LEFT JOIN 
	public.fact_selection_intel_mx_raw AS mx  
ON 
    fdsc.match_id=mx.id
LEFT JOIN
    sp_stats AS ds_0 
ON 
    fdsc.starting_point_id = ds_0.starting_point_id
    AND ds_0.store_activation_date = fdsc.store_activation_date
LEFT JOIN
    submarket_stats AS ss_0 
ON 
    ss_0.submarket_id = fdsc.submarket_id 
    AND ss_0.store_activation_date = fdsc.store_activation_date
LEFT JOIN
    submarket_age AS sa_1 
ON 
    fdsc.submarket_id = sa_1.submarket_id 
    AND sa_1.store_activation_date = fdsc.store_activation_date
LEFT JOIN 
	store_requests AS sr 
ON 
    fdsc.match_id = sr.match_id 
WHERE 1
  -- remove fradulent stores   
    AND(fdsc.store_id NOT IN  (
        SELECT store_id FROM  PUBLIC.DIMENSION_STORE WHERE LAST_DEACTIVATION_REASON = 'Fraudulent Store'
      ) or fdsc.store_id is null)
;
"""
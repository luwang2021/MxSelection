from ddselection.data.query_config import *

query_da_acnp_train = f"""

With STORES_MOST_RECENT_DEACTIVATION AS (
    SELECT
        *
    FROM
        {DANP_TRAIN_5MONTH_TABLE_NAME}
    WHERE
        create_date in (select max(create_date) from {DANP_TRAIN_5MONTH_TABLE_NAME})
)
--  statistics at store level before deactivation
, STORE_STATS_BEFORE_DEACTIVATION AS (
    SELECT
        MRD.STORE_ID,
        MRD.MATCH_ID,
        MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP,
        mrd.reactivation_timestamp,
        COUNT(DISTINCT DD_0.DELIVERY_ID) AS NUM_ORDERS_BEFORE,
        COUNT(DISTINCT DD_0.ACTIVE_DATE) AS NUM_DAYS_ACTIVE_BEFORE,
        AVG(DD_0.FEE / 100.0) AS AVG_FEE_BEFORE,
        AVG(DD_0.SUBTOTAL / 100.0) AS AVG_SUBTOTAL_BEFORE,
        SUM(DD_0.SUBTOTAL / 100.0) AS TOTAL_SUBTOTAL_BEFORE,
        MEDIAN(DD_0.SUBTOTAL / 100.0) AS MEDIAN_SUBTOTAL_BEFORE,
        MAX(DD_0.SUBTOTAL / 100.0) AS MAX_SUBTOTAL_BEFORE,
        SUM(CASE
            WHEN DD_0.IS_FIRST_ORDERCART IS NOT DISTINCT FROM true THEN 1
            ELSE 0
            END) AS NUM_FIRST_ORDERCARTS_BEFORE,
        SUM(CASE
            WHEN DD_0.IS_PARTNER IS NOT DISTINCT FROM true THEN 1.0
            ELSE 0.0
            END) / NULLIF(COUNT(1), 0) AS PCT_PARTNER_DELIVS_BEFORE,
        COUNT(DISTINCT
            CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.ACTIVE_DATE
            ELSE NULL
            END) AS NUM_DAYS_ACTIVE_7DAYS_BEFORE,
        AVG(CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.SUBTOTAL / 100.0
            ELSE NULL
            END) AS AVG_SUBTOTAL_7DAYS_BEFORE,
        COUNT(DISTINCT
            CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.DELIVERY_ID
            ELSE NULL
            END) AS NUM_ORDERS_7DAYS_BEFORE,
        AVG(CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.FEE / 100.0
            ELSE NULL
            END) AS AVG_FEE_7DAYS_BEFORE,
        MEDIAN(CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.SUBTOTAL / 100.0
            ELSE NULL
            END) AS MEDIAN_SUBTOTAL_7DAYS_BEFORE,
        SUM(CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.SUBTOTAL
            ELSE NULL
            END) AS TOTAL_SUBTOTAL_7DAYS_BEFORE,
        AVG(CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.COMMISSION_RATE
            ELSE NULL
            END) AS AVG_COMMISSION_RATE_7DAYS_BEFORE,
        MEDIAN(CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.COMMISSION_RATE
            ELSE NULL
            END) AS MEDIAN_COMMISSION_RATE_7DAYS_BEFORE,
        COUNT(DISTINCT
            CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) AND DD_0.IS_FIRST_ORDERCART IS NOT DISTINCT FROM true THEN DD_0.DELIVERY_ID
            ELSE NULL
            END) AS NUM_FIRST_ORDERCARTS_7DAYS_BEFORE,
        COUNT(DISTINCT
            CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -9, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) AND DD_0.IS_PARTNER IS NOT DISTINCT FROM true THEN DD_0.DELIVERY_ID
            ELSE NULL
            END) AS NUM_PARTNER_DELIVS_7DAYS_BEFORE,
        COUNT(DISTINCT
            CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.ACTIVE_DATE
            ELSE NULL
            END) AS NUM_DAYS_ACTIVE_28DAYS_BEFORE,
        COUNT(DISTINCT
            CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.DELIVERY_ID
            ELSE NULL
            END) AS NUM_ORDERS_28DAYS_BEFORE,
        AVG(CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.SUBTOTAL / 100.0
            ELSE NULL
            END) AS AVG_SUBTOTAL_28DAYS_BEFORE,
        SUM(CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.SUBTOTAL/100.0
            ELSE NULL
            END) AS TOTAL_SUBTOTAL_28DAYS_BEFORE,
        AVG(CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.FEE / 100.0
            ELSE NULL
            END) AS AVG_FEE_28DAYS_BEFORE,
        MEDIAN(CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.SUBTOTAL / 100.0
            ELSE NULL
            END) AS MEDIAN_SUBTOTAL_28DAYS_BEFORE,
        AVG(CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.COMMISSION_RATE
            ELSE NULL
            END) AS AVG_COMMISSION_RATE_28DAYS_BEFORE,
        MEDIAN(CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) THEN DD_0.COMMISSION_RATE
            ELSE NULL
            END) AS MEDIAN_COMMISSION_RATE_28DAYS_BEFORE,
        COUNT(DISTINCT
            CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) AND DD_0.IS_FIRST_ORDERCART IS NOT DISTINCT FROM true THEN DD_0.DELIVERY_ID
            ELSE NULL
            END) AS NUM_FIRST_ORDERCARTS_28DAYS_BEFORE,
        COUNT(DISTINCT
            CASE
            WHEN (DD_0.ACTIVE_DATE between dateadd('DAY', -30, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) AND DD_0.IS_PARTNER IS NOT DISTINCT FROM true THEN DD_0.DELIVERY_ID
            ELSE NULL
            END) AS NUM_PARTNER_DELIVS_28DAYS_BEFORE
    FROM
        STORES_MOST_RECENT_DEACTIVATION AS MRD
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_0
          ON MRD.STORE_ID = DD_0.STORE_ID
          AND DD_0.ACTIVE_DATE between dateadd('DAY', -180, MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP) and MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP
         -- AND DD_0.ACTIVE_DATE <= MRD.MOST_RECENT_DEACTIVATION_TIMESTAMP
          AND DD_0.IS_FILTERED_CORE = TRUE
    GROUP BY (1, 2, 3, 4)
)
-- stats of the business before reactivation
, BUSINESS_STATS_BEFORE_REACTIVATION AS (
    SELECT
        MRD_0.*,
        COUNT(DISTINCT DD_1.STORE_ID) AS NUM_STORES_BUSINESS_BEFORE,
        COALESCE(COUNT(DISTINCT DD_1.DELIVERY_ID), 0) AS NUM_ORDERS_BUSINESS_BEFORE,
        CAST(COUNT(DISTINCT DD_1.DELIVERY_ID) as FLOAT) / CAST(NULLIF(COUNT(DISTINCT DD_1.STORE_ID), 0) as FLOAT) AS NUM_ORDERS_PER_STORE_BUSINESS_BEFORE,
        COALESCE(SUM(DD_1.SUBTOTAL / 100.0) / NULLIF(COUNT(DISTINCT DD_1.STORE_ID), 0), 0) AS AVG_SUBTOTAL_PER_STORE_BUSINESS_BEFORE,
        SUM(DD_1.SUBTOTAL / 100.0) AS TOTAL_SUBTOTAL_BUSINESS_BEFORE,
        COALESCE(SUM(DD_1.SUBTOTAL / 100.0) / NULLIF(COUNT(DISTINCT DD_1.ACTIVE_DATE), 0), 0) AS TOTAL_SUBTOTAL_PER_ACTIVE_DATE_BUSINESS_BEFORE
    FROM
        (
          SELECT DISTINCT
              STORES_MOST_RECENT_DEACTIVATION.BUSINESS_ID,
              STORES_MOST_RECENT_DEACTIVATION.REACTIVATION_TIMESTAMP
             FROM
              STORES_MOST_RECENT_DEACTIVATION
        ) AS MRD_0
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_1
    ON
        MRD_0.BUSINESS_ID = DD_1.BUSINESS_ID
        --AND DD_1.ACTIVE_DATE <= MRD_0.REACTIVATION_TIMESTAMP
        AND DD_1.ACTIVE_DATE between dateadd('DAY', -180, MRD_0.REACTIVATION_TIMESTAMP) and MRD_0.REACTIVATION_TIMESTAMP
        AND DD_1.IS_FILTERED_CORE IS NOT DISTINCT FROM true
    GROUP BY
        1, 2
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
          SR_0.store_id
        --, FDSC_0.RESTAURANT_NAME
        --, FDSC_0.MATCH_ID
        , SR_0.MOST_RECENT_DEACTIVATION_TIMESTAMP
        , datediff(month, DATE_TRUNC('DAY',SR_0.REACTIVATION_TIMESTAMP), DD_8.ACTIVE_DATE) AS age_month
        , MAX(date(SR_0.REACTIVATION_TIMESTAMP)) AS REACTIVATION_TIMESTAMP
        , MIN(DD_8.ACTIVE_DATE) AS MIN_ACTIVE_DATE
        , SUM(DD_8.SUBTOTAL) / 100.0 AS TOTAL_SUBTOTAL_MONTH
    FROM
        STORES_MOST_RECENT_DEACTIVATION AS SR_0
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_8
    ON
        SR_0.STORE_ID = DD_8.STORE_ID
        AND DD_8.IS_FILTERED_CORE = 1
        -- AND DD_8.ACTIVE_DATE BETWEEN SR_0.STORE_ACTIVATION_DATE AND '{{cur_date}}'
        AND DD_8.ACTIVE_DATE BETWEEN '{{test_date}}' AND '{{cur_date}}'
        AND SR_0.REACTIVATION_TIMESTAMP >= '2018-10-01'
    GROUP BY
        1, 2, 3
    HAVING
        age_month >= 1
)


--  stats of starting point 7 days before deactivation.
, SP_STATS_BEFORE AS (
    SELECT
        SR_1.STORE_ID,
        SR_1.STARTING_POINT_ID,
        SR_1.MOST_RECENT_DEACTIVATION_TIMESTAMP,
        COUNT(DISTINCT DD_4.DELIVERY_ID) AS NUM_ORDERS_SP_7DAYS_BEFORE,
        SUM(DD_4.SUBTOTAL / 100.0) AS TOTAL_SUBTOTAL_SP_7DAYS_BEFORE,
        AVG(DD_4.FEE / 100.0) AS AVG_FEE_SP_7DAYS_BEFORE,
        SUM(CASE
          WHEN DD_4.IS_PARTNER IS NOT DISTINCT FROM true THEN 1
          ELSE 0
        END) * 1.0 / NULLIF(COUNT(DD_4.DELIVERY_ID), 0) AS PCT_PARTNER_SP_7DAYS_BEFORE,
        COUNT(DISTINCT DD_4.STORE_ID) AS NUM_STORES_SP_7DAYS_BEFORE,
        COUNT(DISTINCT DD_4.CREATOR_ID) AS NUM_CONSUMERS_SP_7DAYS_BEFORE
    FROM
        STORE_REACTIVATION AS SR_1
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_4
    ON
        SR_1.STARTING_POINT_ID = DD_4.STORE_STARTING_POINT_ID
        AND DD_4.ACTIVE_DATE <= SR_1.MOST_RECENT_DEACTIVATION_TIMESTAMP
        AND DD_4.ACTIVE_DATE > dateadd('DAY', -7, SR_1.MOST_RECENT_DEACTIVATION_TIMESTAMP)
        AND DD_4.IS_FILTERED_CORE IS NOT DISTINCT FROM true
    GROUP BY
        1, 2, 3
)

-- these are stats of starting points 7 days before reactivation. legacy misnomer
, SP_STATS_AFTER AS (
    SELECT
        SR_2.STORE_ID,
        SR_2.STARTING_POINT_ID,
        SR_2.REACTIVATION_TIMESTAMP,
        COUNT(DISTINCT DD_5.DELIVERY_ID) AS NUM_ORDERS_SP_7DAYS_AFTER,
        SUM(DD_5.SUBTOTAL / 100.0) AS TOTAL_SUBTOTAL_SP_7DAYS_AFTER,
        AVG(DD_5.FEE / 100.0) AS AVG_FEE_SP_7DAYS_AFTER,
        SUM(CASE
          WHEN DD_5.IS_PARTNER IS NOT DISTINCT FROM true THEN 1
          ELSE 0
        END) * COALESCE(1.0 / NULLIF(COUNT(DD_5.DELIVERY_ID), 0), 0) AS PCT_PARTNER_SP_7DAYS_AFTER,
        COUNT(DISTINCT DD_5.STORE_ID) AS NUM_STORES_SP_7DAYS_AFTER,
        COUNT(DISTINCT DD_5.CREATOR_ID) AS NUM_CONSUMERS_SP_7DAYS_AFTER
    FROM
        STORE_REACTIVATION AS SR_2
    LEFT OUTER JOIN
        PUBLIC.DIMENSION_DELIVERIES AS DD_5
    ON
        SR_2.STARTING_POINT_ID = DD_5.STORE_STARTING_POINT_ID
        AND DD_5.ACTIVE_DATE <= SR_2.REACTIVATION_TIMESTAMP
        AND DD_5.ACTIVE_DATE > dateadd('DAY', -7, SR_2.REACTIVATION_TIMESTAMP)
        AND DD_5.IS_FILTERED_CORE IS NOT DISTINCT FROM true
    GROUP BY 1, 2, 3
)

, SP_STATS_COMBINED AS (
    SELECT
        SPB.STORE_ID,
        SPB.STARTING_POINT_ID,
        SPB.MOST_RECENT_DEACTIVATION_TIMESTAMP,
        SPB.NUM_ORDERS_SP_7DAYS_BEFORE,
        SPB.TOTAL_SUBTOTAL_SP_7DAYS_BEFORE,
        SPB.NUM_STORES_SP_7DAYS_BEFORE,
        SPB.NUM_CONSUMERS_SP_7DAYS_BEFORE,
        SPB.PCT_PARTNER_SP_7DAYS_BEFORE,
        SPB.AVG_FEE_SP_7DAYS_BEFORE,
        SPA.REACTIVATION_TIMESTAMP,
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
        SP_STATS_BEFORE AS SPB
    JOIN
        SP_STATS_AFTER AS SPA
    ON
        SPB.STORE_ID = SPA.STORE_ID
        AND SPB.STARTING_POINT_ID = SPA.STARTING_POINT_ID
        AND SPB.MOST_RECENT_DEACTIVATION_TIMESTAMP < SPA.REACTIVATION_TIMESTAMP
)

, STORE_REQUESTS AS (
    SELECT
        SA_0.MATCH_ID,
        SA_0.store_ID nimda_store_ID,
        SA_0.REACTIVATION_TIMESTAMP,
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
          -- WHEN R.REQUESTED_AT >= dateadd('DAY', -84, SA_0.REACTIVATION_TIMESTAMP) THEN r.consumer_id
		      WHEN r.requested_at BETWEEN dateadd('day', -(28*3), SA_0.REACTIVATION_TIMESTAMP) AND dateadd('DAY', -1, SA_0.REACTIVATION_TIMESTAMP) THEN r.consumer_id
          ELSE NULL
--          NUM_REQUESTS_L14DAYS is a legacy misnomer, it is actually 7 days of requests.
          END) AS NUM_REQUESTS_L84DAYS,
        COUNT(
          DISTINCT
          CASE
          -- WHEN R.REQUESTED_AT >= dateadd('DAY', -28, SA_0.REACTIVATION_TIMESTAMP) THEN r.consumer_id
          WHEN r.requested_at BETWEEN dateadd('day', -28, SA_0.REACTIVATION_TIMESTAMP) AND dateadd('DAY', -1, SA_0.REACTIVATION_TIMESTAMP) THEN r.consumer_id
          ELSE NULL
          END) AS NUM_REQUESTS_L28DAYS
    FROM
        STORES_MOST_RECENT_DEACTIVATION SA_0
    LEFT OUTER JOIN
        public.fact_selection_intel_mx_external_store_requests r
    ON
        sa_0.match_id = r.mx_id
        AND R.REQUESTED_AT<SA_0.REACTIVATION_TIMESTAMP
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

SELECT
    SAR.STORE_ID,
    SAR.age_month,
    SAR.min_active_date,
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
    SAR.MOST_RECENT_DEACTIVATION_TIMESTAMP,
    SAR.REACTIVATION_TIMESTAMP,
--    SR.MOST_RECENT_DELIVERY_TIMESTAMP,
    S_0.MERCHANT_TYPE,
    SBD.NUM_ORDERS_BEFORE,
    SBD.NUM_DAYS_ACTIVE_BEFORE,
    SBD.AVG_FEE_BEFORE,
    SBD.AVG_FEE_28DAYS_BEFORE,
    SBD.AVG_SUBTOTAL_BEFORE,
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
    BSD.NUM_STORES_BUSINESS_BEFORE,
    BSD.NUM_ORDERS_BUSINESS_BEFORE,
    BSD.NUM_ORDERS_PER_STORE_BUSINESS_BEFORE,
    BSD.AVG_SUBTOTAL_PER_STORE_BUSINESS_BEFORE,
    BSD.TOTAL_SUBTOTAL_BUSINESS_BEFORE,
    BSD.TOTAL_SUBTOTAL_PER_ACTIVE_DATE_BUSINESS_BEFORE,
    S_0.REACTIVATION_MONTH,
    S_0.REACTIVATION_YEAR,
    S_0.DEACTIVATION_MONTH,
    S_0.DEACTIVATION_YEAR,
    S_0.NUM_DAYS_DEACTIVE,
    S_0.NUM_DAYS_SINCE_REACTIVATION,
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
    SAR.total_subtotal_month,
	r.NUM_REQUESTS_L84DAYS,
	r.NUM_REQUESTS_L28DAYS,
	r.NUM_REQUESTS_ALL_TIME,
	COALESCE(CAST(R.NUM_REQUESTS_ALL_TIME AS float)/NULLIF(R.DAYS_BTW_FIRST_AND_LAST_REQUEST,0),0) AS num_request_per_day,
	R.NUM_DELIVERY_TO_REQUESTS_RATIO,
	r.AVG_SUBTOTAL_FROM_CONSUMER_REQUEST,
    '{{cur_date}}' AS CREATE_DATE
FROM
    STORE_STATS_AFTER_REACTIVATION SAR
LEFT JOIN
    STORES_MOST_RECENT_DEACTIVATION AS S_0
ON
    S_0.STORE_ID = SAR.STORE_ID
    AND S_0.MOST_RECENT_DEACTIVATION_TIMESTAMP = SAR.MOST_RECENT_DEACTIVATION_TIMESTAMP
LEFT OUTER JOIN
    PUBLIC.fact_selection_intel_mx_raw mx
ON
    sar.store_id = mx.nimda_id
LEFT OUTER JOIN
    STORE_STATS_BEFORE_DEACTIVATION AS SBD
ON
    sar.STORE_ID = SBD.STORE_ID
    AND sar.MOST_RECENT_DEACTIVATION_TIMESTAMP = SBD.MOST_RECENT_DEACTIVATION_TIMESTAMP
LEFT OUTER JOIN
    BUSINESS_STATS_BEFORE_REACTIVATION AS BSD
ON
    S_0.BUSINESS_ID = BSD.BUSINESS_ID
    AND S_0.reactivation_timestamp = BSD.reactivation_timestamp
LEFT OUTER JOIN
    SP_STATS_COMBINED AS SPC
ON
    SPC.STORE_ID = sar.STORE_ID
    AND SPC.STARTING_POINT_ID = S_0.STARTING_POINT_ID
    AND S_0.reactivation_timestamp = SPC.reactivation_timestamp
LEFT OUTER JOIN
    geo_intelligence.PUBLIC.MAINDB_STARTING_POINT AS SP
ON
    SP.ID = S_0.STARTING_POINT_ID
LEFT OUTER JOIN store_requests AS r ON s_0.match_id=r.match_id
-- remove fradulent stores
WHERE
    (sar.store_id NOT IN  (
     SELECT store_id FROM  PUBLIC.DIMENSION_STORE WHERE LAST_DEACTIVATION_REASON = 'Fraudulent Store'
    ) or sar.store_id is null)
    AND NUM_DAYS_ACTIVE_BEFORE > 0
"""

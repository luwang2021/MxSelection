from ddselection.data.query_config import *

query_head = """
WITH empty AS (
    SELECT 0
)
"""

review_ratings_sales_query = f"""
, review_ratings_sales AS (
    SELECT
          match_id
        , nimda_id as store_id
        , avg(reviews_mes) as reviews_mes
        , avg(reviews_yelp) as reviews_yelp
        , avg(rating_mes) as ratings_mes
        , avg(rating_yelp) as rating_yelp
        , avg(rating_factus) as rating_factus
        , avg(sales_chdus) as sales_chdus
        , avg(stars_al) as stars_al
        , avg(reviews_al) as reviews_al
    FROM
        {NUM_FEATURE_TABLE_NAME}
    GROUP BY
        match_id, nimda_id
)
"""

cat_pool_query = f"""
, cat_from_vendor AS (
    SELECT
          match_id
        , nimda_id as store_id
        , max(cohort_encoded) as cohort
        , max(cohort) as cohort_raw
        , max(sales_chd_agg_encoded) as sales_chd_agg
        , max(years_in_business_encoded) as years_in_business
        , max(price_yelp_encoded) as price_yelp
        , max(price_mes_encoded) as price_mes
        , max(price_factus_encoded) as price_factus
        , max(price_al_encoded) as price_al
        , max(number_of_units_encoded) as number_of_units
        , max(dayparts_encoded) as dayparts
        , max(average_check_encoded) as average_check
        , max(average_check_amt_encoded) as average_check_amt
        , max(credit_score_rating_encoded) as credit_score_rating
    FROM
        {CAT_FEATURE_TABLE_NAME}
    GROUP BY
        match_id, nimda_id
)
"""

# old version, for training
search_feature_query_danp = f"""
WITH search_data AS (
    SELECT
        store_id,
        search_date,
        sum(query_count/store_count) search_count
    FROM
        luwang.fact_store_search_counts_tfidf_match_train_20180101_20220108 -- this needs to have a date partition for replication
    WHERE
        store_id IS NOT NULL
    GROUP BY
        store_id, search_date
)
, mx_activation AS (
    select
        match_id
        , max(reactivation_timestamp) as reactivation_timestamp
    from
        {DANP_TRAIN_5MONTH_TABLE_NAME}
    where
        create_date IN (select max(create_date) from {DANP_TRAIN_5MONTH_TABLE_NAME})
    group by
        match_id
)
, search_84_before AS (
    SELECT
        td.match_id AS MATCH_ID,
        COALESCE(avg(sd.search_count), 0) avg_search_count_84_days_before_null_excluded,
        COALESCE(SUM(sd.search_count), 0) total_search_count_84_days_before
    FROM
        search_data sd
    LEFT JOIN
        mx_activation td
    ON
        td.match_id=sd.store_id
        AND sd.search_date BETWEEN dateadd('day', -(28*3+1), td.REACTIVATION_TIMESTAMP) AND dateadd('day', -1, td.REACTIVATION_TIMESTAMP)
    GROUP BY
        td.match_id
), search_28_before AS (
    SELECT
        td.match_id AS MATCH_ID,
        COALESCE(avg(sd.search_count), 0) avg_search_count_28_days_before_null_excluded,
        COALESCE(SUM(sd.search_count), 0) total_search_count_28_days_before
    FROM
        search_data sd
    LEFT JOIN
        mx_activation td
    ON
        td.match_id=sd.store_id
        AND sd.search_date BETWEEN dateadd('day', -(28+1), td.REACTIVATION_TIMESTAMP) AND dateadd('day', -1, td.REACTIVATION_TIMESTAMP)
    GROUP BY
        td.match_id
), search_14_before AS (
    SELECT
        td.match_id AS MATCH_ID,
        COALESCE(avg(sd.search_count), 0) avg_search_count_14_days_before_null_excluded,
        COALESCE(SUM(sd.search_count), 0) total_search_count_14_days_before
    FROM
        search_data sd
    LEFT JOIN
        mx_activation td
    ON
        td.match_id=sd.store_id
        AND sd.search_date BETWEEN dateadd('day', -(14+1), td.REACTIVATION_TIMESTAMP) AND dateadd('day', -1, td.REACTIVATION_TIMESTAMP)
    GROUP BY
        td.match_id
)
"""

search_feature_query_nn = f"""
WITH search_data AS (
    SELECT
        store_id,
        search_date,
        sum(query_count/store_count) search_count
    FROM
        luwang.fact_store_search_counts_tfidf_match_train_20180101_20220108 -- this needs to have a date partition for replication
    WHERE
        store_id IS NOT NULL
    GROUP BY
        store_id, search_date
)
, mx_activation AS (
    select
        match_id
        , max(store_activation_date) as store_activation_date
    from
        {NN_TRAIN_5MONTH_TABLE_NAME}
    where
        create_date IN (select max(create_date) from {NN_TRAIN_5MONTH_TABLE_NAME})
    group by
        match_id
)
, search_84_before AS (
    SELECT
        nn.match_id,
        COALESCE(avg(sd.search_count), 0) avg_search_count_84_days_before_null_excluded,
        COALESCE(SUM(sd.search_count), 0) total_search_count_84_days_before
    FROM
        search_data sd
    LEFT JOIN
        mx_activation nn
    ON
        nn.match_id=sd.store_id
        AND sd.search_date BETWEEN dateadd('day', -(28*3+1), nn.store_activation_date) AND dateadd('day', -1, nn.store_activation_date)
    GROUP BY
        nn.match_id
), search_28_before AS (
    SELECT
        nn.match_id,
        COALESCE(avg(sd.search_count), 0) avg_search_count_28_days_before_null_excluded,
        COALESCE(SUM(sd.search_count), 0) total_search_count_28_days_before
    FROM
        search_data sd
    LEFT JOIN
        mx_activation nn
    ON
        nn.match_id=sd.store_id
        AND sd.search_date BETWEEN dateadd('day', -(28+1), nn.store_activation_date) AND dateadd('day', -1, nn.store_activation_date)
    GROUP BY
        nn.match_id
), search_14_before AS (
    SELECT
        nn.match_id,
        COALESCE(avg(sd.search_count), 0) avg_search_count_14_days_before_null_excluded,
        COALESCE(SUM(sd.search_count), 0) total_search_count_14_days_before
    FROM
        search_data sd
    LEFT JOIN
        mx_activation nn
    ON
        nn.match_id=sd.store_id
        AND sd.search_date BETWEEN dateadd('day', -(14+1), nn.store_activation_date) AND dateadd('day', -1, nn.store_activation_date)
    GROUP BY
        nn.match_id
)
"""

# for prediction

prediction_search_feature_query_danp = """
WITH search_data AS (
    SELECT
        store_id,
        search_date,
        sum(query_count/store_count) search_count
    FROM
        --luwang.fact_store_search_counts_tfidf_match_train_20180101_20220108        --new (start from 01/2020)     -- train (only till 05/2020) -- this needs to have a date partition for replication
        luwang.fact_store_search_counts_tfidf_match_train_20180101_20220108        --new (start from 01/2020)     -- train (only till 05/2020) -- this needs to have a date partition for replication
    WHERE
        store_id IS NOT NULL
    GROUP BY
        store_id, search_date
)
, search_84_before AS (
    SELECT
        sd.store_id AS MATCH_ID,
        COALESCE(avg(sd.search_count), 0) avg_search_count_84_days_before_null_excluded,
        COALESCE(SUM(sd.search_count), 0) total_search_count_84_days_before
    FROM
        search_data sd
    WHERE
        sd.search_date BETWEEN dateadd('day', -(28*3+2), '{test_date}') AND dateadd('day', -2, '{test_date}')
    GROUP BY
        sd.store_id
), search_28_before AS (
    SELECT
        sd.store_id AS MATCH_ID,
        COALESCE(avg(sd.search_count), 0) avg_search_count_28_days_before_null_excluded,
        COALESCE(SUM(sd.search_count), 0) total_search_count_28_days_before
    FROM
        search_data sd
    WHERE
        sd.search_date BETWEEN dateadd('day', -(28+2), '{test_date}') AND dateadd('day', -2, '{test_date}')
    GROUP BY
        sd.store_id
), search_14_before AS (
    SELECT
        sd.store_id AS MATCH_ID,
        COALESCE(avg(sd.search_count), 0) avg_search_count_14_days_before_null_excluded,
        COALESCE(SUM(sd.search_count), 0) total_search_count_14_days_before
    FROM
        search_data sd
    WHERE
        sd.search_date BETWEEN dateadd('day', -(14+2), '{test_date}') AND dateadd('day', -2, '{test_date}')
    GROUP BY
        sd.store_id
)
"""

prediction_search_feature_query_nn = """
WITH search_data AS (
    SELECT
        store_id,
        search_date,
        sum(query_count/store_count) search_count
    FROM
        --luwang.fact_store_search_counts_tfidf_match_train_20180101_20220108
        luwang.fact_store_search_counts_tfidf_match_train_20180101_20220108     --_ train -- this needs to have a date partition for replication
    WHERE
        store_id IS NOT NULL
    GROUP BY
        store_id, search_date
)
, search_84_before AS (
    SELECT
        sd.store_id AS MATCH_ID,
        COALESCE(avg(sd.search_count), 0) avg_search_count_84_days_before_null_excluded,
        COALESCE(SUM(sd.search_count), 0) total_search_count_84_days_before
    FROM
        search_data sd
    WHERE
        sd.search_date BETWEEN dateadd('day', -(28*3+2), '{test_date}') AND dateadd('day', -2, '{test_date}')
    GROUP BY
        sd.store_id
), search_28_before AS (
    SELECT
        sd.store_id AS MATCH_ID,
        COALESCE(avg(sd.search_count), 0) avg_search_count_28_days_before_null_excluded,
        COALESCE(SUM(sd.search_count), 0) total_search_count_28_days_before
    FROM
        search_data sd
    WHERE
        sd.search_date BETWEEN dateadd('day', -(28+2), '{test_date}') AND dateadd('day', -2, '{test_date}')
    GROUP BY
        sd.store_id
), search_14_before AS (
    SELECT
        sd.store_id AS MATCH_ID,
        COALESCE(avg(sd.search_count), 0) avg_search_count_14_days_before_null_excluded,
        COALESCE(SUM(sd.search_count), 0) total_search_count_14_days_before
    FROM
        search_data sd
    WHERE
        sd.search_date BETWEEN dateadd('day', -(14+2), '{test_date}') AND dateadd('day', -2, '{test_date}')
    GROUP BY
        sd.store_id
)
"""

# search data
q_search_data_danp = prediction_search_feature_query_danp + \
f"""
SELECT
      td.match_id
    , sb84.avg_search_count_84_days_before_null_excluded
    , sb84.total_search_count_84_days_before
    , sb28.avg_search_count_28_days_before_null_excluded
    , sb28.total_search_count_28_days_before
    , sb14.avg_search_count_14_days_before_null_excluded
    , sb14.total_search_count_14_days_before
    , '{{test_date}}' AS create_date
FROM
    {DANP_FEATURES_DAILY_TABLE_NAME} td
LEFT JOIN
    search_84_before sb84
ON
    td.MATCH_ID=sb84.match_id
LEFT JOIN
    search_28_before sb28
ON
    td.MATCH_ID=sb28.match_id
LEFT JOIN
    search_14_before sb14
ON
    td.MATCH_ID=sb14.match_id
WHERE 1
    --AND td.TOTAL_SUBTOTAL_FIRST_120_148_DAYS>0
    --AND td.merchant_type='active_np'
    AND td.create_date = '{{test_date}}'
    --AND td.REACTIVATION_TIMESTAMP >'2018-10-27'
;
"""

q_search_data_nn = prediction_search_feature_query_nn + \
f"""
SELECT
      nn.match_id
    , sb84.avg_search_count_84_days_before_null_excluded
    , sb84.total_search_count_84_days_before
    , sb28.avg_search_count_28_days_before_null_excluded
    , sb28.total_search_count_28_days_before
    , sb14.avg_search_count_14_days_before_null_excluded
    , sb14.total_search_count_14_days_before
    , '{{test_date}}' AS create_date
FROM
    {NN_FEATURES_DAILY_TABLE_NAME} nn
LEFT JOIN
    search_84_before sb84
ON
    nn.match_id = sb84.match_id
LEFT JOIN
    search_28_before sb28
ON
    nn.match_id = sb28.match_id
LEFT JOIN
    search_14_before sb14
ON
    nn.match_id = sb14.match_id
where 1
    --AND TOTAL_SUBTOTAL_FIRST_120_148_DAYS > 0
    AND nn.create_date = '{{test_date}}'
    --AND STORE_ACTIVATION_DATE > '2018-10-27'
;
"""

query_sub = """
WITH sub_mx_stats AS
(
SELECT
    count(distinct id) as total_mx
    , sub_id
FROM
    fact_selection_intel_mx_filtered_raw
GROUP BY
    sub_id
)
SELECT
    dd_2.SUBMARKET_ID,
    '{test_date}' AS create_date,
    avg(sms.total_mx) AS submarket_total_stores,
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
        COUNT(DISTINCT
            CASE
            WHEN DD_2.IS_PARTNER = 1 THEN DD_2.STORE_ID
            ELSE NULL
            END) / AVG(sms.total_mx) ,
    0) AS SUBMARKET_STORE_PARTNER_PERCENT_overall,
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
    PUBLIC.DIMENSION_DELIVERIES AS DD_2
LEFT JOIN
    sub_mx_stats sms
ON
    dd_2.submarket_id = sms.sub_id
WHERE
    DD_2.ACTIVE_DATE BETWEEN dateadd('DAY', -7, '{test_date}') AND dateadd('DAY', -1, '{test_date}')
    AND DD_2.IS_FILTERED_CORE = 1
GROUP BY
    1, 2
"""

query_sp = """
WITH sp_mx_stats AS
(
SELECT
    count(distinct id) as total_mx
    , sp_id
FROM
    fact_selection_intel_mx_filtered_raw
GROUP BY
    sp_id
)
SELECT
    dd_2.store_starting_point_id,
    '{test_date}' AS create_date,
    avg(sms.total_mx) AS sp_total_stores,
    SUM(DD_2.SUBTOTAL / 100.0) AS sp_TOTAL_SALES,
    COUNT(DD_2.DELIVERY_ID) AS sp_TOTAL_ORDERS,
    COUNT(DISTINCT DD_2.STORE_ID) AS sp_LIVE_STORES,
    COUNT(DISTINCT DD_2.CREATOR_ID) AS sp_NUM_CONSUMERS,
    COALESCE(
        SUM(CASE
            WHEN DD_2.IS_PARTNER IS NOT DISTINCT FROM true THEN 1.0
            ELSE 0.0
            END) / NULLIF(COUNT(DD_2.DELIVERY_ID), 0),
            0) AS sp_DELIVERY_PARTNER_PERCENT,
    COALESCE(
        COUNT(DISTINCT
            CASE
            WHEN DD_2.IS_PARTNER = 1 THEN DD_2.STORE_ID
            ELSE NULL
            END) / CAST(NULLIF(COUNT(DISTINCT DD_2.STORE_ID), 0) as FLOAT),
    0) AS sp_STORE_PARTNER_PERCENT,
    COALESCE(
        COUNT(DISTINCT
            CASE
            WHEN DD_2.IS_PARTNER = 1 THEN DD_2.STORE_ID
            ELSE NULL
            END) / AVG(sms.total_mx) ,
    0) AS sp_STORE_PARTNER_PERCENT_overall,
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
     0) AS sp_SALES_PER_partner
FROM
    PUBLIC.DIMENSION_DELIVERIES AS DD_2
LEFT JOIN
    sp_mx_stats sms
ON
    dd_2.store_starting_point_id = sms.sp_id
WHERE
    DD_2.ACTIVE_DATE BETWEEN dateadd('DAY', -7, '{test_date}') AND dateadd('DAY', -1, '{test_date}')
    AND DD_2.IS_FILTERED_CORE = 1
GROUP BY
    1, 2
"""

query_ent_l28d_sales = f"""
SELECT
    ent.sub_id,
    ent.sp_id,
    ent.restaurant_name,
    SUM(CASE
            WHEN ent.nimda_id is not null AND dd.is_partner =1
            THEN dd.subtotal
            ELSE NULL
        END) / 100 /
        COUNT(distinct CASE WHEN ent.nimda_id is not null AND dd.is_partner =1
                            THEN ent.nimda_id
                            ELSE NULL
                       END) AS l28d_avg_sales,
    '{{test_date}}' AS created_date
FROM
    DIMENSION_DELIVERIES dd
INNER JOIN
    {ENTERPRISE_MX_TABLE_NAME} ent          ---- Only look at training data that are ent stores
ON
    dd.store_id = ent.nimda_id
WHERE
    dd.IS_FILTERED_CORE = 1
    AND dd.active_date between dateadd('DAY', -28, '{{test_date}}') AND dateadd('DAY', -1, '{{test_date}}')
    --AND ent.sp_id = 300
group by 1,2,3
"""

query_ent_sub_l28d_sales = f"""
SELECT
    ent.sub_id,
    --ent.sp_id,
    ent.restaurant_name,
    SUM(CASE
            WHEN ent.nimda_id is not null AND dd.is_partner =1
            THEN dd.subtotal
            ELSE NULL
        END) / 100 /
        COUNT(distinct CASE WHEN ent.nimda_id is not null AND dd.is_partner =1
                            THEN ent.nimda_id
                            ELSE NULL
                       END) AS l28d_avg_sales,
    '{{test_date}}' AS created_date
FROM
    DIMENSION_DELIVERIES dd
INNER JOIN
    {ENTERPRISE_MX_TABLE_NAME} ent          ---- Only look at training data that are ent stores
ON
    dd.store_id = ent.nimda_id
WHERE
    dd.IS_FILTERED_CORE = 1
    AND dd.active_date between dateadd('DAY', -28, '{{test_date}}') AND dateadd('DAY', -1, '{{test_date}}')
    --AND ent.sp_id = 300
group by 1,2
"""

query_ent_global_l28d_sales = f"""
SELECT
    --ent.sub_id,
    --ent.sp_id,
    ent.restaurant_name,
    SUM(CASE
            WHEN ent.nimda_id is not null AND dd.is_partner =1
            THEN dd.subtotal
            ELSE NULL
        END) / 100 /
        COUNT(distinct CASE WHEN ent.nimda_id is not null AND dd.is_partner =1
                            THEN ent.nimda_id
                            ELSE NULL
                       END) AS l28d_avg_sales,
    '{{test_date}}' AS created_date
FROM
    DIMENSION_DELIVERIES dd
INNER JOIN
    {ENTERPRISE_MX_TABLE_NAME} ent          ---- Only look at training data that are ent stores
ON
    dd.store_id = ent.nimda_id
WHERE
    dd.IS_FILTERED_CORE = 1
    AND dd.active_date between dateadd('DAY', -28, '{{test_date}}') AND dateadd('DAY', -1, '{{test_date}}')
    --AND ent.sp_id = 300
group by 1
"""


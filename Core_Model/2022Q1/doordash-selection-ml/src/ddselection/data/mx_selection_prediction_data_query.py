from ddselection.data.selection_features_query import *
from ddselection.data.query_config import *


# pull prediction data from the prediction table (only at the test day)
query_nn_pred_features = query_head + review_ratings_sales_query + cat_pool_query + \
f"""
    SELECT
          nnf.MATCH_ID
        , nnf.STORE_NAME
        , nnf.VENDOR_1_ID
        , nnf.STARTING_POINT_ID
        , nnf.SUBMARKET_ID
        , nnf.requestable
        , nnf.ACTIVATION_MONTH
        , nnf.ACTIVATION_YEAR
        , nnf.FIRST_REQUEST_REQUESTED_AT
        , nnf.LAST_REQUEST_REQUESTED_AT
        , nnf.NUM_REQUESTS_L84DAYS
        , nnf.NUM_REQUESTS_L28DAYS
        , nnf.NUM_REQUESTS_ALL_TIME
        , nnf.num_request_per_day
        , nnf.AVG_SUBTOTAL_FROM_CONSUMER_REQUEST
        , nnf.NUM_DELIVERY_TO_REQUESTS_RATIO
        , nnf.create_date
        , nnf.country_id
        , nnf.rar_not_shown
        , msub.submarket_sales_per_partner
        , msub.submarket_store_partner_percent
        , msub.submarket_delivery_partner_percent
        , msp.sp_delivery_partner_percent
        , msp.sp_sales_per_partner
        , msp.sp_store_partner_percent
        , nnsf.avg_search_count_84_days_before_null_excluded
        , nnsf.total_search_count_84_days_before
        , nnsf.avg_search_count_28_days_before_null_excluded
        , nnsf.total_search_count_28_days_before
        , nnsf.avg_search_count_14_days_before_null_excluded
        , nnsf.total_search_count_14_days_before
        , rr.reviews_mes
        , rr.ratings_mes
        , rr.reviews_yelp
        , rr.rating_yelp
        , rr.rating_factus
        , rr.stars_al
        , rr.reviews_al
        , rr.sales_chdus
        , cfv.sales_chd_agg
        , cfv.cohort
        , cfv.years_in_business
        , cfv.price_yelp
        , cfv.price_mes
        , cfv.price_factus
        , cfv.price_al
        , cfv.number_of_units
        , cfv.dayparts
        , cfv.average_check
        , cfv.average_check_amt
        , cfv.credit_score_rating
        , 4 as age_month
        , year(dateadd('MONTH', 4,'{{test_date}}')) as sales_year
        , month(dateadd('MONTH', 4,'{{test_date}}')) as sales_month
        , '{{test_date}}' as activation_date
    FROM
        {NN_FEATURES_DAILY_TABLE_NAME} nnf
    LEFT JOIN
    (
        SELECT
            *
        FROM
            {SUBMARKET_FEATURE_TABLE_NAME}
        WHERE
            create_date = '{{test_date}}'
    ) msub
    ON
        nnf.submarket_id = msub.submarket_id
    LEFT JOIN
    (
        SELECT
            *
        FROM
            {SP_FEATURE_TABLE_NAME}
        WHERE
            create_date = '{{test_date}}'
    ) msp
    ON
        nnf.starting_point_id = msp.store_starting_point_id
    LEFT JOIN
    (
        SELECT
            *
        FROM
            {NN_SEARCH_FEATURES_DAILY_TABLE_NAME}
        WHERE
            create_date = '{{test_date}}'
    ) nnsf
    ON
        nnf.match_id = nnsf.match_id
    LEFT JOIN
        review_ratings_sales rr
    ON
        nnf.match_id=rr.match_id
    LEFT JOIN
        cat_from_vendor cfv
    ON
        nnf.match_id=cfv.match_id
    WHERE
        nnf.create_date = '{{test_date}}'
"""


query_ac_pred_features = query_head + review_ratings_sales_query + cat_pool_query + \
f"""
    SELECT
          danpf.*
        , danpsf.avg_search_count_84_days_before_null_excluded
        , danpsf.total_search_count_84_days_before
        , danpsf.avg_search_count_28_days_before_null_excluded
        , danpsf.total_search_count_28_days_before
        , danpsf.avg_search_count_14_days_before_null_excluded
        , danpsf.total_search_count_14_days_before
        , rr.reviews_mes
        , rr.ratings_mes
        , rr.reviews_yelp
        , rr.rating_yelp
        , rr.rating_factus
        , rr.stars_al
        , rr.reviews_al
        , rr.sales_chdus
        , cfv.sales_chd_agg
        , cfv.cohort
        , cfv.years_in_business
        , cfv.price_yelp
        , cfv.price_mes
        , cfv.price_factus
        , cfv.price_al
        , cfv.number_of_units
        , cfv.dayparts
        , cfv.average_check
        , cfv.average_check_amt
        , cfv.credit_score_rating
        , 4 as age_month
        , year(dateadd('MONTH', 4,'{{test_date}}')) as sales_year
        , month(dateadd('MONTH', 4,'{{test_date}}')) as sales_month
        , '{{test_date}}' as activation_date
    FROM
        {DANP_FEATURES_DAILY_TABLE_NAME} danpf
    LEFT JOIN
    (
        SELECT
            *
        FROM
            {DANP_SEARCH_FEATURES_DAILY_TABLE_NAME}
        WHERE
            create_date = '{{test_date}}'
    ) danpsf
    ON
        danpf.match_id = danpsf.match_id
    LEFT JOIN
        review_ratings_sales rr
    ON
        danpf.match_id=rr.match_id
    LEFT JOIN
        cat_from_vendor cfv
    ON
        danpf.match_id=cfv.match_id
    WHERE
        merchant_type_prod = 'active'
        AND danpf.create_date = '{{test_date}}'
"""

query_da_pred_features = query_head + review_ratings_sales_query + cat_pool_query + \
f"""
    SELECT
          danpf.*
        , danpsf.avg_search_count_84_days_before_null_excluded
        , danpsf.total_search_count_84_days_before
        , danpsf.avg_search_count_28_days_before_null_excluded
        , danpsf.total_search_count_28_days_before
        , danpsf.avg_search_count_14_days_before_null_excluded
        , danpsf.total_search_count_14_days_before
        , rr.reviews_mes
        , rr.ratings_mes
        , rr.reviews_yelp
        , rr.rating_yelp
        , rr.rating_factus
        , rr.stars_al
        , rr.reviews_al
        , rr.sales_chdus
        , cfv.sales_chd_agg
        , cfv.cohort
        , cfv.years_in_business
        , cfv.price_yelp
        , cfv.price_mes
        , cfv.price_factus
        , cfv.price_al
        , cfv.number_of_units
        , cfv.dayparts
        , cfv.average_check
        , cfv.average_check_amt
        , cfv.credit_score_rating
        , 4 as age_month
        , year(dateadd('MONTH', 4,'{{test_date}}')) as sales_year
        , month(dateadd('MONTH', 4,'{{test_date}}')) as sales_month
        , '{{test_date}}' as activation_date
    FROM
        {DANP_FEATURES_DAILY_TABLE_NAME} danpf
    LEFT JOIN
    (
        SELECT
            *
        FROM
            {DANP_SEARCH_FEATURES_DAILY_TABLE_NAME}
        WHERE
            create_date = '{{test_date}}'
    ) danpsf
    ON
        danpf.match_id = danpsf.match_id
    LEFT JOIN
        review_ratings_sales rr
    ON
        danpf.match_id=rr.match_id
    LEFT JOIN
        cat_from_vendor cfv
    ON
        danpf.match_id=cfv.match_id
    WHERE
        merchant_type_prod = 'deactivated'
        AND danpf.create_date = '{{test_date}}'
"""

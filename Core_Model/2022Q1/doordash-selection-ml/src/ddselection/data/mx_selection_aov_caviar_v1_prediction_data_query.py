from ddselection.data.selection_features_query import *
from ddselection.data.query_config import *


# pull prediction data from the prediction table (only at the test date)
query_nn_pred_features_aov_v1 = query_head + review_ratings_sales_query + cat_pool_query + \
F"""
    SELECT 
          nnf.*
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
        , rr.sales_chdus
        , cfv.sales_chd_agg
        , cfv.cohort
        , cfv.years_in_business
        , cfv.price_yelp
        , cfv.price_mes
        , cfv.price_factus
        , cfv.number_of_units
        , cfv.dayparts
        , cfv.average_check
        , cfv.average_check_amt
        , cfv.credit_score_rating
        , '{{test_date}}' as activation_date
    FROM 
        {NN_AOV_FEATURES_DAILY_TABLE_NAME} nnf
    LEFT JOIN
        {NN_SEARCH_FEATURES_DAILY_TABLE_NAME} nnsf
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
        AND nnsf.create_date = '{{test_date}}'
"""
query_ac_pred_features_aov_v1 = query_head + review_ratings_sales_query + cat_pool_query + \
F"""
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
        , rr.sales_chdus
        , cfv.sales_chd_agg
        , cfv.cohort
        , cfv.years_in_business
        , cfv.price_yelp
        , cfv.price_mes
        , cfv.price_factus
        , cfv.number_of_units
        , cfv.dayparts
        , cfv.average_check
        , cfv.average_check_amt
        , cfv.credit_score_rating
        , '{{test_date}}' as activation_date
    FROM 
        {DANP_AOV_FEATURES_DAILY_TABLE_NAME} danpf
    LEFT JOIN
        {DANP_SEARCH_FEATURES_DAILY_TABLE_NAME} danpsf
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
        AND danpsf.create_date = '{{test_date}}'
"""
query_da_pred_features_aov_v1 = query_head + review_ratings_sales_query + cat_pool_query + \
F"""
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
        , rr.sales_chdus
        , cfv.sales_chd_agg
        , cfv.cohort
        , cfv.years_in_business
        , cfv.price_yelp
        , cfv.price_mes
        , cfv.price_factus
        , cfv.number_of_units
        , cfv.dayparts
        , cfv.average_check
        , cfv.average_check_amt
        , cfv.credit_score_rating
        , '{{test_date}}' as activation_date
    FROM 
        {DANP_AOV_FEATURES_DAILY_TABLE_NAME} danpf
    LEFT JOIN
        {DANP_SEARCH_FEATURES_DAILY_TABLE_NAME} danpsf
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
        AND danpsf.create_date = '{{test_date}}'
"""
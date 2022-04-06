from ddselection.data.selection_features_query import *
from ddselection.data.query_config import *


q_aov_training_data_ac_v1 = search_feature_query_danp + review_ratings_sales_query + cat_pool_query +\
F"""
SELECT
      td.*
    , td.reactivation_timestamp as activation_date
    , b.premium as is_caviar_premium
    , case when td.aov_first_148_days >= 40 then 1 else 0 end as aov_first_148_days_high
    , sb84.avg_search_count_84_days_before_null_excluded
    , sb84.total_search_count_84_days_before
    , sb28.avg_search_count_28_days_before_null_excluded
    , sb28.total_search_count_28_days_before
    , sb14.avg_search_count_14_days_before_null_excluded
    , sb14.total_search_count_14_days_before
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
FROM
    {DANP_AOV_TRAIN_TABLE_NAME} td
JOIN
    {CAVIAR_PREMIUM_TABLE_NAME} b
ON
    td.store_id = b.store_id
LEFT JOIN
    search_84_before sb84
ON
    td.match_id=sb84.match_id
LEFT JOIN
    search_28_before sb28
ON
    td.match_id=sb28.match_id
LEFT JOIN
    search_14_before sb14
ON
    td.match_id=sb14.match_id
LEFT JOIN
    review_ratings_sales rr
ON
    td.match_id=rr.match_id
LEFT JOIN
    cat_from_vendor cfv
ON
    td.match_id=cfv.match_id
WHERE 1
    AND td.TOTAL_SUBTOTAL_FIRST_120_148_DAYS >= 0
    AND td.merchant_type='active_np'
    AND td.create_date IN (select max(create_date) from chendong.aov_training_data_danp_test)
    AND td.REACTIVATION_TIMESTAMP >'2018-10-27' 
"""

q_aov_training_data_nn_v1 = search_feature_query_nn + review_ratings_sales_query + cat_pool_query +\
F"""
SELECT
      nn.*
    , nn.store_activation_date as activation_date
    , b.premium as is_caviar_premium
    , case when nn.aov_first_148_days >= 40 then 1 else 0 end as aov_first_148_days_high
    , sb84.avg_search_count_84_days_before_null_excluded
    , sb84.total_search_count_84_days_before
    , sb28.avg_search_count_28_days_before_null_excluded
    , sb28.total_search_count_28_days_before
    , sb14.avg_search_count_14_days_before_null_excluded
    , sb14.total_search_count_14_days_before
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
FROM
    {NN_AOV_TRAIN_TABLE_NAME} nn
JOIN
    {CAVIAR_PREMIUM_TABLE_NAME} b
ON
    nn.store_id = b.store_id
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
LEFT JOIN
    review_ratings_sales rr
ON
    nn.match_id=rr.match_id
LEFT JOIN
    cat_from_vendor cfv
ON
    nn.match_id=cfv.match_id
where 1
    AND TOTAL_SUBTOTAL_FIRST_120_148_DAYS >= 0
    AND nn.create_date IN (select max(create_date) from {NN_AOV_TRAIN_TABLE_NAME})
    AND STORE_ACTIVATION_DATE > '2018-10-27'
"""

q_aov_training_data_da_v1 = search_feature_query_danp + review_ratings_sales_query + cat_pool_query +\
F"""
SELECT
      td.*
    , td.reactivation_timestamp as activation_date
    , b.premium as is_caviar_premium
    , case when td.aov_first_148_days >= 40 then 1 else 0 end as aov_first_148_days_high
    , sb84.avg_search_count_84_days_before_null_excluded
    , sb84.total_search_count_84_days_before
    , sb28.avg_search_count_28_days_before_null_excluded
    , sb28.total_search_count_28_days_before
    , sb14.avg_search_count_14_days_before_null_excluded
    , sb14.total_search_count_14_days_before
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
FROM
    {DANP_AOV_TRAIN_TABLE_NAME} td
JOIN
    {CAVIAR_PREMIUM_TABLE_NAME} b
ON
    td.store_id = b.store_id
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
LEFT JOIN
    review_ratings_sales rr
ON
    td.match_id=rr.match_id
LEFT JOIN
    cat_from_vendor cfv
ON
    td.match_id=cfv.match_id
WHERE 1
    AND td.TOTAL_SUBTOTAL_FIRST_120_148_DAYS >= 0
    AND td.merchant_type='deactivated_np'
    AND td.create_date IN (select max(create_date) from {DANP_AOV_TRAIN_TABLE_NAME})
    AND td.REACTIVATION_TIMESTAMP>'2018-10-27'
"""

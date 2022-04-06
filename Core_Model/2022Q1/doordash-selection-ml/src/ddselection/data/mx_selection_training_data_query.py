from ddselection.data.selection_features_query import *
from ddselection.data.query_config import *

q_training_data_ac = search_feature_query_danp + \
    review_ratings_sales_query + cat_pool_query + \
    f"""
    SELECT
          td.*
        --, year(td.reactivation_timestamp) as reactivation_year
        --, month(td.reactivation_timestamp) as reactivation_month
        , td.reactivation_timestamp as activation_date
        , month(td.min_active_date) as sales_month
        , quarter(td.min_active_date) as sales_quarter
        , year(td.min_active_date) as sales_year
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
    FROM
        {DANP_TRAIN_TABLE_NAME} td
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
        --AND td.TOTAL_SUBTOTAL_FIRST_120_148_DAYS >= 0
        AND td.merchant_type='active_np'
        AND td.create_date IN (select max(create_date) from {DANP_TRAIN_TABLE_NAME})
    """

q_training_data_nn = search_feature_query_nn + \
    review_ratings_sales_query + cat_pool_query + \
    f"""
    SELECT
          nnf.match_id
        , nnf.store_id
        , nnf.store_activation_date as activation_date
        , nnf.total_subtotal_month
        , nnf.requestable
        , nnf.num_requests_l28days
        , nnf.avg_subtotal_from_consumer_request
        , nnf.min_active_date
        , year(nnf.store_activation_date) as activation_year
        , month(nnf.store_activation_date) as activation_month
        , month(nnf.min_active_date) as sales_month
        , quarter(nnf.min_active_date) as sales_quarter
        , year(nnf.min_active_date) as sales_year
        , nnf.age_month
        , nnf.country_id
        , nnf.rar_not_shown
        , msub.submarket_sales_per_partner
        , msub.submarket_store_partner_percent
        --, msub.submarket_store_partner_percent_overall
        , msub.submarket_delivery_partner_percent
        , msp.sp_delivery_partner_percent
        , msp.sp_sales_per_partner
        , msp.sp_store_partner_percent
        --, msp.sp_store_partner_percent_overall
        , sb84.avg_search_count_84_days_before_null_excluded
        , sb84.total_search_count_84_days_before
        , sb28.avg_search_count_28_days_before_null_excluded
        , sb28.total_search_count_28_days_before
        , sb14.avg_search_count_14_days_before_null_excluded
        , sb14.total_search_count_14_days_before
        --, msub.*
        --, msp.*
        , rr.reviews_mes
        , rr.ratings_mes
        , rr.reviews_yelp
        , rr.rating_yelp
        , rr.rating_factus
        , rr.sales_chdus
        , rr.stars_al
        , rr.reviews_al
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
        --corr(msub.submarket_sales_per_partner, total_subtotal_month)
    FROM
        {NN_TRAIN_TABLE_NAME} nnf
    LEFT JOIN
        {SUBMARKET_FEATURE_TABLE_NAME} msub
    ON
        nnf.submarket_id = msub.submarket_id
        AND nnf.store_activation_date = msub.create_date
        --AND dateadd('DAY', -4*28, nnf.min_active_date) = msub.create_date
    LEFT JOIN
        {SP_FEATURE_TABLE_NAME} msp
    ON
        nnf.starting_point_id = msp.store_starting_point_id
        AND nnf.store_activation_date = msp.create_date
        --AND dateadd('DAY', -4*28, nnf.min_active_date) = msp.create_date
    LEFT JOIN
        search_84_before sb84
    ON
        nnf.match_id = sb84.match_id
    LEFT JOIN
        search_28_before sb28
    ON
        nnf.match_id = sb28.match_id
    LEFT JOIN
        search_14_before sb14
    ON
        nnf.match_id = sb14.match_id
    LEFT JOIN
        review_ratings_sales rr
    ON
        nnf.match_id=rr.match_id
    LEFT JOIN
        cat_from_vendor cfv
    ON
        nnf.match_id=cfv.match_id
    WHERE 1
        AND nnf.create_date IN (select max(create_date) from {NN_TRAIN_TABLE_NAME})
    """


q_training_data_da = search_feature_query_danp + \
    review_ratings_sales_query + cat_pool_query + \
    f"""
    SELECT
          td.*
        , td.reactivation_timestamp as activation_date
        , month(td.min_active_date) as sales_month
        , quarter(td.min_active_date) as sales_quarter
        , year(td.min_active_date) as sales_year
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
        , rr.stars_al
        , rr.reviews_al
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
    FROM
        {DANP_TRAIN_TABLE_NAME} td
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
    WHERE
        td.merchant_type='deactivated_np'
        AND td.create_date IN (select max(create_date) from {DANP_TRAIN_TABLE_NAME})

    """

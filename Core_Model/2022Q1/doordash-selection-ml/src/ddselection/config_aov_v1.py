MODEL_VERSION = 'V1'

# define features
AC_NUM_COLS = [
    'aov_before',
    'avg_fee_before',
    'avg_search_count_84_days_before_null_excluded',
    'total_search_count_84_days_before',
    'avg_search_count_28_days_before_null_excluded',
    'total_search_count_28_days_before',
    'avg_search_count_14_days_before_null_excluded',
    'total_search_count_14_days_before',
    'avg_subtotal_before',
    'avg_subtotal_per_store_business_before',
    'num_consumers_growth_sp',
    'num_days_active_28days_before',
    'num_orders_28days_before',
    'num_orders_per_active_date_before',
    'reactivation_month',
    'reactivation_year',
    'total_subtotal_28days_before',
    'total_subtotal_7days_before',
    'total_subtotal_per_active_date_before',
    'total_subtotal_per_active_date_business_before',
    'reviews_mes',
    'ratings_mes',
    'reviews_yelp',
    'rating_yelp',
    'rating_factus',
    'sales_chdus',
]

AC_CAT_COLS = [
    'sales_chd_agg',
    'cohort',
    'years_in_business',
    'price_yelp',
    'price_mes',
    'price_factus',
    'number_of_units',
    'dayparts',
    'average_check',
    'average_check_amt',
    'credit_score_rating',
]

AC_Y_COL = [
    'is_caviar_premium',
]

DA_NUM_COLS = [
    'avg_search_count_84_days_before_null_excluded',
    'total_search_count_84_days_before',
    'avg_search_count_28_days_before_null_excluded',
    'total_search_count_28_days_before',
    'avg_search_count_14_days_before_null_excluded',
    'total_search_count_14_days_before',
    'num_requests_l28days',
    'aov_before',
    'avg_fee_before',
    'avg_subtotal_before',
    'avg_subtotal_per_store_business_before',
    'deactivation_month',
    'deactivation_year',
    'num_consumers_growth_sp',
    'num_first_ordercarts_before',
    'num_orders_28days_before',
    'num_orders_per_active_date_before',
    'reactivation_month',
    'reactivation_year',
    'total_subtotal_28days_before',
    'total_subtotal_per_active_date_before',
    'total_subtotal_per_active_date_business_before',
    'total_subtotal_scaler_sp',
    'reviews_mes',
    'ratings_mes',
    'reviews_yelp',
    'rating_yelp',
    'rating_factus',
    'sales_chdus',
]

DA_CAT_COLS = [
    'requestable',
    'sales_chd_agg',
    'cohort',
    'years_in_business',
    'price_yelp',
    'price_mes',
    'price_factus',
    'number_of_units',
    'dayparts',
    'average_check',
    'average_check_amt',
    'credit_score_rating',
]

DA_Y_COL = [
    'is_caviar_premium',
]

NN_NUM_COLS = [
    'avg_search_count_84_days_before_null_excluded',
    'total_search_count_84_days_before',
    'avg_search_count_28_days_before_null_excluded',
    'total_search_count_28_days_before',
    'avg_search_count_14_days_before_null_excluded',
    'total_search_count_14_days_before',
    'num_requests_l28days',
    'activation_year',
    'avg_subtotal_from_consumer_request',
    'submarket_sales_per_partner',
    'submarket_store_partner_percent',
    'submarket_delivery_partner_percent',
    'submarket_aov',
    'sp_delivery_partner_percent',
    'sp_sales_per_partner',
    'sp_store_partner_percent',
    'sp_aov',
    'reviews_mes',
    'ratings_mes',
    'reviews_yelp',
    'rating_yelp',
    'rating_factus',
    'sales_chdus',
]

NN_CAT_COLS = [
    'requestable',
    'sales_chd_agg',
    'cohort',
    'years_in_business',
    'price_yelp',
    'price_mes',
    'price_factus',
    'number_of_units',
    'dayparts',
    'average_check',
    'average_check_amt',
    'credit_score_rating',
]

NN_Y_COL = [
    'is_caviar_premium',
]

PASSTHROUGH_COLS = [
    'match_id',
    'activation_date'
]

SEGMENT_FEATURE_MAP = {
    'nn': {'num': NN_NUM_COLS, 'cat': NN_CAT_COLS, 'y': NN_Y_COL},
    'ac': {'num': AC_NUM_COLS, 'cat': AC_CAT_COLS, 'y': AC_Y_COL},
    'da': {'num': DA_NUM_COLS, 'cat': DA_CAT_COLS, 'y': DA_Y_COL}
}

DATE_SPLIT_COL = ['activation_date']

DATE_ACTIVATION_COL = ['activation_date']

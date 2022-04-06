# training data table names
#NN_TRAIN_TABLE_NAME = 'luwang.mx_selection_intel_nn_train_country_v1_20190104_20220131'
#DANP_TRAIN_TABLE_NAME = 'luwang.mx_selection_intel_danp_country_v1_20190104_20220131'
#NN_TRAIN_TABLE_NAME = 'luwang.mx_selection_intel_nn_train_country_v1_20200101_20220131'
#DANP_TRAIN_TABLE_NAME = 'luwang.mx_selection_intel_danp_country_v1_20200101_20220131'
NN_TRAIN_TABLE_NAME = 'luwang.mx_selection_intel_nn_train_country_v1_20201025_20220131'
DANP_TRAIN_TABLE_NAME = 'luwang.mx_selection_intel_danp_country_v1_20200101_20220131'

# old tables for ad hoc analysis
# NN_TRAIN_TABLE_NAME = 'chendong.selection_ml_training_data_v3_test_190104_210131'
# DANP_TRAIN_TABLE_NAME = 'chendong.selection_ml_training_data_v3_danp_test_190104_210131'

# validation data table names, also used to store activation date of each mx to compute search features
NN_TRAIN_5MONTH_TABLE_NAME = 'public.mx_selection_intel_net_new_train_v1_1'
DANP_TRAIN_5MONTH_TABLE_NAME = 'public.mx_selection_intel_deactivated_activeNP_train_v1_1'

# prediction data table names
NN_FEATURES_DAILY_TABLE_NAME = 'public.mx_selection_intel_nn_features_country_v1_20210501_20210731'
DANP_FEATURES_DAILY_TABLE_NAME = 'public.mx_selection_intel_danp_features_country_v1_20210501_20210731'
NN_SEARCH_FEATURES_DAILY_TABLE_NAME = 'public.mx_selection_intel_nn_search_features_20210501_20210731_v1'
DANP_SEARCH_FEATURES_DAILY_TABLE_NAME = 'public.mx_selection_intel_danp_search_features_20210501_20210731_v1'

# feature table names
NUM_FEATURE_TABLE_NAME = 'luwang.mx_vendor_features_w_al'
CAT_FEATURE_TABLE_NAME = 'luwang.mx_vendor_features_w_al'
SUBMARKET_FEATURE_TABLE_NAME = 'public.mx_selection_intel_sub_l7d_features_20210501_20210731'
SP_FEATURE_TABLE_NAME = 'public.mx_selection_intel_sp_l7d_features_20210501_20210731'

# for enterprise adj
ENTERPRISE_MX_TABLE_NAME = 'luwang.selection_intel_mx_enterprise'
ent_global_mx_table_name = 'public.mx_selection_intel_ent_global_l28d_sales_20210501_20210731'
ent_sub_mx_table_name = 'public.mx_selection_intel_ent_sub_l28d_sales_20210501_20210731'
ent_sp_mx_table_name = 'public.mx_selection_intel_ent_sp_l28d_sales_daily_20210501_20210731'

# for caviar & dd(high) aov models
NN_AOV_TRAIN_TABLE_NAME = 'chendong.aov_training_data_test'
DANP_AOV_TRAIN_TABLE_NAME = 'chendong.aov_training_data_danp_test'
CAVIAR_PREMIUM_TABLE_NAME = 'chendong.mx_caviar_is_premium'
CAVIAR_AOV_TABLE_NAME = 'chendong.mx_caviar_aov'

NN_AOV_FEATURES_DAILY_TABLE_NAME = 'public.mx_selection_intel_aov_caviar_nn_features_daily'
DANP_AOV_FEATURES_DAILY_TABLE_NAME = 'public.mx_selection_intel_aov_caviar_danp_features_daily'

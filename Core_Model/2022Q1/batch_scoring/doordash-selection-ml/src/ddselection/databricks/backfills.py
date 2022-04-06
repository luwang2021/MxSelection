import logging

from datetime import datetime, timedelta
from ddselection.databricks.mx_selection_prediction import (
    gen_predictions_cohort
)
from ddselection.databricks.snowflake import (
    upload_pd_df, load_data_w_dates_spark, write_spark_df_to_snowflake
)
from ddselection.data.utils import set_access_param
from ddselection.data.mx_selection_ent_adj_query import ent_adj_query
from ddselection.data.selection_features_query import (
    query_sub, query_sp,
    q_search_data_nn,
    q_search_data_danp,
    query_ent_l28d_sales,
    query_ent_sub_l28d_sales,
    query_ent_global_l28d_sales
)
from ddselection.data.mx_selection_prediction_data_daacnp_raw_query import (
    query_danp
)
from ddselection.data.mx_selection_prediction_data_net_new_raw_query import (
    query_nn
)
from ddselection.data.mx_selection_aov_caviar_prediction_data_daacnp_raw_query import (
    query_aov_danp
)
from ddselection.data.mx_selection_aov_caviar_prediction_data_net_new_raw_query import (
    query_aov_nn
)
from ddselection.databricks.utils import (
    create_prediction_table_with_actual,
    generate_mx_rank_df_v1
)
from ddselection.metrics.utils import (
    combine_actual_pred_rank_df
)

logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


def _backfill(dates, query, table_name, options, spark):
    """Load query data (with backfill / test date) and output to snowflake using spark"""
    for date in dates:
        logger.info('backfill date {date} for {table}'.format(
            date=date, table=table_name)
        )
        df = load_data_w_dates_spark(
            query=query,
            test_date=date,
            options=options,
            spark=spark
        )
        write_spark_df_to_snowflake(
            df,
            table_name,
            options=options,
            spark=spark
        )
    spark._jvm.net.snowflake\
         .spark.snowflake\
         .Utils.runQuery(
             options,
             "grant select on {}.{} to role read_only_users".
             format(options['sfschema'], table_name)
         )


def backfill_predictions(
    schema,
    role,
    user,
    password,
    dates,
    spark,
    prediction_func,
    table_name_raw='mx_ml_5thmonth_sales_prediction_raw',
    model_version='v2',
    model_suffix='_test_v2_1',
    adj=False,
    mlflow_version=None
):
    """Load register selection models
       and backfill predictions (raw and deduped predictions).
       output prediction table name: raw_table_name + '_model_version'
       deduped table name: raw_table_name + '_model_version' + '_deduped'

    Args:
        schema         (str): databricks schema
        role           (str): user role in snowflake
        user           (str): databricks user credential
        password       (str): databricks password credential
        dates          (str): prediction dates to backfill
        spark          (spark): databricks spark
        prediction_func(func): function to generate predictions of various models
        table_name_raw (func): output table name (a model version is added below)
        model_version  (str): model version
        model_suffix   (str): model name in mlflow
        adj            (bool): whether to output adjusted predictions
        mlflow_version (int): the model version of the model in mlflow
    Returns:
        None, backfilled data is uploaded to snowflake

    """
    options, params = set_access_param(
        schema, role, user, password
    )

    table_name = table_name_raw + '_' + model_version
    for pred_date in dates:
        logger.info('fill prediction date: {}'.format(pred_date))
        output = prediction_func(
            schema,
            role,
            user,
            password,
            pred_date=pred_date,
            model_suffix=model_suffix,
            mlflow_version=mlflow_version
        )
        if output is None:
            continue
        output['create_date'] = pred_date

        upload_pd_df(
            output, table_name,
            options=options, spark=spark, mode='append'
        )

        # hard coded here
        logger.info('dedupe the prediction')
        if adj:
            output_dedupe = output.groupby(by=['match_id'])\
                .agg({
                    'pred': 'mean',
                    'pred_adj': 'mean',
                    'data_preprocess_model_version': 'max',
                    'ml_model_version': 'max',
                    'bias_adj_model_version': 'max',
                    'model': 'max'
                }).reset_index()
        else:
            output_dedupe = output.groupby(by=['match_id'])\
                .agg({
                    'pred': 'mean',
                    'data_preprocess_model_version': 'max',
                    'ml_model_version': 'max',
                    'bias_adj_model_version': 'max',
                    'model': 'max'
                }).reset_index()

        output_dedupe['create_date'] = pred_date

        upload_pd_df(
            output_dedupe, table_name + '_deduped',
            options=options, spark=spark, mode='append'
        )


def backfill_predictions_cohort(
    schema,
    role,
    user,
    password,
    dates,
    spark,
    table_name_raw='mx_ml_5thmonth_sales_prediction_raw',
    model_version='v2',
    model_suffix='_test_v2_1',
    adj=True,
    mlflow_version=None
):
    """Load register selection models
       and backfill predictions (raw and deduped predictions).
       output prediction table name: raw_table_name + '_model_version'
       deduped table name: raw_table_name + '_model_version' + '_deduped'

    Args:
        schema         (str): databricks schema
        role           (str): user role in snowflake
        user           (str): databricks user credential
        password       (str): databricks password credential
        dates          (str): prediction dates to backfill
        spark          (spark): databricks spark
        table_name_raw (func): output table name
                               (a model version is added, see below)
        model_version  (str): model version
        model_suffix   (str): model name in mlflow
        adj            (bool): whether to output linear transformed
                               predictions (deprecated)
        mlflow_version (int): the model version of the model in mlflow
    Returns:
        None, backfilled data is uploaded to snowflake

    """
    options, params = set_access_param(
        schema, role, user, password
    )

    table_name = table_name_raw + '_' + model_version
    for pred_date in dates:
        logger.info('fill prediction date: {}'.format(pred_date))
        output = gen_predictions_cohort(
            schema,
            role,
            user,
            password,
            pred_date=pred_date,
            model_suffix=model_suffix,
            mlflow_version=mlflow_version
        )
        if output is None:
            continue
        output['create_date'] = pred_date

        upload_pd_df(
            output, table_name,
            options=options, spark=spark, mode='append'
        )

        # hard coded here
        logger.info('dedupe the prediction')
        if adj:
            output_dedupe = output.groupby(by=['match_id'])\
                .agg({
                    'pred': 'mean',
                    'pred_adj': 'mean',
                    'data_preprocess_model_version': 'max',
                    'ml_model_version': 'max',
                    'bias_adj_model_version': 'max',
                    'model': 'max'
                }).reset_index()
        else:
            output_dedupe = output.groupby(by=['match_id'])\
                .agg({
                    'pred': 'mean',
                    'data_preprocess_model_version': 'max',
                    'ml_model_version': 'max',
                    'bias_adj_model_version': 'max',
                    'model': 'max'
                }).reset_index()

        output_dedupe['create_date'] = pred_date

        upload_pd_df(
            output_dedupe, table_name + '_deduped',
            options=options, spark=spark, mode='append'
        )


def backfill_ent_adj(
    schema,
    role,
    user,
    password,
    dates,
    raw_prediction_table_name,
    output_table_name,
    spark,
    query=ent_adj_query,
):
    """Backfill enterprise adj predictions

    Args:
        schema                    (str): databricks schema
        role                      (str): user role in snowflake
        user                      (str): databricks user credential
        password                  (str): databricks password credential
        dates                     (str): prediction dates to backfill
        raw_prediction_table_name (str): the table name of raw predictions
        output_table_name         (str): the table name for output
        spark                     (spark): databricks spark
        query                     (str): query to execute enterprise adj

    Returns:
        None, backfilled data is uploaded to snowflake

    """
    options, _ = set_access_param(
        schema, role, user, password
    )
    _backfill(
        dates=dates,
        query=query.format(
            raw_pred_table_name=raw_prediction_table_name,
            test_date='{test_date}'
        ),
        table_name=output_table_name,
        options=options,
        spark=spark
    )


def backfill_sm_features(
    schema,
    role,
    user,
    password,
    dates,
    output_table_name,
    spark,
    query=query_sub,
):
    """Backfill submarket level features

    Args:
        schema                    (str): databricks schema
        role                      (str): user role in snowflake
        user                      (str): databricks user credential
        password                  (str): databricks password credential
        dates                     (str): prediction dates to backfill
        output_table_name         (str): the table name for output
        spark                     (spark): databricks spark
        query                     (str): query to pull features

    Returns:
        None, backfilled data is uploaded to snowflake

    """
    options, _ = set_access_param(
        schema, role, user, password
    )
    _backfill(
        dates=dates,
        query=query,
        table_name=output_table_name,
        options=options,
        spark=spark
    )


def backfill_sp_features(
    schema,
    role,
    user,
    password,
    dates,
    output_table_name,
    spark,
    query=query_sp,
):
    """Backfill starting point level features

    Args:
        schema                    (str): databricks schema
        role                      (str): user role in snowflake
        user                      (str): databricks user credential
        password                  (str): databricks password credential
        dates                     (str): prediction dates to backfill
        output_table_name         (str): the table name for output
        spark                     (spark): databricks spark
        query                     (str): query to pull features

    Returns:
        None, backfilled data is uploaded to snowflake

    """
    options, _ = set_access_param(
        schema, role, user, password
    )
    _backfill(
        dates=dates,
        query=query,
        table_name=output_table_name,
        options=options,
        spark=spark
    )


def backfill_ent_sp_features(
    schema,
    role,
    user,
    password,
    dates,
    output_table_name,
    spark,
    query=query_ent_l28d_sales,
):
    """Backfill starting point level features for enterprise mx

    Args:
        schema                    (str): databricks schema
        role                      (str): user role in snowflake
        user                      (str): databricks user credential
        password                  (str): databricks password credential
        dates                     (str): prediction dates to backfill
        output_table_name         (str): the table name for output
        spark                     (spark): databricks spark
        query                     (str): query to pull features

    Returns:
        None, backfilled data is uploaded to snowflake

    """
    options, _ = set_access_param(
        schema, role, user, password
    )
    _backfill(
        dates=dates,
        query=query,
        table_name=output_table_name,
        options=options,
        spark=spark
    )


def backfill_ent_sm_features(
    schema,
    role,
    user,
    password,
    dates,
    output_table_name,
    spark,
    query=query_ent_sub_l28d_sales,
):
    """Backfill submarket level features for enterprise

    Args:
        schema                    (str): databricks schema
        role                      (str): user role in snowflake
        user                      (str): databricks user credential
        password                  (str): databricks password credential
        dates                     (str): prediction dates to backfill
        output_table_name         (str): the table name for output
        spark                     (spark): databricks spark
        query                     (str): query to pull features

    Returns:
        None, backfilled data is uploaded to snowflake

    """
    options, _ = set_access_param(
        schema, role, user, password
    )
    _backfill(
        dates=dates,
        query=query,
        table_name=output_table_name,
        options=options,
        spark=spark
    )


def backfill_ent_global_features(
    schema,
    role,
    user,
    password,
    dates,
    output_table_name,
    spark,
    query=query_ent_global_l28d_sales,
):
    """Backfill global level features for enterprise

    Args:
        schema                    (str): databricks schema
        role                      (str): user role in snowflake
        user                      (str): databricks user credential
        password                  (str): databricks password credential
        dates                     (str): prediction dates to backfill
        output_table_name         (str): the table name for output
        spark                     (spark): databricks spark
        query                     (str): query to pull features

    Returns:
        None, backfilled data is uploaded to snowflake

    """
    options, _ = set_access_param(
        schema, role, user, password
    )
    _backfill(
        dates=dates,
        query=query,
        table_name=output_table_name,
        options=options,
        spark=spark
    )


def backfill_prediction_features_danp(
    schema,
    role,
    user,
    password,
    dates,
    output_table_name,
    spark,
    query=query_danp,
):
    """Backfill features for selection models (danp mx)

    Args:
        schema                    (str): databricks schema
        role                      (str): user role in snowflake
        user                      (str): databricks user credential
        password                  (str): databricks password credential
        dates                     (str): prediction dates to backfill
        output_table_name         (str): the table name for output
        spark                     (spark): databricks spark
        query                     (str): query to pull features

    Returns:
        None, backfilled data is uploaded to snowflake

    """
    options, _ = set_access_param(
        schema, role, user, password
    )
    _backfill(
        dates=dates,
        query=query,
        table_name=output_table_name,
        options=options,
        spark=spark
    )


def backfill_prediction_features_nn(
    schema,
    role,
    user,
    password,
    dates,
    output_table_name,
    spark,
    query=query_nn,
):
    """Backfill features for selection models (net new mx)

    Args:
        schema                    (str): databricks schema
        role                      (str): user role in snowflake
        user                      (str): databricks user credential
        password                  (str): databricks password credential
        dates                     (str): prediction dates to backfill
        output_table_name         (str): the table name for output
        spark                     (spark): databricks spark
        query                     (str): query to pull features

    Returns:
        None, backfilled data is uploaded to snowflake

    """
    options, _ = set_access_param(
        schema, role, user, password
    )
    _backfill(
        dates=dates,
        query=query,
        table_name=output_table_name,
        options=options,
        spark=spark
    )


def backfill_search_features_danp(
    schema,
    role,
    user,
    password,
    dates,
    output_table_name,
    spark,
    query=q_search_data_danp,
):
    """Backfill search features for selection models (danp mx)

    Args:
        schema                    (str): databricks schema
        role                      (str): user role in snowflake
        user                      (str): databricks user credential
        password                  (str): databricks password credential
        dates                     (str): prediction dates to backfill
        output_table_name         (str): the table name for output
        spark                     (spark): databricks spark
        query                     (str): query to pull features

    Returns:
        None, backfilled data is uploaded to snowflake

    """

    options, _ = set_access_param(
        schema, role, user, password
    )
    _backfill(
        dates=dates,
        query=query,
        table_name=output_table_name,
        options=options,
        spark=spark
    )


def backfill_search_features_nn(
    schema,
    role,
    user,
    password,
    dates,
    output_table_name,
    spark,
    query=q_search_data_nn,
):
    """Backfill search features for selection models (net new mx)

    Args:
        schema                    (str): databricks schema
        role                      (str): user role in snowflake
        user                      (str): databricks user credential
        password                  (str): databricks password credential
        dates                     (str): prediction dates to backfill
        output_table_name         (str): the table name for output
        spark                     (spark): databricks spark
        query                     (str): query to pull features

    Returns:
        None, backfilled data is uploaded to snowflake

    """
    options, _ = set_access_param(
        schema, role, user, password
    )
    _backfill(
        dates=dates,
        query=query,
        table_name=output_table_name,
        options=options,
        spark=spark
    )


def backfill_aov_prediction_features_danp(
    schema,
    role,
    user,
    password,
    dates,
    output_table_name,
    spark,
    query=query_aov_danp,
):
    """Backfill features for selection aov models (danp mx)

    Args:
        schema                    (str): databricks schema
        role                      (str): user role in snowflake
        user                      (str): databricks user credential
        password                  (str): databricks password credential
        dates                     (str): prediction dates to backfill
        output_table_name         (str): the table name for output
        spark                     (spark): databricks spark
        query                     (str): query to pull features

    Returns:
        None, backfilled data is uploaded to snowflake

    """
    options, _ = set_access_param(
        schema, role, user, password
    )
    _backfill(
        dates=dates,
        query=query,
        table_name=output_table_name,
        options=options,
        spark=spark
    )


def backfill_aov_prediction_features_nn(
    schema,
    role,
    user,
    password,
    dates,
    output_table_name,
    spark,
    query=query_aov_nn,
):
    """Backfill features for selection aov models (net new mx)

    Args:
        schema                    (str): databricks schema
        role                      (str): user role in snowflake
        user                      (str): databricks user credential
        password                  (str): databricks password credential
        dates                     (str): prediction dates to backfill
        output_table_name         (str): the table name for output
        spark                     (spark): databricks spark
        query                     (str): query to pull features

    Returns:
        None, backfilled data is uploaded to snowflake

    """
    options, _ = set_access_param(
        schema, role, user, password
    )
    _backfill(
        dates=dates,
        query=query,
        table_name=output_table_name,
        options=options,
        spark=spark
    )


def backfill_performance_metrics_v2(
    schema,
    role,
    user,
    password,
    spark,
    current_dates,
    table_name,
    target_col_name='TOTAL_SUBTOTAL_FIRST_28_DAYS',
    pred_col_name='pred_adj',
    true_col_name='actual',
    lookback_days=30 + 1,
    model_version='v2',
    suffix='',
    factor=1,
    danp_table_with_actual_values='selection_intel_train_v1',
    nn_table_with_actual_values='selection_intel_train_v1',
    include_partner=True
):
    """Backfill performance metrics for selection model predictions

    Args:
        schema                        (str): databricks schema
        role                          (str): user role in snowflake
        user                          (str): databricks user credential
        password                      (str): databricks password credential
        spark                         (spark): databricks spark
        current_dates                 (str): evaluation dates to backfill
                                             same as prediction dates to pull
                                             predictions to compare with
                                             actual sales
        target_col_name               (str): the column name of actual values
        pred_col_name                 (str): the column name of predictions
        true_col_name                 (str): the column name of actual values
                                             in the intermediate combined table
                                             (prediction and actual), hard coded
                                             as 'actual'
        lookback_days                 (int): lookback days to find the prediction
                                             e.g. use the value predicted 150 days ago
        model_version                 (int): the model version,
                                             concatenated with output table names
        suffix                        (str): suffix to concatenate to intermediate table names
        factor                        (float): factor to adjust the prediction, deprecated
        danp_table_with_actual_values (str): the table name of actual values for danp mx
        nn_table_with_actual_values   (str): the table name of actual values for nn mx
        include_partner               (bool): if active parters are incluced in ranking

    Returns:
        None, backfilled data is uploaded to snowflake

    """
    options, params = set_access_param(
        schema, role, user, password
    )

    raw_prediction_table_name = table_name + '_deduped'
    new_prediction_table_name = table_name + '_' \
        + model_version + '_deduped' + '_full'
    for current_date in current_dates:
        pred_date = (
            datetime.strptime(current_date, '%Y-%m-%d') - timedelta(days=lookback_days)
        ).strftime('%Y-%m-%d')
        logger.info("fill current date: {}, pred date: {}".format(
            current_date, pred_date
        ))
        create_prediction_table_with_actual(
            raw_prediction_table_name=raw_prediction_table_name,
            new_prediction_table_name=new_prediction_table_name,
            schema=schema,
            options=options,
            spark=spark,
            test_date=pred_date,
            factor=factor,
            actual_col_name=target_col_name,
            target_raw_pred_name=pred_col_name,
            danp_table_with_actual_values=danp_table_with_actual_values,
            nn_table_with_actual_values=nn_table_with_actual_values,
            model_col_name='model'
        )

        # generate tam tables
        decile_df = generate_mx_rank_df_v1(
            schema=schema,
            params=params,
            options=options,
            spark=spark,
            raw_prediction_table_name=raw_prediction_table_name,
            new_prediction_table_name=new_prediction_table_name,
            test_date=pred_date,
            pred_date=pred_date,
            tam_table_name=table_name + '_' + model_version \
                + '_tam_decile_df_5thmonth_projected_factor_' \
                + str(factor).replace('.', '_') + suffix,
            pred_col_name=pred_col_name,
            include_partner=include_partner
        )
        decile_actual_df = generate_mx_rank_df_v1(
            schema=schema,
            params=params,
            options=options,
            spark=spark,
            raw_prediction_table_name=raw_prediction_table_name,
            new_prediction_table_name=new_prediction_table_name,
            test_date=pred_date,
            pred_date=pred_date,
            tam_table_name=table_name + '_' + model_version \
                + '_tam_decile_actual_df_5thmonth_projected_factor_' \
                + str(factor).replace('.', '_') + suffix,
            pred_col_name=true_col_name,
            include_partner=include_partner
        )

        if decile_df is not None:
            # store tam tables
            decile_df['pred_date'] = pred_date
            decile_df['create_date'] = current_date
            decile_actual_df['pred_date'] = pred_date
            decile_actual_df['create_date'] = current_date
            if model_version == 'v1':
                upload_pd_df(
                    df=decile_df,
                    table_name=table_name + '_' \
                        + model_version + '_decile_df_5thmonth_projected_factor_' \
                        + str(factor).replace('.', '_') + suffix,
                    options=options,
                    spark=spark,
                    mode='append'
                )
                upload_pd_df(
                    df=decile_actual_df,
                    table_name=table_name + '_' + model_version \
                        + '_decile_actual_df_5thmonth_projected_factor_' \
                        + str(factor).replace('.', '_') + suffix,
                    options=options,
                    spark=spark,
                    mode='append'
                )
                decile_data_for_metrics = combine_actual_pred_rank_df(
                    schema, params,
                    decile_actual_df, decile_df,
                    table_name + '_' + model_version + '_deduped' + '_full'
                )
                decile_data_for_metrics['create_date'] = current_date
                upload_pd_df(
                    df=decile_data_for_metrics,
                    table_name=table_name + '_' \
                        + model_version \
                        + '_decile_data_for_metrics_5thmonth_projected_factor_' \
                        + str(factor).replace('.', '_') + suffix,
                    options=options,
                    spark=spark,
                    mode='append'
                )
            else:
                upload_pd_df(
                    df=decile_df,
                    table_name=table_name \
                        + '_decile_df_5thmonth_projected_factor_' \
                        + str(factor).replace('.', '_') + suffix,
                    options=options,
                    spark=spark,
                    mode='append'
                )
                upload_pd_df(
                    df=decile_actual_df,
                    table_name=table_name \
                        + '_decile_actual_df_5thmonth_projected_factor_' \
                        + str(factor).replace('.', '_') + suffix,
                    options=options,
                    spark=spark,
                    mode='append'
                )
                decile_data_for_metrics = combine_actual_pred_rank_df(
                    schema, params,
                    decile_actual_df, decile_df,
                    table_name + '_' + model_version + '_deduped' + '_full'
                )
                decile_data_for_metrics['create_date'] = current_date
                upload_pd_df(
                    df=decile_data_for_metrics,
                    table_name=table_name \
                        + '_decile_data_for_metrics_5thmonth_projected_factor_'\
                        + str(factor).replace('.', '_') + suffix,
                    options=options,
                    spark=spark,
                    mode='append'
                )

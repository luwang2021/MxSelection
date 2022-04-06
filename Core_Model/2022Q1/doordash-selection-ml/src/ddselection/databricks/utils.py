import time
import logging
import mlflow
import mlflow.xgboost
import mlflow.sklearn

from mlflow.tracking.client import MlflowClient
from mlflow.entities.model_registry.model_version_status import (
    ModelVersionStatus
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, FloatType
)

from ddselection.utils import *
from ddselection.data.utils import *
from ddselection.databricks.snowflake import upload_pd_df
from ddselection.metrics.utils import tam_calculation_v1, decile_rank

logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


def _wait_until_ready(model_name, model_version):
    """Wait until the mlflow model is ready"""
    client = MlflowClient()
    for _ in range(10):
        model_version_details = client.get_model_version(
            name=model_name,
            version=model_version,
        )
        status = ModelVersionStatus.from_string(model_version_details.status)
        logger.info("Model status: %s" % ModelVersionStatus.to_string(status))
        if status == ModelVersionStatus.READY:
            break
        time.sleep(1)


# example:
# https://docs.databricks.com/applications/mlflow/model-registry-example.html
# client.delete_registered_model(name="nn_data_preprocess_test")


def mlflow_run(
    datasets,
    model_segments,
    date_column,
    validation_dates,
    activation_date_col='activation_date',
    activation_cohort=True,
    test=False,
    test_suffix='_test_v2_1',
    build_model_fn=None
):
    """Run model fit and register it through mlflow

    Args:
        datasets            (list[pandas DF]): training data sets
        model_segments      (list[str]): model segment names e.g. 'nn', 'ac', etc.
        date_column         (str): the column name for date splitting
                                   and validation
        validation_dates    (list[str]): list of 3 dates indicating
                                         the start of validation date
                                         start of the test date and
                                         the end of the test date
        activation_date_col (str): the activation date of merchants
        activation_cohort   (bool): whether to split the data based on age
        test                (bool): indicate whether it is for test,
                                    if yes, test suffix will be added
                                    to the model name
        test_suffix         (str):  model suffix for testing
        build_model_fn      (func): function to initialize models

    Returns:
        None, fitted models are registered through mlflow

    """
    if test:
        test_suffix = test_suffix
    else:
        test_suffix = ''
    # if not build_model_fn:
    #    build_model_fn = build_model
    with mlflow.start_run():
        for i in range(len(model_segments)):
            seg = model_segments[i]
            model = build_model_fn(seg)
            output, overall_bias, r2 = fit_and_validation(
                models=[model],
                datasets=[datasets[i]],
                dates_cols=date_column,
                train_date=validation_dates[0],
                validation_date=validation_dates[1],
                test_date=validation_dates[2],
                activation_date_col=activation_date_col,
                activation_cohort=activation_cohort,
            )
            # log metrics
            mlflow.log_metrics({'overall_bias' + test_suffix: overall_bias})
            mlflow.log_metrics({'r2_' + seg + test_suffix: r2})

            # log models
            mlflow.sklearn.log_model(
                model.data_process_pipeline,
                artifact_path='sklearn-dataprocess-model',
                registered_model_name=seg + '_data_preprocess' + test_suffix
            )
            mlflow.sklearn.log_model(
                model.ml_model,
                artifact_path='xgboost-model',
                registered_model_name=seg + '_ml_model' + test_suffix
            )
            mlflow.sklearn.log_model(
                model.bias_adj_model,
                artifact_path='sklearn-adj-model',
                registered_model_name=seg + '_bias_adj_model' + test_suffix
            )

            # move to staging
            client = MlflowClient()
            for model_name in [
                seg + '_data_preprocess' + test_suffix,
                seg + '_ml_model' + test_suffix,
                seg + '_bias_adj_model' + test_suffix
            ]:
                model_version_infos = client.search_model_versions(
                    "name = '%s'" % model_name
                )
                new_model_version = max(
                    [
                        int(model_version_info.version)
                        for model_version_info in model_version_infos
                    ]
                )
                _wait_until_ready(model_name, new_model_version)
                # move all other versions except production to archived
                for model_info in model_version_infos:
                    if int(model_info.version) < new_model_version:
                        if model_info.current_stage != 'Production' \
                           and model_info.current_stage != 'Archived':
                            client.transition_model_version_stage(
                                name=model_name,
                                version=int(model_info.version),
                                stage="Archived",
                            )
                client.transition_model_version_stage(
                    name=model_name,
                    version=new_model_version,
                    stage="Staging",
                )


def load_model(
    model_segment,
    model_stage,
    model_suffix='_test'
):
    """Load models registerd in mlflow

    Args:
        model_segment  (str): model segment names e.g. 'nn', 'ac', etc.
        model_stage    (str): model stage in mlflow
        model_suffix   (str): model name suffix
    Returns:
        ml model: data pre process model
        ml model: ml model
        ml model: bias adjustment model
    """
    client = MlflowClient()
    # load pipelines
    data_preprocess_model_name = model_segment + \
        '_data_preprocess' + model_suffix
    data_preprocess_model_version = client.get_latest_versions(
        data_preprocess_model_name, stages=[model_stage]
    )[0].version
    data_preprocess_model_uri = "models:/{model_name}/{model_stage}".format(
        model_name=data_preprocess_model_name, model_stage=model_stage
    )
    data_preprocess_model = mlflow.sklearn.load_model(
        data_preprocess_model_uri
    )
    logger.info('loaded model {} {}'.format(
        data_preprocess_model_uri, data_preprocess_model_version)
    )
    ml_model_name = model_segment + '_ml_model' + model_suffix
    ml_model_version = client.get_latest_versions(
        ml_model_name, stages=[model_stage]
    )[0].version
    ml_model_uri = "models:/{model_name}/{model_stage}".format(
        model_name=ml_model_name, model_stage=model_stage
    )
    ml_model = mlflow.sklearn.load_model(ml_model_uri)
    logger.info('loaded model {} {}'.format(ml_model_uri, ml_model_version))
    bias_adj_model_name = model_segment + '_bias_adj_model' + model_suffix
    bias_adj_model_version = client.get_latest_versions(
        bias_adj_model_name, stages=[model_stage]
    )[0].version
    bias_adj_model_uri = "models:/{model_name}/{model_stage}".format(
        model_name=bias_adj_model_name, model_stage=model_stage
    )
    bias_adj_model = mlflow.sklearn.load_model(bias_adj_model_uri)
    logger.info('loaded model {} {}'.format(
        bias_adj_model_uri, bias_adj_model_version)
    )
    return data_preprocess_model, ml_model, bias_adj_model


def create_prediction_table_with_actual(
    raw_prediction_table_name,
    new_prediction_table_name,
    schema,
    options,
    spark,
    test_date,
    factor=1,
    actual_col_name='TOTAL_SUBTOTAL_FIRST_28_DAYS',
    target_raw_pred_name='pred_adj',
    danp_table_with_actual_values='selection_intel_train_v1',
    nn_table_with_actual_values='selection_intel_train_v1',
    model_col_name='model'
):
    """Combine predicted values with actual values

    Args:
        raw_prediction_table_name     (str): table name of the raw prediction
        new_prediction_table_name     (str): table name for the full data including
                                             prediction and actual values
        schema                        (str): snowflake db schema
        options                       (dict): snowflake access option through databricks
        spark                         (spark): databricks spark
        test_date                     (str): created date
        factor                        (float): factor to adjust the prediction, deprecated
        actual_col_name               (str): the column name of actual values
        target_raw_pred_name          (str): the column name of predictions
        danp_table_with_actual_values (str): the table name of actual values for danp mx
        nn_table_with_actual_values   (str): the table name of actual values for nn mx
        model_col_name                (str): the column name that indicates the model
                                             in the table of predictions
    Returns:
        str: the output table name (the table is created in snowflake)
    """
    query = """
    CREATE OR REPLACE TABLE {schema}.{new_prediction_table_name} AS
    WITH pred_data AS (
        SELECT
              p.match_id
            , mx.restaurant_name external_store_name
            , mx.nimda_id STORE_ID
            , mx.VENDOR_1_ID
            , mx.VENDOR_2_ID
            , ds.name NIMDA_STORE_NAME
            , mx.SUB_ID submarket_id
            , mx.SP_ID starting_point_id
            , p.data_preprocess_model_version
            , p.ml_model_version
            , p.bias_adj_model_version
            , p.{target_raw_pred_name} * {factor} AS {target_raw_pred_name}
            , p.{model_col_name} as model_id
            , '{test_date}' AS create_date
        FROM
            {schema}.{raw_prediction_table_name} p
        LEFT JOIN
            PUBLIC.FACT_SELECTION_INTEL_MX_RAW mx
        ON
            p.match_id=mx.id
        LEFT JOIN
            PUBLIC.dimension_store ds
        ON
            mx.NIMDA_ID=ds.store_id
        WHERE
            p.create_date = '{test_date}'
    ), train_data AS (
        SELECT
              store_id
            , match_id
            , MAX(activation_date) AS activation_date
            , AVG(actual) AS actual
        FROM
        (
            SELECT
                  store_id
                , match_id
                , reactivation_timestamp AS activation_date
                , {actual_col_name} AS actual
                --, TOTAL_SUBTOTAL_FIRST_120_148_DAYS AS actual
            FROM
                {schema}.{danp_table_with_actual_values}
            WHERE
                reactivation_timestamp = dateadd('DAY', 1, '{test_date}')
                AND {actual_col_name} is not NULL
                AND create_date IN (select max(create_date) from {schema}.{danp_table_with_actual_values})

            UNION

            SELECT
                  store_id
                , match_id
                , STORE_ACTIVATION_DATE AS activation_date
                , {actual_col_name} AS actual
                --, TOTAL_SUBTOTAL_FIRST_120_148_DAYS AS actual
            FROM
                {schema}.{nn_table_with_actual_values}
            WHERE
                STORE_ACTIVATION_DATE = dateadd('DAY', 1, '{test_date}')
                AND {actual_col_name} is not NULL
                --AND TOTAL_SUBTOTAL_FIRST_120_148_DAYS is not NULL
                AND create_date IN (select max(create_date) from {schema}.{nn_table_with_actual_values})
         ) a
         GROUP BY store_id, match_id
    )
    SELECT
          train_data.activation_date as activation_date_actual
        , CASE
              WHEN train_data.actual is NULL THEN pred_data.{target_raw_pred_name}
              ELSE train_data.actual
          END AS actual
        , pred_data.*
    FROM
        pred_data
    LEFT JOIN
        train_data
    ON
        train_data.match_id = pred_data.match_id

    """
    spark._jvm.net\
         .snowflake\
         .spark\
         .snowflake\
         .Utils.runQuery(
             options,
             query.format(
                 schema=schema,
                 raw_prediction_table_name=raw_prediction_table_name,
                 new_prediction_table_name=new_prediction_table_name,
                 test_date=test_date,
                 factor=factor,
                 actual_col_name=actual_col_name,
                 target_raw_pred_name=target_raw_pred_name,
                 danp_table_with_actual_values=danp_table_with_actual_values,
                 nn_table_with_actual_values=nn_table_with_actual_values,
                 model_col_name=model_col_name,
             ))
    return new_prediction_table_name


def generate_mx_rank_df_v1(
    schema,
    params,
    options,
    spark,
    raw_prediction_table_name,
    new_prediction_table_name,
    test_date,
    pred_date,
    tam_table_name='tam_pred_test',
    pred_col_name='pred_adj',
    include_partner=True
):
    """Calculate tam and create the decile rank

    Args:
        schema                    (str): databricks schema
        params                    (dict): snowflake params
        options                   (dict): options for databricks spark access to snowflake
        spark                     (spark): databricks spark
        raw_prediction_table_name (str): the table name of raw predictions, deprecated
        new_prediction_table_name (str): the table name of updated predictions
        test_date                 (str): the date for tam calculation
        pred_date                 (str): the date of predictions
        tam_table_name            (str): the intermediate tam table name
        pred_col_name             (str): the column name of predictions
        include_partner           (bool): if active parters are incluced in ranking
    Returns:
        pandas DF: df with decile ranks

    """
    logger.info("calculate tam values")
    tam_df = tam_calculation_v1(
        prediction_table_name=new_prediction_table_name,
        schema=schema,
        params=params,
        pred_col_name=pred_col_name,
        testdate=test_date,
        pred_date=pred_date
    )
    spark_df_schema = StructType([
        StructField("match_id", StringType(), True),
        StructField("store_id", FloatType(), True),
        StructField("vendor_1_id", StringType(), True),
        StructField("vendor_2_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("national", FloatType(), True),
        StructField("submarket_id", FloatType(), True),
        StructField("starting_point_id", FloatType(), True),
        StructField("is_active", IntegerType(), True),
        StructField("is_partner", IntegerType(), True),
        StructField("tam_value", FloatType(), True),
        StructField("model_id", StringType(), True),
        StructField("dte", StringType(), True)
    ])
    # create tam prediction table
    logger.info("create the tam prediction table: {}".format(tam_table_name))
    if len(tam_df) > 0:
        logger.info("shape: ({}, {})".format(tam_df.shape[0], tam_df.shape[1]))
        upload_pd_df(
            tam_df,
            table_name=tam_table_name,
            options=options,
            spark=spark,
            spark_df_schema=spark_df_schema
        )
    else:
        logger.info("tam prediction table empty!")
        return None
    logger.info("calculate decile ranks")
    decile_df = decile_rank(
        tam_table_name,
        schema=schema,
        params=params,
        target_col_name='tam_value',
        include_partner=include_partner
    )
    return decile_df

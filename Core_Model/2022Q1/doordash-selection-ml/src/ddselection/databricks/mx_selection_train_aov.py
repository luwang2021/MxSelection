from datetime import datetime, timedelta

from ddselection.data.mx_selection_aov_v2_data_load import (
    load_mxselection_aov_training_data_v2
)
from ddselection.data.mx_selection_aov_v0_data_load import (
    load_mxselection_aov_training_data_v0
)
from ddselection.databricks.snowflake import *
from ddselection.databricks.utils import *
from ddselection.models_aov_v2 import build_model as build_model_v2
from ddselection.config_aov_v2 import DATE_ACTIVATION_COL as DATE_ACTIVATION_COL_V2, DATE_SPLIT_COL as DATE_SPLIT_COL_V2
from ddselection.models_aov_v0 import build_model as build_model_v0
from ddselection.config_aov_v0 import DATE_ACTIVATION_COL as DATE_ACTIVATION_COL_V0, DATE_SPLIT_COL as DATE_SPLIT_COL_V0


def train_aov(
    schema,
    role,
    user,
    password,
    test_date,
    data_load_func,
    date_split_col,
    date_activation_col,
    build_model_func,
    model_suffix,
):
    """Train and register selection aov models

    Args:
        schema        (str): databricks schema
        role          (str): user role in snowflake
        user          (str): databricks user credential
        password      (str): databricks password credential
        test_date     (str): start date for validation
        data_load_func(func): a function to load training data
        date_split_col(str): str denotes the date column for validate split
        date_activation_col(str): str denotes the activation date column
        model_suffix  (str): model name in mlflow
    Returns:
        None, models are registerd through mlflow

    """
    nn_train, ac_train, da_train = data_load_func(
        schema, role, user, password
    )

    # train & predict date for validation
    today_str = test_date
    today = datetime.strptime(today_str, '%Y-%m-%d')

    mx_activation_start_date_validation = \
        (today - timedelta(days=30 * 7)).strftime('%Y-%m-%d')
    mx_activation_start_date_test = \
        (today - timedelta(days=30 * 6)).strftime('%Y-%m-%d')
    mx_activation_end_date_test = \
        (today - timedelta(days=30 * 5)).strftime('%Y-%m-%d')

    dataset = (
        nn_train, ac_train, da_train
    )
    mlflow_run(
        dataset,
        model_segments=('nn', 'ac', 'da'),
        date_column=date_split_col,
        validation_dates=(
            mx_activation_start_date_validation,
            mx_activation_start_date_test,
            mx_activation_end_date_test
        ),
        activation_date_col=date_activation_col,
        activation_cohort=False,
        test=True,
        test_suffix=model_suffix,
        build_model_fn=build_model_func
    )


def train_aov_v0(
    schema,
    role,
    user,
    password,
    test_date,
    model_suffix,
):
    """Train and register selection aov V2 models

    Args:
        schema        (str): databricks schema
        role          (str): user role in snowflake
        user          (str): databricks user credential
        password      (str): databricks password credential
        test_date     (str): start date for validation
        model_suffix  (str): model name in mlflow
    Returns:
        None, models are registerd through mlflow

    """
    return train_aov(
        schema=schema,
        role=role,
        user=user,
        password=password,
        test_date=test_date,
        data_load_func=load_mxselection_aov_training_data_v0,
        date_split_col=DATE_SPLIT_COL_V0[0],
        date_activation_col=DATE_ACTIVATION_COL_V0[0],
        build_model_func=build_model_v0,
        model_suffix=model_suffix
    )


def train_aov_v2(
    schema,
    role,
    user,
    password,
    test_date,
    model_suffix,
):
    """Train and register selection aov V2 models

    Args:
        schema        (str): databricks schema
        role          (str): user role in snowflake
        user          (str): databricks user credential
        password      (str): databricks password credential
        test_date     (str): start date for validation
        model_suffix  (str): model name in mlflow
    Returns:
        None, models are registerd through mlflow

    """
    return train_aov(
        schema=schema,
        role=role,
        user=user,
        password=password,
        test_date=test_date,
        data_load_func=load_mxselection_aov_training_data_v2,
        date_split_col=DATE_SPLIT_COL_V2[0],
        date_activation_col=DATE_ACTIVATION_COL_V2[0],
        build_model_func=build_model_v2,
        model_suffix=model_suffix
    )

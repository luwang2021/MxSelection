import pandas as pd

from ddselection.data.mx_selection_aov_v2_data_load import (
    load_mxselection_aov_prediction_data_v2
)
from ddselection.data.mx_selection_aov_v1_data_load import (
    load_mxselection_aov_prediction_data_v1
)
from ddselection.data.mx_selection_aov_v0_data_load import (
    load_mxselection_aov_prediction_data_v0
)
from ddselection.models_aov_v0 import forecast_sales as forecast_sales_v0
from ddselection.models_aov_v1 import forecast_sales as forecast_sales_v1
from ddselection.models_aov_v2 import forecast_sales as forecast_sales_v2


def gen_predictions_aov(
    schema,
    role,
    user,
    password,
    pred_date,
    data_load_func,
    forecast_func,
    model_suffix='_test_v2_1',
    mlflow_version=None
):
    """Load register selection aov models and predict

    Args:
        schema        (str): databricks schema
        role          (str): user role in snowflake
        user          (str): databricks user credential
        password      (str): databricks password credential
        pred_date     (str): the date of prediction
        data_load_func(func): a function to load prediction data
        model_suffix  (str): model name in mlflow
        mlflow_version(int): the model version of the model in mlflow
    Returns:
        pandas DF: a pandas DF including all predictions

    """
    nn_pred_features, ac_pred_features, da_pred_features = \
        data_load_func(
            pred_date=pred_date, schema=schema,
            role=role, user=user, password=password
        )

    nn_forecast = forecast_func(
        nn_pred_features,
        model_segment='nn',
        model_stage='staging',
        test=True,
        test_suffix=model_suffix,
        mlflow_version=mlflow_version
    )
    ac_forecast = forecast_func(
        ac_pred_features,
        model_segment='ac',
        model_stage='staging',
        test=True,
        test_suffix=model_suffix,
        mlflow_version=mlflow_version
    )
    da_forecast = forecast_func(
        da_pred_features,
        model_segment='da',
        model_stage='staging',
        test=True,
        test_suffix=model_suffix,
        mlflow_version=mlflow_version
    )

    return pd.concat(
        [
            nn_forecast,
            ac_forecast,
            da_forecast,
        ]
    )


def gen_predictions_aov_v0(
    schema,
    role,
    user,
    password,
    pred_date,
    model_suffix='_test_v2_1',
    mlflow_version=None
):
    """Generate predictions of aov V1 model

    Args:
        schema        (str): databricks schema
        role          (str): user role in snowflake
        user          (str): databricks user credential
        password      (str): databricks password credential
        pred_date     (str): the date of prediction
        model_suffix  (str): model name in mlflow
        mlflow_version(int): the model version of the model in mlflow
    Returns:
        pandas DF: a pandas DF including all predictions

    """
    return gen_predictions_aov(
        schema=schema,
        role=role,
        user=user,
        password=password,
        pred_date=pred_date,
        data_load_func=load_mxselection_aov_prediction_data_v0,
        forecast_func=forecast_sales_v0,
        model_suffix=model_suffix,
        mlflow_version=mlflow_version
    )


def gen_predictions_aov_v1(
    schema,
    role,
    user,
    password,
    pred_date,
    model_suffix='_test_v2_1',
    mlflow_version=None
):
    """Generate predictions of aov V1 model

    Args:
        schema        (str): databricks schema
        role          (str): user role in snowflake
        user          (str): databricks user credential
        password      (str): databricks password credential
        pred_date     (str): the date of prediction
        model_suffix  (str): model name in mlflow
        mlflow_version(int): the model version of the model in mlflow
    Returns:
        pandas DF: a pandas DF including all predictions

    """
    return gen_predictions_aov(
        schema=schema,
        role=role,
        user=user,
        password=password,
        pred_date=pred_date,
        data_load_func=load_mxselection_aov_prediction_data_v1,
        forecast_func=forecast_sales_v1,
        model_suffix=model_suffix,
        mlflow_version=mlflow_version
    )


def gen_predictions_aov_v2(
    schema,
    role,
    user,
    password,
    pred_date,
    model_suffix='_test_v2_1',
    mlflow_version=None
):
    """Generate predictions of aov V2 model

    Args:
        schema        (str): databricks schema
        role          (str): user role in snowflake
        user          (str): databricks user credential
        password      (str): databricks password credential
        pred_date     (str): the date of prediction
        model_suffix  (str): model name in mlflow
        mlflow_version(int): the model version of the model in mlflow
    Returns:
        pandas DF: a pandas DF including all predictions

    """
    return gen_predictions_aov(
        schema=schema,
        role=role,
        user=user,
        password=password,
        pred_date=pred_date,
        data_load_func=load_mxselection_aov_prediction_data_v2,
        forecast_func=forecast_sales_v2,
        model_suffix=model_suffix,
        mlflow_version=mlflow_version
    )

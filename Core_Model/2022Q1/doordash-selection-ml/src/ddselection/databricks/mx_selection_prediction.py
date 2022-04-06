import pandas as pd

from ddselection.data.mx_selection_data_load import (
    load_mxselection_prediction_data
)
from ddselection.models import forecast_sales


def gen_predictions_cohort(
    schema,
    role,
    user,
    password,
    pred_date,
    model_suffix='_test_v2_1',
    mlflow_version=None
):
    """Load register selection models and predict

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
    nn_pred_features, ac_pred_features, da_pred_features = \
        load_mxselection_prediction_data(
            pred_date=pred_date, schema=schema,
            role=role, user=user, password=password
        )

    # local and enterprise segments
    # hard coded
    nn_pred_local_features = \
        nn_pred_features[nn_pred_features.cohort == 0].copy()
    ac_pred_local_features = \
        ac_pred_features[ac_pred_features.cohort == 0].copy()
    da_pred_local_features = \
        da_pred_features[da_pred_features.cohort == 0].copy()
    nn_pred_ent_features = \
        nn_pred_features[nn_pred_features.cohort != 0].copy()
    ac_pred_ent_features = \
        ac_pred_features[ac_pred_features.cohort != 0].copy()
    da_pred_ent_features = \
        da_pred_features[da_pred_features.cohort != 0].copy()

    # predictions for local & ent segments

    nn_local_forecast = forecast_sales(
        nn_pred_local_features,
        model_segment='nn',
        model_stage='staging',
        test=True,
        test_suffix='_local' + model_suffix,
        mlflow_version=mlflow_version
    )
    ac_local_forecast = forecast_sales(
        ac_pred_local_features,
        model_segment='ac',
        model_stage='staging',
        test=True,
        test_suffix='_local' + model_suffix,
        mlflow_version=mlflow_version
    )
    da_local_forecast = forecast_sales(
        da_pred_local_features,
        model_segment='da',
        model_stage='staging',
        test=True,
        test_suffix='_local' + model_suffix,
        mlflow_version=mlflow_version
    )
    nn_ent_forecast = forecast_sales(
        nn_pred_ent_features,
        model_segment='nn',
        model_stage='staging',
        test=True,
        test_suffix='_ent' + model_suffix,
        mlflow_version=mlflow_version
    )
    ac_ent_forecast = forecast_sales(
        ac_pred_ent_features,
        model_segment='ac',
        model_stage='staging',
        test=True,
        test_suffix='_ent' + model_suffix,
        mlflow_version=mlflow_version
    )
    da_ent_forecast = forecast_sales(
        da_pred_ent_features,
        model_segment='da',
        model_stage='staging',
        test=True,
        test_suffix='_ent' + model_suffix,
        mlflow_version=mlflow_version
    )

    return pd.concat(
        [
            nn_local_forecast,
            ac_local_forecast,
            da_local_forecast,
            nn_ent_forecast,
            ac_ent_forecast,
            da_ent_forecast
        ]
    )

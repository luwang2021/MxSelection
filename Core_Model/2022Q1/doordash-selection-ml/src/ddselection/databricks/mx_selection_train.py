from datetime import datetime, timedelta

from ddselection.data.mx_selection_data_load import (
    load_mxselection_training_data
)
from ddselection.databricks.snowflake import *
from ddselection.databricks.utils import *
from ddselection.models import build_model
from ddselection.config import DATE_ACTIVATION_COL, DATE_SPLIT_COL


def train_cohort(
    schema,
    role,
    user,
    password,
    test_date,
    model_suffix,
):
    """Train and register selection models

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
    nn_train, ac_train, da_train = load_mxselection_training_data(
        schema, role, user, password
    )

    # train & predict date for validation
    today_str = test_date
    today = datetime.strptime(today_str, '%Y-%m-%d')

    mx_activation_start_date_validation = \
        (today - timedelta(days=30 * 4)).strftime('%Y-%m-%d')
    mx_activation_start_date_test = \
        (today - timedelta(days=30)).strftime('%Y-%m-%d')
    mx_activation_end_date_test = today.strftime('%Y-%m-%d')

    # local segment
    nn_local_train = nn_train[nn_train.cohort == 0].copy()
    ac_local_train = ac_train[ac_train.cohort == 0].copy()
    da_local_train = da_train[da_train.cohort == 0].copy()

    # enterprise segment
    nn_ent_train = nn_train[nn_train.cohort != 0].copy()
    ac_ent_train = ac_train[ac_train.cohort != 0].copy()
    da_ent_train = da_train[da_train.cohort != 0].copy()

    # save local models
    dataset_local = (
        nn_local_train, ac_local_train, da_local_train
    )
    mlflow_run(
        dataset_local,
        model_segments=('nn', 'ac', 'da'),
        date_column=DATE_SPLIT_COL[0],
        validation_dates=(
            mx_activation_start_date_validation,
            mx_activation_start_date_test,
            mx_activation_end_date_test
        ),
        activation_date_col=DATE_ACTIVATION_COL[0],
        activation_cohort=True,
        test=True,
        test_suffix='_local' + model_suffix,
        build_model_fn=build_model
    )

    # save enterprise models
    dataset_ent = (
        nn_ent_train, ac_ent_train, da_ent_train
    )
    mlflow_run(
        dataset_ent,
        model_segments=('nn', 'ac', 'da'),
        date_column=DATE_SPLIT_COL[0],
        validation_dates=(
            mx_activation_start_date_validation,
            mx_activation_start_date_test,
            mx_activation_end_date_test
        ),
        activation_date_col=DATE_ACTIVATION_COL[0],
        activation_cohort=True,
        test=True,
        test_suffix='_ent' + model_suffix,
        build_model_fn=build_model
    )

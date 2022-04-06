import pandas as pd
import numpy as np
import logging

from sklearn.metrics import r2_score
from scipy.stats.stats import pearsonr, spearmanr, kendalltau

logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


# offline validation
def split_data(
    df, date_col, date1, date2, date3,
    activation_date_col="activation_date",
    age_col='age_month', activation_cohort=False
):
    """Split data by dates to create train, validation, and test

    Args:
        df                  (pandas DF): a df with date columns to split
        date_col            (str): the column name for date splitting
        date1               (str): the end date of training data
                                   or the start date of validation data
        date2               (str): the end date of validation data
                                   or the start date of the test data
        date3               (str): the end date of the test data
        activation_date_col (str): the activation date of merchants
        age_col             (str): the column name to indicate the merchant age
        activation_cohort   (bool): whether to split based on age

    Returns:
        pandas DFs: training, validation and testing data

    """
    train_ind = df[date_col] <= date1
    val_ind = (df[date_col] > date1) & (df[date_col] <= date2)
    if activation_cohort:
        test_ind = (df[activation_date_col] > date2) & \
            (df[activation_date_col] <= date3) & \
            (df[age_col] == 5)
    else:
        test_ind = (df[date_col] > date2) & (df[date_col] <= date3)
    return df.loc[train_ind], df.loc[val_ind], df.loc[test_ind]


def model_fit(
    model,
    df_train,
    df_dev=None
):
    """Fit selection models, a wrapper to call model.fit

    Args:
        model    (ml model): a model to fit
        df_train (pandas DF): training data
        df_dev   (pandas DF): validation data, if None then no validation

    Returns:
        None
    """
    if df_dev is not None:
        model.fit(
            df_train[model.num_var + model.cat_var],
            df_train[[model.output_column]],
            eval_set=[(df_dev, df_dev[[model.output_column]])],
            eval_metric='rmse',
            early_stopping_rounds=10,
            verbose=True
        )
    else:
        model.fit(
            df_train[model.num_var + model.cat_var],
            df_train[[model.output_column]],
            verbose=True
        )


def predict(
    model,
    df_dev,
    df_test
):
    """Model prediction, a wrapper to call prediction func

    Args:
        model   (ml model): a fitted model
        df_dev  (pandas DF): validation data
        df_test (pandas DF): testing data

    Returns:
        pandas DF: a df with predictions and the model's meta data

    """
    model_dev_pred = model.predict(df_dev)
    model.bias_adj_model.fit(
        model_dev_pred[['pred']],
        df_dev[[model.output_column]]
    )
    model_test_pred = model.predict(df_test)
    model_test_pred['pred_adj'] = model.bias_adj_model.predict(
        model_test_pred[['pred']]
    )
    model_test_pred['actual'] = df_test[model.output_column]
    model_test_pred['model'] = model.model_name
    model_test_pred['model_version'] = model.model_version
    return model_test_pred


def refit_predict_adj(
    model,
    df_train,
    df_dev,
    df_test
):
    """Model refit and prediction

    Args:
        model    (ml model): a model to fit
        df_train (pandas DF): training data
        df_dev   (pandas DF): validation data, if None then no validation
        df_test  (pandas DF): testing data

    Returns:
        pandas DF: a df with predictions and the model's meta data

    """
    # refit with dev set
    model_fit(model, pd.concat([df_train, df_dev]))
    model_test_pred = model.predict(df_test)
    model_test_pred['actual'] = df_test[model.output_column]
    model_test_pred['model'] = model.model_name
    model_test_pred['model_version'] = model.model_version
    return model_test_pred


def fit_and_validation(
    models,
    datasets,
    dates_cols,
    train_date,
    validation_date,
    test_date,
    activation_date_col="activation_date",
    activation_cohort=False
):
    """Selection Model fit and validation

    Args:
        models              (list[ml model]): models to fit
        datasets            (list[pandas DF]): datasets for training
        dates_cols          (str): the column name for date splitting
        train_date          (str): the end date of training data
                                   or the start date of validation data
        validation_date     (str): the end date of validation data
                                   or the start date of the test data
        test_date           (str): the end date of the test data
        activation_date_col (str): the activation date of merchants
        activation_cohort   (bool): whether to split the data based on age

    Returns:
        pandas DF: predictions with models' meta data
        float: overall bias
        float: overall R squared

    """
    df_output = []
    for i in range(len(models)):
        df_train, df_dev, df_test = split_data(
            datasets[i],
            dates_cols,
            train_date,
            validation_date,
            test_date,
            activation_date_col=activation_date_col,
            activation_cohort=activation_cohort
        )
        logger.info("fit and validation")
        logger.info("training data shape: ({}, {})".format(
            df_train.shape[0],
            df_train.shape[1])
        )
        logger.info("dev data shape: ({}, {})".format(
            df_dev.shape[0],
            df_dev.shape[1])
        )
        logger.info("test data shape: ({}, {})".format(
            df_test.shape[0],
            df_test.shape[1])
        )
        model = models[i]
        model_fit(model, df_train, df_dev)

        train_pred = model.predict(df_train)
        train_r_score = pearsonr(
            df_train[model.output_column],
            train_pred.pred
        )
        logger.info(
            'training performance pearson correlation: {} \
            spearmanr: {} kendalltau: {} bias: {}'.format(
                train_r_score,
                spearmanr(
                    df_train[model.output_column],
                    train_pred.pred
                ),
                kendalltau(
                    df_train[model.output_column],
                    train_pred.pred
                ),
                np.sum(train_pred.pred) /
                np.sum(df_train[model.output_column]) - 1
            )
        )
        dev_pred = model.predict(df_dev)
        dev_r_score = pearsonr(df_dev[model.output_column], dev_pred.pred)
        logger.info(
            'dev performance pearson correlation: {} \
            spearmanr: {} kendalltau: {} bias: {}'.format(
                dev_r_score,
                spearmanr(df_dev[model.output_column], dev_pred.pred),
                kendalltau(df_dev[model.output_column], dev_pred.pred),
                np.sum(dev_pred.pred) / np.sum(df_dev[model.output_column]) - 1
            )
        )
        model_pred_df = refit_predict_adj(model, df_train, df_dev, df_test)
        df_output.append(model_pred_df)

    # test performance overall
    output = pd.concat(df_output)
    overall_bias = np.sum(output.pred) / np.sum(output.actual) - 1
    r2 = r2_score(output.actual, output.pred)
    logger.info(
        'testing performance pearson correlation: {} \
        spearmanr: {} kendalltau: {}'.format(
            pearsonr(output.actual, output.pred),
            spearmanr(output.actual, output.pred),
            kendalltau(output.actual, output.pred)
        )
    )
    logger.info("r2 : {}, bias: {}".format(r2, overall_bias))
    return output, overall_bias, r2

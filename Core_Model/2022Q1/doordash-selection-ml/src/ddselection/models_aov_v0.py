# high aov model for mx pricing, built in Q3 2020, discontinued
# same as aov_v1 except the objective is different
import pandas as pd
import numpy as np
import logging
import xgboost
import mlflow
import mlflow.xgboost
import mlflow.sklearn

from mlflow.tracking.client import MlflowClient
from sklearn.linear_model import LinearRegression
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer

from ddselection.config_aov_v0 import *

logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


class SelectionModel(object):
    def __init__(
        self,
        store_name_column,
        num_var,
        cat_var=None,
        passthrough_columns=None,
        model_version=MODEL_VERSION,
        ml_model_name='xgb',
        output_column='total_subtotal_first_120_148_days'
    ):
        """
        """
        self.num_var = num_var
        self.cat_var = cat_var
        self.passthrough_columns = passthrough_columns
        self.model_version = model_version
        self.output_column = output_column
        self.store_name_column = store_name_column
        self.data_process_pipeline = None
        self.ml_model = None
        self.bias_adj_model = None

    def build_data_process_pipeline(self):
        num_transformer = Pipeline(
            [
                ('impute', SimpleImputer()),
                ('scaler', StandardScaler())
            ]
        )
        cat_transformer = Pipeline(
            [
                ('impute', SimpleImputer(strategy='most_frequent')),
                ('onehot', OneHotEncoder(
                    categories='auto',
                    sparse=False,
                    handle_unknown='ignore'))
            ]
        )
        if self.cat_var and self.num_var:
            self.data_process_pipeline = ColumnTransformer(
                transformers=[
                    ('numerical_process', num_transformer, self.num_var),
                    ('cat_process', cat_transformer, self.cat_var)
                ],
            )
        elif self.num_var:
            self.data_process_pipeline = ColumnTransformer(
                transformers=[
                    ('numerical_process', num_transformer, self.num_var),
                ],
            )
        elif self.cat_var:
            self.data_process_pipeline = ColumnTransformer(
                transformers=[
                    ('cat_process', cat_transformer, self.cat_var)
                ],

            )
        else:
            return None
        return self.data_process_pipeline

    def build_model(self, model='xgb'):
        if model == 'xgb':
            self.ml_model = xgboost.XGBRegressor(
                objective='binary:logistic',
                n_estimators=108,
                eta=0.1,
                max_depth=6,
                reg_lambda=1,
                n_jobs=16,
                max_bin=180,
                random_state=42,
                verbosity=3
            )
        return self.ml_model

    def build_bias_adj_module(self, model='linear'):
        if model == 'linear':
            self.bias_adj_model = LinearRegression()
        return self.bias_adj_model

    def data_preprocess(self, X, fit=False):
        if self.data_process_pipeline:
            if fit:
                X_train_processed = self.data_process_pipeline.\
                    fit_transform(X[self.num_var + self.cat_var])
            else:
                X_train_processed = self.data_process_pipeline.\
                    transform(X[self.num_var + self.cat_var])
            onehot_cols = ''
            if self.cat_var:
                # add col names to X_train_processed
                onehot_cols = self.data_process_pipeline.\
                    named_transformers_['cat_process'].\
                    named_steps['onehot'].\
                    get_feature_names(input_features=self.cat_var)
            all_feature_cols = self.num_var + list(onehot_cols)
            X_processed_pd = pd.DataFrame(X_train_processed,
                                          columns=all_feature_cols)
            return all_feature_cols, X_processed_pd
        return X.columns, X

    def fit(
        self,
        X_train,
        Y_train,
        eval_set=None,
        **kwargs
    ):
        all_feature_cols, X_train_processed_pd = self.\
            data_preprocess(X_train[self.num_var + self.cat_var], fit=True)
        eval_set_processed = []
        if eval_set is not None:
            for X, y in eval_set:
                tmp = self.data_process_pipeline.\
                    transform(X[self.num_var + self.cat_var])
                eval_set_processed.append(
                    (pd.DataFrame(tmp, columns=all_feature_cols), y)
                )
            self.ml_model.fit(
                X=X_train_processed_pd,
                y=Y_train,
                eval_set=eval_set_processed,
                **kwargs
            )
        else:
            self.ml_model.fit(
                X=X_train_processed_pd,
                y=Y_train,
                **kwargs
            )
        return X_train_processed_pd, self.data_process_pipeline, self.ml_model

    def predict(self, X):
        """
        """
        # TODO change to a feature dictionary
        X_processed = self.data_process_pipeline.\
            transform(X[self.num_var + self.cat_var])
        onehot_cols = ''
        if self.cat_var:
            onehot_cols = self.data_process_pipeline. \
                named_transformers_['cat_process'].\
                named_steps['onehot'].\
                get_feature_names(input_features=self.cat_var)
        all_feature_cols = self.num_var + list(onehot_cols)
        X_processed_pd = pd.DataFrame(X_processed, columns=all_feature_cols)
        sol = X[self.passthrough_columns].copy()
        sol['pred'] = np.reshape(
            self.ml_model.predict(X_processed_pd), (-1, 1)
        )
        return sol

    @staticmethod
    def predict_with_pipelines(
            X,
            num_cols,
            cat_cols,
            passthrough_columns,
            data_process_pipeline,
            ml_model,
            output_col_name='pred'
    ):
        """
        """
        # TODO change to a feature dictionary
        X_processed = data_process_pipeline.transform(X[num_cols + cat_cols])
        onehot_cols = ''
        if len(cat_cols) > 0:
            onehot_cols = data_process_pipeline. \
                named_transformers_['cat_process']. \
                named_steps['onehot']. \
                get_feature_names(input_features=cat_cols)
        all_feature_cols = num_cols + list(onehot_cols)
        X_processed_pd = pd.DataFrame(X_processed, columns=all_feature_cols)
        sol = X[passthrough_columns].copy()
        sol[output_col_name] = np.reshape(
            ml_model.predict(X_processed_pd), (-1, 1)
        )
        return sol


class ActiveNonPartnersModel(SelectionModel):
    def __init__(
        self,
        store_name_column,
        num_var,
        cat_var=None,
        passthrough_columns=None,
        model_version=MODEL_VERSION,
        ml_model_name='xgb',
        output_column='total_subtotal_first_120_148_days'
    ):
        """
        """
        super().__init__(
            num_var=num_var,
            store_name_column=store_name_column,
            cat_var=cat_var,
            passthrough_columns=passthrough_columns,
            ml_model_name=ml_model_name,
            output_column=output_column
        )
        self.model_name = 'ac'
        self.build_data_process_pipeline()
        self.build_model(model=ml_model_name)
        self.build_bias_adj_module()


class NetNewModel(SelectionModel):
    def __init__(
        self,
        store_name_column,
        num_var,
        cat_var,
        passthrough_columns=None,
        model_version=MODEL_VERSION,
        ml_model_name='xgb',
        output_column='total_subtotal_first_120_148_days'
    ):
        """
        """
        super().__init__(
            store_name_column=store_name_column,
            num_var=num_var,
            cat_var=cat_var,
            passthrough_columns=passthrough_columns,
            ml_model_name=ml_model_name,
            output_column=output_column
        )
        self.model_name = 'nn'
        self.build_data_process_pipeline()
        self.build_model(model=ml_model_name)
        self.build_bias_adj_module()

    def build_model(self, model='xgb'):
        if model == 'xgb':
            self.ml_model = xgboost.XGBRegressor(
                objective='binary:logistic',
                n_estimators=100,
                # , eta=0.1
                subsample=0.8,
                max_depth=5,
                learning_rate=0.1,
                # , reg_lambda=1
                n_jobs=16,
                # , max_bin=120
                random_state=42,
                verbosity=3
            )
        return self.ml_model


class DeactivatedModel(SelectionModel):
    def __init__(
        self,
        store_name_column,
        num_var,
        cat_var,
        passthrough_columns=None,
        model_version=MODEL_VERSION,
        ml_model_name='xgb',
        output_column='total_subtotal_first_120_148_days'
    ):
        """
        """
        super().__init__(
            store_name_column=store_name_column,
            num_var=num_var,
            cat_var=cat_var,
            passthrough_columns=passthrough_columns,
            ml_model_name=ml_model_name,
            output_column=output_column
        )
        self.model_name = 'da'
        self.build_data_process_pipeline()
        self.build_model(model=ml_model_name)
        self.build_bias_adj_module()

    def build_model(self, model='xgb'):
        if model == 'xgb':
            self.ml_model = xgboost.XGBRegressor(
                objective='binary:logistic',
                n_estimators=98,
                eta=0.1,
                max_depth=5,
                reg_lambda=1,
                n_jobs=16,
                max_bin=120,
                random_state=42,
                verbosity=3,
            )
        return self.ml_model


def build_model(model_name=None):
    model = None
    if model_name:
        if model_name == 'nn':
            model = NetNewModel(
                store_name_column='external_store_name',
                num_var=NN_NUM_COLS,
                cat_var=NN_CAT_COLS,
                passthrough_columns=PASSTHROUGH_COLS,
                output_column=NN_Y_COL[0]
            )
        elif model_name == 'ac':
            model = ActiveNonPartnersModel(
                store_name_column='external_store_name',
                num_var=AC_NUM_COLS,
                cat_var=AC_CAT_COLS,
                passthrough_columns=PASSTHROUGH_COLS,
                output_column=AC_Y_COL[0]
            )
        elif model_name == 'da':
            model = DeactivatedModel(
                store_name_column='external_store_name',
                num_var=DA_NUM_COLS,
                cat_var=DA_CAT_COLS,
                passthrough_columns=PASSTHROUGH_COLS,
                output_column=DA_Y_COL[0]
            )
    return model


def build_models():
    return build_model('nn'), build_model('ac'), build_model('da')


def forecast_sales(
    data,
    model_segment,
    model_stage,
    test=False,
    test_suffix='_test_v2_1',
    mlflow_version=None
):

    if test:
        test_suffix = test_suffix
    else:
        test_suffix = ''
    client = MlflowClient()
    # load pipelines
    data_preprocess_model_name = \
        model_segment + '_data_preprocess' + test_suffix
    if mlflow_version:
        data_preprocess_model_uri = \
            "models:/{model_name}/{model_version}".format(
                model_name=data_preprocess_model_name,
                model_version=mlflow_version
            )
        data_preprocess_model = \
            mlflow.sklearn.load_model(data_preprocess_model_uri)
        logger.info('loaded {} {}'.format(
            data_preprocess_model_uri,
            mlflow_version)
        )

        ml_model_name = model_segment + '_ml_model' + test_suffix
        ml_model_uri = \
            "models:/{model_name}/{model_version}".format(
                model_name=ml_model_name,
                model_version=mlflow_version
            )
        ml_model = mlflow.sklearn.load_model(ml_model_uri)
        logger.info('loaded {} {}'.format(ml_model_uri, mlflow_version))

        bias_adj_model_name = model_segment + '_bias_adj_model' + test_suffix
        bias_adj_model_uri = \
            "models:/{model_name}/{model_version}".format(
                model_name=bias_adj_model_name,
                model_version=mlflow_version
            )
        bias_adj_model = mlflow.sklearn.load_model(bias_adj_model_uri)
        logger.info('loaded {} {}'.format(bias_adj_model_uri, mlflow_version))
        data_preprocess_model_version = ml_model_version = \
            bias_adj_model_version = mlflow_version

    else:
        data_preprocess_model_version = client.get_latest_versions(
            data_preprocess_model_name,
            stages=[model_stage]
        )[0].version
        data_preprocess_model_uri = \
            "models:/{model_name}/{model_stage}".format(
                model_name=data_preprocess_model_name,
                model_stage=model_stage
            )
        data_preprocess_model = \
            mlflow.sklearn.load_model(data_preprocess_model_uri)
        logger.info(
            'loaded {} {}'.format(
                data_preprocess_model_uri,
                data_preprocess_model_version)
        )

        ml_model_name = model_segment + '_ml_model' + test_suffix
        ml_model_version = client.get_latest_versions(
            ml_model_name,
            stages=[model_stage]
        )[0].version
        ml_model_uri = \
            "models:/{model_name}/{model_stage}".format(
                model_name=ml_model_name,
                model_stage=model_stage
            )
        ml_model = mlflow.sklearn.load_model(ml_model_uri)
        logger.info('loaded {} {}'.format(ml_model_uri, ml_model_version))

        bias_adj_model_name = model_segment + '_bias_adj_model' + test_suffix
        bias_adj_model_version = client.get_latest_versions(
            bias_adj_model_name,
            stages=[model_stage]
        )[0].version
        bias_adj_model_uri = \
            "models:/{model_name}/{model_stage}".format(
                model_name=bias_adj_model_name,
                model_stage=model_stage
            )
        bias_adj_model = mlflow.sklearn.load_model(bias_adj_model_uri)
        logger.info('loaded {} {}'.format(
            bias_adj_model_uri, bias_adj_model_version)
        )

    model_pred = SelectionModel.predict_with_pipelines(
        data,
        num_cols=SEGMENT_FEATURE_MAP[model_segment]['num'],
        cat_cols=SEGMENT_FEATURE_MAP[model_segment]['cat'],
        passthrough_columns=PASSTHROUGH_COLS,
        data_process_pipeline=data_preprocess_model,
        ml_model=ml_model
    )

    model_pred['model'] = \
        model_segment + '_' + MODEL_VERSION + '_' + model_stage
    model_pred['data_preprocess_model_version'] = data_preprocess_model_version
    model_pred['ml_model_version'] = ml_model_version
    model_pred['bias_adj_model_version'] = bias_adj_model_version

    return model_pred

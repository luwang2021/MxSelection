import logging
import pandas as pd
import numpy as np
import snowflake.connector


logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


def set_access_param(schema, role, user, password):
    options = dict(
        sfurl="doordash.snowflakecomputing.com/",
        sfaccount="DOORDASH",
        sfuser=user,
        sfRole=role,
        sfpassword=password,
        sfdatabase="PRODDB",
        sfschema=schema,
        sfwarehouse="ADHOC"
    )

    params = dict(
        user=user,
        password=password,
        role=role,
        account='DOORDASH',
        database='PRODDB',
        schema=schema,
        warehouse='ADHOC'
    )
    return options, params


def load_data(params, query):
    """Loads data from snowflake"""
    with snowflake.connector.connect(**params) as ctx:
        df = pd.read_sql(query, ctx)
        df.columns = [col.lower() for col in df.columns]
    logger.info('loaded data shape: {}'.format(df.shape))
    return df


class DataLoader:
    def __init__(self, dataset, schema, role, user, password):
        self._options, self._params = set_access_param(
            schema, role, user, password
        )
        self._data_queries = dataset.query
        self._date_cols = dataset.date_cols
        self._selected_cols = dataset.selected_cols

    def load_pd(self):
        pd_data = load_data(self._params, self._data_queries)
        if self._date_cols:
            for name in self._date_cols:
                pd_data[name] = pd_data[name].astype(str)
        return pd_data[self._selected_cols]


class Dataset:
    def __init__(self, name, query, selected_cols, date_cols=None):
        self.name = name
        self.query = query
        self.date_cols = date_cols
        self.selected_cols = selected_cols
        if len(np.unique(self.selected_cols)) != len(self.selected_cols):
            raise ValueError("duplications in selected cols")

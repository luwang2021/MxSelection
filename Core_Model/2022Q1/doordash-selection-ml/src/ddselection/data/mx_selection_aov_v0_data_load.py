from ddselection.data.mx_selection_aov_v0_training_data_query import *
from ddselection.data.mx_selection_aov_v0_prediction_data_query import *
from ddselection.data.utils import *
from ddselection.config_aov_v0 import *


logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


def load_mxselection_aov_training_data_v0(schema, role, user, password):
    """Load training data for selection aov v0 models

    Args:
        schema                    (str): databricks schema
        role                      (str): user role in snowflake
        user                      (str): databricks user credential
        password                  (str): databricks password credential

    Returns:
        pandas DFs: training data for
                    nn(net new), ac(active np) and da(deactivated) mx

    """
    nn_dataset = Dataset(
        name='nn',
        query=q_aov_training_data_nn_v0,
        date_cols=DATE_SPLIT_COL,
        selected_cols=(
            NN_NUM_COLS + NN_CAT_COLS + PASSTHROUGH_COLS
            + NN_Y_COL
        )
    )
    ac_dataset = Dataset(
        name='ac',
        query=q_aov_training_data_ac_v0,
        date_cols=DATE_SPLIT_COL,
        selected_cols=(
            AC_NUM_COLS + AC_CAT_COLS + PASSTHROUGH_COLS
            + AC_Y_COL
        )
    )
    da_dataset = Dataset(
        name='da',
        query=q_aov_training_data_da_v0,
        date_cols=DATE_SPLIT_COL,
        selected_cols=(
            DA_NUM_COLS + DA_CAT_COLS + PASSTHROUGH_COLS
            + DA_Y_COL
        )
    )

    logger.info('load net new mx data')
    nn_train = DataLoader(nn_dataset, schema, role, user, password).load_pd()
    logger.info('load active mx np')
    ac_train = DataLoader(ac_dataset, schema, role, user, password).load_pd()
    logger.info('load deactivated mx data')
    da_train = DataLoader(da_dataset, schema, role, user, password).load_pd()
    return nn_train, ac_train, da_train


def load_mxselection_aov_prediction_data_v0(
    pred_date,
    schema,
    role,
    user,
    password
):

    """Load prediction features for selection aov v0 models

    Args:
        pred_date                 (str): prediction date
        schema                    (str): databricks schema
        role                      (str): user role in snowflake
        user                      (str): databricks user credential
        password                  (str): databricks password credential

    Returns:
        pandas DFs: prediction features for
                    nn(net new), ac(active np) and da(deactivated) mx

    """
    nn_pred_features_dataset = Dataset(
        name='nn',
        query=query_nn_pred_features_aov_v0.format(test_date=pred_date),
        date_cols=None,
        selected_cols=(
            NN_NUM_COLS + NN_CAT_COLS + PASSTHROUGH_COLS
        )
    )
    ac_pred_features_dataset = Dataset(
        name='ac',
        query=query_ac_pred_features_aov_v0.format(test_date=pred_date),
        date_cols=None,
        selected_cols=(
            AC_NUM_COLS + AC_CAT_COLS + PASSTHROUGH_COLS
        )
    )
    da_pred_features_dataset = Dataset(
        name='da',
        query=query_da_pred_features_aov_v0.format(test_date=pred_date),
        date_cols=None,
        selected_cols=(
            DA_NUM_COLS + DA_CAT_COLS + PASSTHROUGH_COLS
        )
    )
    logger.info('load net new mx data for prediction')
    nn_pred_features = DataLoader(
        nn_pred_features_dataset, schema, role, user, password
    ).load_pd()
    logger.info('load active mx np data for prediction')
    ac_pred_features = DataLoader(
        ac_pred_features_dataset, schema, role, user, password
    ).load_pd()
    logger.info('load deactivated mx data for prediction')
    da_pred_features = DataLoader(
        da_pred_features_dataset, schema, role, user, password
    ).load_pd()

    return nn_pred_features, ac_pred_features, da_pred_features

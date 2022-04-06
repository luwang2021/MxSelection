from ddselection\
    .data\
    .mx_selection_training_label_net_new_query import query_nn_train
from ddselection\
    .data\
    .mx_selection_training_label_daacnp_query import query_da_acnp_train
from ddselection.data.utils import set_access_param


def mxselection_training_label_gen(
    schema,
    role,
    user,
    password,
    start_date,
    end_date,
    nn_table_name,
    danp_table_name,
    spark,
    mode='append'
):
    """Training data generation

    Args:
        schema          (str): databricks schema
        role            (str): user role in snowflake
        user            (str): databricks user credential
        password        (str): databricks password credential
        start_date      (str): start sales date of the training data
        end_date        (str): end sales date of the training data
        nn_table_name   (str): the output table of new new mx
        danp_table_name (str): the output table of danp mx
        spark           (str): databricks spark
        model_suffix    (str): model name in mlflow
    Returns:
        None, models are registerd through mlflow

    """
    options, _ = set_access_param(
        schema, role, user, password
    )
    query_net_new_df = spark.read.format("snowflake")\
                            .options(**options)\
                            .option(
                                "query",
                                query_nn_train.format(
                                    test_date=start_date,
                                    cur_date=end_date
                                )).load()

    query_da_acnp_df = spark.read.format("snowflake")\
                            .options(**options)\
                            .option(
                                "query",
                                query_da_acnp_train.format(
                                    test_date=start_date,
                                    cur_date=end_date
                                )).load()
    query_net_new_df.write.format("snowflake")\
        .options(**options)\
        .option('dbtable', nn_table_name)\
        .mode(mode)\
        .save()

    spark._jvm.net.snowflake\
         .spark.snowflake\
         .Utils.runQuery(
             options,
             "grant select on {}.{} to role read_only_users".format(
                 options["sfschema"], nn_table_name
             )
         )

    query_da_acnp_df.write.format("snowflake")\
        .options(**options)\
        .option('dbtable', danp_table_name)\
        .mode(mode)\
        .save()

    spark._jvm.net.snowflake\
         .spark.snowflake\
         .Utils.runQuery(
             options,
             "grant select on {}.{} to role read_only_users".format(
                 options["sfschema"], danp_table_name
             )
         )

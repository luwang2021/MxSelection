import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

# databricks snowflake utility functions


def upload_pd_df(
    df,
    table_name,
    options,
    spark,
    spark_df_schema=None,
    mode='overwrite'
):
    """Function to upload pandas pd using spark

    Args:
        df              (pandas DF): the df to upload
        table_name      (str): the table name to upload/create
        options         (dict): options for databricks spark access to snowflake
        spark           (spark): databricks spark
        spark_df_schema (spark schema): table schema
        mode            (str): 'overwrite' or 'append'
    Returns:
        str: created / uploaded table name
    """
    if spark_df_schema:
        spark.createDataFrame(df, schema=spark_df_schema)\
             .write.format("snowflake")\
             .options(**options)\
             .option('dbtable', table_name)\
             .mode(mode)\
             .save()
    else:
        spark.createDataFrame(df)\
             .write.format("snowflake")\
             .options(**options)\
             .option('dbtable', table_name)\
             .mode(mode)\
             .save()
    spark._jvm.net.snowflake\
         .spark.snowflake\
         .Utils.runQuery(
             options,
             "grant select on {}.{} to role read_only_users".
             format(options['sfschema'], table_name)
         )
    return table_name


def drop_table(table_name, schema, options, spark):
    """Drop a table from snowflake, use with caution"""
    drop_query = """
    DROP TABLE IF EXISTS {schema}.{table_name};
    """
    spark._jvm.net.snowflake.spark.\
        snowflake.Utils.runQuery(
            options,
            drop_query.format(
                schema=schema,
                table_name=table_name
            )
        )


def create_table(query, table_name, options, spark):
    """Create a table in snowflake"""
    spark._jvm.net.snowflake.spark.\
        snowflake.Utils.runQuery(options, query)
    spark._jvm.net.snowflake\
         .spark.snowflake\
         .Utils.runQuery(
             options,
             "grant select on {} to role read_only_users".format(table_name)
         )


def update_table(query, options, spark):
    """Update a table in snowflake through the query, use with caution"""
    spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(options, query)


def load_data_w_dates_spark(query, test_date, options, spark):
    """Load the data from snowflake, the query might have 'test_date' to specify"""
    df = spark.read.format("snowflake")\
              .options(**options)\
              .option("query", query.format(test_date=test_date)).load()
    return df


def write_spark_df_to_snowflake(
    spark_df,
    table_name,
    options,
    spark,
    mode='append'
):
    """Write a spark df to snowflake"""
    spark_df.write.format("snowflake")\
            .options(**options)\
            .option('dbtable', table_name)\
            .mode(mode)\
            .save()


def batch_upload_pd_df(
    df,
    table_name,
    options,
    spark,
    bsize=1000000,
    mode="append"
):
    """Batch uploading a big df to snowflake"""
    n_row = df.shape[0]
    n_batch = n_row // bsize
    # overwrite / append the first batch
    write_mode = mode
    for batch in range(n_batch):
        logger.info("upload batch : {}".format(batch))
        upload_pd_df(
            df=df.iloc[
                (batch * bsize): (batch + 1) * bsize,
            ],
            table_name=table_name,
            options=options,
            mode=write_mode,
        )
        write_mode = "append"

    logger.info("upload last batch, size: {}".format(n_row - n_batch * bsize))
    upload_pd_df(
        df=df.iloc[
            (n_batch * bsize):,
        ],
        table_name=table_name,
        options=options,
        spark=spark,
        mode=write_mode,
    )

    spark._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
        options,
        "grant select on {}.{} to role read_only_users".format(
            options["sfschema"], table_name
        ),
    )
    return table_name

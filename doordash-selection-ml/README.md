# Selection Intel ML 
ML models that drive Doordash merchant selection 

## Run on databricks
1. Install the selection ml package through databricks notebook
    1. select a cluster with databricks runtime 7.6+ ML and install required packages listed in `requirements.txt`
    e.g.
    ```
    %sh
    pip install 'joblib==0.17.0'
	pip install 'shap==0.35.0'
	pip install 'xgboost==1.0.0'
	pip install 'scikit-learn==0.23.1'
	pip install 'pyarrow==0.17.0'
    pip install 'mlflow==1.11.0'
    pip install 'snowflake-connector-python==2.3.6'
    ```
    2. install the package from the databricks folder (the example below is for production run)
    ```
    %pip install --force-reinstall /dbfs/FileStore/jars/mxselection/prod/doordash_selection_models-*.*.*-py3-none-any.whl
    ```
    3. import the package
    ```
    import ddselection
    ```
2. Mx selection model training and registration through mlflow (example [notebook](https://doordash.cloud.databricks.com/#notebook/447108/command/447109))
    ```
    from ddselection.databricks.mx_selection_train import train_cohort
    train_cohort(
        schema=user_schema, 
        role=user_role, 
        user=user, 
        password=password, 
        test_date='2020-05-05', # for validation
        model_suffix='_model_version'
    )
    ```
3. Mx selection batch scoring (example [notebook](https://doordash.cloud.databricks.com/#notebook/447201/command/447202))
    ```
    from ddselection.databricks.mx_selection_prediction import gen_predictions_cohort
    pred_df = gen_predictions_cohort(
        schema=user_schema, 
        role=user_role, 
        user=user, 
        password=password,
        pred_date='2020-07-01', 
        model_suffix='_model_version'
    )
    ```
4. Selection ml data
    1. training data (example [notebook](https://doordash.cloud.databricks.com/#notebook/477231/command/483598))
    ```
    from ddselection.databricks.mx_selection_training_label_gen import mxselection_training_label_gen
    mxselection_training_label_gen(
        schema=user_schema,
        role=user_role,
        user=user,
        password=password,
        start_date='2019-01-04', # the start date to collect training labels
        end_date='2021-01-31', # the end date to collect training labels
        nn_table_name=NN_TABLE_NAME, # output table for net new mx
        danp_table_name=DANP_TABLE_NAME, # output table for danp mx
        spark=spark, mode='append'
    )

    ```
    2. validation data (example [notebook](https://doordash.cloud.databricks.com/#notebook/476091/command/476092))
    
    The 5th month sales after the activation date of each merchant is used as the true label to validate the model performance.
    
    3. backfill features (example [notebook](https://doordash.cloud.databricks.com/#notebook/459498/command/459499))
    
    All features need to be backfilled to do training and prediciton. 
5. Contribute to selection models
    1. define traning and prediction data 
        1. add queries for [training](https://github.com/doordash/doordash-selection-ml/blob/master/src/ddselection/data/mx_selection_aov_caviar_v2_training_data_query.py) and [prediction](https://github.com/doordash/doordash-selection-ml/blob/master/src/ddselection/data/mx_selection_aov_caviar_v2_prediction_data_query.py) data. Upstream tables might need to define and add to the [config](https://github.com/doordash/doordash-selection-ml/blob/master/src/ddselection/data/query_config.py) file.
        2. define data loaders for training and prediction [link](https://github.com/doordash/doordash-selection-ml/blob/master/src/ddselection/data/mx_selection_aov_v2_data_load.py).
    2. add new models and corresponding configs
        1. check the selection [model](https://github.com/doordash/doordash-selection-ml/blob/master/src/ddselection/models.py) & [config](https://github.com/doordash/doordash-selection-ml/blob/master/src/ddselection/config.py) vs aov [model](https://github.com/doordash/doordash-selection-ml/blob/master/src/ddselection/models_aov_v2.py) and [config](https://github.com/doordash/doordash-selection-ml/blob/master/src/ddselection/config_aov_v2.py) for more details.
    3. add [training](https://github.com/doordash/doordash-selection-ml/blob/master/src/ddselection/databricks/mx_selection_train_aov.py) and [prediction](https://github.com/doordash/doordash-selection-ml/blob/master/src/ddselection/databricks/mx_selection_aov_prediction.py) functions


## For package development and testing
1. Build a Python wheel package and upload it to the selection databricks folder 
    1. `git clone` the package to a local directory with [virtual environment](https://docs.python.org/3/tutorial/venv.html) (python >= 3.6) created and packages in `requirements.txt` installed
    2. run `python -m build` to build wheels in the local directory
    3. `dbfs cp` to move the package file to the mx selection databricks folder
    	1. `dbfs cp path_to_the_package/dist/*.whl dbfs:/FileStore/jars/mxselection/dev` for development
    	2. `dbfs cp path_to_the_package/dist/*.whl dbfs:/FileStore/jars/mxselection/prod` for production




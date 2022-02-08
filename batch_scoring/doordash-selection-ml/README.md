# Batch scoring logic for Selection Intel ML 
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
    pip install 'mlflow==1.23.0'
    pip install 'snowflake-connector-python==2.3.6'
    ```
    2. install the package from the databricks folder (the example below is for production run)
    ```
    %pip install --force-reinstall /dbfs/FileStore/jars/mxselection/prod/doordash_selection_models-batch-*.*.*-py3-none-any.whl
    ```
    3. import the package
    ```
    import ddselection
    ```

2. Mx selection batch scoring (example [notebook](https://doordash.cloud.databricks.com/#notebook/447201/command/447202))
    ```
    from ddselection.databricks.mx_selection_prediction import gen_predictions_cohort
    pred_df = gen_predictions_cohort(
        schema=user_schema, 
        role=user_role, 
        user=user, 
        password=password,
        pred_date='2022-02-07', 
        model_suffix='_model_version'
    )
    ```




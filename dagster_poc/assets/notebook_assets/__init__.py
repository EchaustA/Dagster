import dagster as dg

@dg.asset(
    kinds={"databricks", "pyspark", "bronze"},
    metadata={
        "databricks_notebook_path": "/Workspace/current_user/sample_notebook1.ipynb"
    }
)
def databricks_notebook_asset_1():
    return dg.Output(None)

@dg.asset(
    kinds={"databricks", "pyspark", "bronze"},
    metadata={
        "databricks_notebook_path": "/Workspace/current_user/sample_notebook2.ipynb"
    }
)
def databricks_notebook_asset_2():
    return dg.Output(None)

@dg.asset(
    kinds={"databricks", "pyspark", "bronze"},
    deps=[
        databricks_notebook_asset_1,
        databricks_notebook_asset_2
    ],
    metadata={
        "notebook_path": "/Workspace/current_user/sample_notebook3.ipynb"
    }
)
def databricks_downstream_notebook_asset_1():
    return dg.Output(None)



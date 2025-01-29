import dagster as dg
from .resources.sample_io_manager import SampleIOManager
from .resources.sample_spark_resource import SampleSparkResource
from .assets import (
    notebook_assets
)
import random

@dg.sensor(
    name="databricks_notebook_assets_sensor",
    job_name="databricks_job_run",
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30
    # asset_selection='*'
)
def databricks_notebook_assets_sensor(context: dg.SensorEvaluationContext):
    # context.instance.get_asset_records()

    # context

    # dg.AssetRecor
    
    asset_keys = [dg.AssetKey("databricks_notebook_asset_1"), dg.AssetKey("databricks_notebook_asset_2")]
    if random.choice(range(1,4)) % 2 == 0:
        yield dg.RunRequest(
            # run_key="NothingSpecific",
            run_config={"ops": {"log_asset_selection": {"config": {"asset_keys": asset_keys}}}}
            # job_name="databricks_job_run",
            # asset_selection=[dg.AssetKey("databricks_notebook_asset_1"), dg.AssetKey("databricks_notebook_asset_2")]
        )
    else:
        yield dg.SkipReason("Whatever reason")


class AssetSelectionConfig(dg.Config):
    asset_keys: list[dg.AssetKey]

@dg.op()
def log_asset_selection(context: dg.OpExecutionContext) -> list[dg.AssetKey]:
    # Access the asset keys from the config
    asset_keys = context.op_config["asset_keys"]
    context.log.info(f"Inferred asset keys: {asset_keys}")
    return asset_keys


@dg.op()
def map_asset_to_notebook(context: dg.OpExecutionContext, asset_keys: list[dg.AssetKey]) -> dict[dg.AssetKey, str]:
    asset_to_notebook_map = {}

    asset_graph = defs.get_asset_graph()
    for asset_key in asset_keys:
        # dg.DagsterInstance().get_
        metadata = asset_graph.get(asset_key).metadata
        # metadata = context.assets_def.get_asset_spec(asset_key).metadata
        # context.get_output_metadata()
        # Assuming metadata contains the notebook path under the key 'notebook_path'
        # metadata = context.instance.get_latest_materialization_event(asset_key).asset_materialization.metadata
        notebook_path = metadata.get('databricks_notebook_path', None)
        if notebook_path:
            asset_to_notebook_map[asset_key] = notebook_path
            context.log.info(f"Inferred notebook path for {asset_key}: {notebook_path}")
        else:
            context.log.warning(f"No notebook path found for {asset_key}")
    return asset_to_notebook_map

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute
@dg.op()
def prepare_submit_tasks(context: dg.OpExecutionContext, asset_notebook_map: dict[dg.AssetKey, str]) -> list[jobs.SubmitTask]:
    submit_tasks = []
    for asset_key, notebook_path in asset_notebook_map.items():
        submit_tasks.append(
            jobs.SubmitTask(
                task_key=asset_key.to_user_string(),
                notebook_task=jobs.NotebookTask(
                    notebook_path=notebook_path
                ),
                # new_cluster=compute.ClusterSpec(
                #     clu
                # )
            )
        )
    context.log.info("Successfully formatted SubmitTask list")
    return submit_tasks


def submit_databricks_job_run(context: dg.OpExecutionContext, w: WorkspaceClient, submit_tasks: list[jobs.SubmitTask]):
    
    w.jobs.submit(
        run_name=f"Dagster job run ID: {context.run_id}",
        tasks=submit_tasks
    )
    
    context.log.info("Submitting databricks job run...")


    

@dg.job()
def databricks_job_run():
    asset_keys = log_asset_selection()
    asset_notebook_map = map_asset_to_notebook(asset_keys)
    submit_tasks = prepare_submit_tasks(asset_notebook_map)


    return submit_tasks


    # path1 = notebook_assets.databricks_notebook_asset_1()
    # path2 = notebook_assets.databricks_notebook_asset_2()
    # logger = dg.get_dagster_logger()

    # all_paths = [path1,path2]

    # logger.info(f"Running for: {'\n'.join(all_paths)}")

    # for path in all_paths:
    #     yield dg.AssetMaterialization(
    #         asset_key = asset_key
    #     )

all_assets = dg.with_source_code_references(dg.load_assets_from_package_module(notebook_assets, group_name="databricks_notebooks"))

# all_resources = {
#     "spark_io_manager": SampleIOManager(
        
#     ),
#     "spark_resource1": SampleSparkResource(
#         conn_string="Muahaha"
#     ),
#     "spark_resource2": SampleSparkResource(
#         conn_string="Muahaha"
#     )
# }


all_sensors = [databricks_notebook_assets_sensor]
all_jobs = [databricks_job_run]


defs = dg.Definitions(
    assets=all_assets,
    # resources=all_resources,
    jobs=all_jobs,
    sensors=all_sensors
)

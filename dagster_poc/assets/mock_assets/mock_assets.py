import dagster as dg


@dg.asset(
    io_manager_key="spark_io_manager",
    required_resource_keys="spark_resource1"
)
def upstream_asset(context: dg.AssetExecutionContext):
    return 2


@dg.asset(
    io_manager_key="spark_io_manager",
    required_resource_keys="spark_resource2",
    ins={
        "upstream_asset": dg.AssetIn(
            key=dg.AssetKey("upstream_asset")
        )
    }
)
def downstream_asset(context: dg.AssetExecutionContext, upstream_asset: int):
    return 2 + upstream_asset
import dagster as dg


class SampleSparkResource(dg.ConfigurableResource):
    conn_string: str
    

    def setup_for_execution(self, context: dg.InitResourceContext):
        context.logger.info(
            f'Retrieved resource! {context.resources.spark_resource}'
        )
        self.spark = f"{self.conn_string}"

    def teardown_after_execution(self, context: dg.InitResourceContext):
        pass
import dagster as dg


class SampleIOManager(dg.ConfigurableIOManager):
    sample_input: str

    def load_input(context: dg.InputContext):
        context.log.info(
            f"{context.resources}"
        )
        context.log.info(
            f'Retrieved resource! {context.resources.spark_resource}'
        )
        context.upstream_output.resources
        return 2

    def handle_output(context: dg.OutputContext):
        context.log.info(
            f"{context.resources}"
        )
        context.log.info(
            f'Retrieved resource! {context.resources.spark_resource}'
        )
        return 2
from airflow.models import BaseOperator
from dags.drafts.great_exp.gx_manager import GxManager


class GxOperator(BaseOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):

        gx_manager = GxManager(
            env=self.params["env"],
            source=self.params["source"],
            engine=self.params.get("engine", "athena"),
            conf=self.params,
            params=self.params
        )
        gx_manager.exec()
        gx_manager.build_data_docs()


from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class CustomOperator(BaseOperator):
    @apply_defaults
    def __init__(self, custom_message, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.custom_message = custom_message

    def execute(self, context):
        logging.info(f"Custom message: {self.custom_message}")

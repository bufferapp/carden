from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import logging


class BigQueryWriter:
    """Simple big query buffered writter."""

    def __init__(self, table_id, dataset_id, project_id, client=None, buffer_size=100):
        self.table_id = table_id
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.client = client
        self.rows_buffer = []
        self.buffer_size = buffer_size

    def _flush_rows_buffer(self):
        if self.rows_buffer:
            logging.info(f"Writing {len(self.rows_buffer)} rows to {self.table_id}")

            try:
                errors = self.client.insert_rows_json(
                    self.table_ref,
                    self.rows_buffer,
                    skip_invalid_rows=True,
                    ignore_unknown_values=True,
                )

                if errors:
                    logging.warning(f"Warning, {len(errors)} errors!")
                    logging.warning(errors)
            except BadRequest:
                logging.warning(f"Warning, bad request!")
                logging.warning(BadRequest)

            self.rows_buffer = []

    def __enter__(self):
        if not self.client:
            self.client = bigquery.Client(project=self.project_id)
        self.dataset_ref = self.client.dataset(self.dataset_id)
        self.table_ref = self.dataset_ref.table(self.table_id)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._flush_rows_buffer()

    def write(self, row):
        self.rows_buffer.append(row)
        if len(self.rows_buffer) >= self.buffer_size:
            self._flush_rows_buffer()


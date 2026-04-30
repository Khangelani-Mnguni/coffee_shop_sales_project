import pandas as pd
from google.cloud import bigquery
from schema import SCHEMA


class BigQueryLoader:
    def __init__(self, project_id, dataset, write_mode="append"):
        self.client = bigquery.Client(project=project_id)
        self.dataset = dataset
        self.write_mode = write_mode

    def _prepare_dataframe(self, df: pd.DataFrame, store: str):
        # Normalize column names
        df.columns = [col.lower().strip() for col in df.columns]

        # Enforce schema
        for col, dtype in SCHEMA.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)

        # Add metadata
        df["store_name"] = store
        df["ingestion_timestamp"] = pd.Timestamp.utcnow()

        return df

    def load_csv(self, file_path: str, table_name: str, store: str):
        df = pd.read_csv(file_path)
        df = self._prepare_dataframe(df, store)

        table_id = f"{self.client.project}.{self.dataset}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=(
                bigquery.WriteDisposition.WRITE_APPEND
                if self.write_mode == "append"
                else bigquery.WriteDisposition.WRITE_TRUNCATE
            )
        )

        job = self.client.load_table_from_dataframe(
            df,
            table_id,
            job_config=job_config
        )

        job.result()

        print(f"[SUCCESS] {len(df)} rows loaded → {table_id}")
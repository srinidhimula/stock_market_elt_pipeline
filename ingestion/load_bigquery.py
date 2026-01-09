from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def get_table_ref(client, project, dataset, table):
    return f"{project}.{dataset}.{table}"


def create_table_if_not_exists(client, table_id):
    schema = [
        bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("adjusted_close", "FLOAT"),
        bigquery.SchemaField("volume", "FLOAT"),
        bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
    ]

    try:
        client.get_table(table_id)
        print(f"Table already exists: {table_id}")
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        )
        client.create_table(table)
        print(f"Created table: {table_id}")


def load_dataframe(client, df, table_id):
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    )

    job.result()
    print(f"Loaded {job.output_rows} rows into {table_id}")

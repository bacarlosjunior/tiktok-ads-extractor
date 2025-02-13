import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from datetime import datetime, date, timedelta
import logging
from time import sleep

logger = logging.getLogger()

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.



schema=[

        bigquery.SchemaField("date", bigquery.enums.SqlTypeNames.DATE, mode="REQUIRED"),
        bigquery.SchemaField("campaign_name", bigquery.enums.SqlTypeNames.STRING, mode="REQUIRED"),
        bigquery.SchemaField("campaign_id", bigquery.enums.SqlTypeNames.INTEGER, mode="REQUIRED"),
        bigquery.SchemaField("ad_name", bigquery.enums.SqlTypeNames.STRING, mode="REQUIRED"),
        bigquery.SchemaField("ad_id", bigquery.enums.SqlTypeNames.INTEGER, mode="REQUIRED"),
        bigquery.SchemaField("clicks", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("impressions", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("adgroup_name", bigquery.enums.SqlTypeNames.STRING, mode="REQUIRED"),
        bigquery.SchemaField("adgroup_id", bigquery.enums.SqlTypeNames.INTEGER, mode="REQUIRED"),
        bigquery.SchemaField("spend", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("conversion", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("conversion_rate", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("ctr", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("cpc", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("cpm", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("cost_per_conversion", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("real_time_conversion", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("real_time_cost_per_conversion", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("real_time_conversion_rate", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("result", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("cost_per_result", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("result_rate", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("real_time_result", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("real_time_cost_per_result", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("real_time_result_rate", bigquery.enums.SqlTypeNames.FLOAT, mode="REQUIRED"),
        bigquery.SchemaField("country", bigquery.enums.SqlTypeNames.STRING, mode="REQUIRED"),
    ]

clustering_fields_tiktok = ["date"]

def exist_dataset_table(client, table_id, dataset_id, project_id, schema, clustering_fields=None):
    try:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        client.get_dataset(dataset_ref)  # Make an API request.

    except NotFound:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)  # Make an API request.
        logger.info("Created dataset {}.{}".format(
            client.project, dataset.dataset_id))

    try:
        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
        client.get_table(table_ref)  # Make an API request.

    except NotFound:

        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)

        table = bigquery.Table(table_ref, schema=schema)

        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date"
        )

        if clustering_fields is not None:
            table.clustering_fields = clustering_fields

        table = client.create_table(table)  # Make an API request.
        logger.info("Created table {}.{}.{}".format(
            table.project, table.dataset_id, table.table_id))

    return True

def insert_df_bq(schema,client, table_id, dataset_id, project_id, ads_analytics_data):

    table_ref = '{}.{}.{}'.format(project_id, dataset_id, table_id)
    table = client.get_table(table_ref)
    job_config = bigquery.LoadJobConfig(schema=schema)

    job = client.load_table_from_dataframe(ads_analytics_data,table, job_config = job_config)

    result = job.result()
    if  result == False:
        print('No results in insert_df_bq')
    else:
        print("Success uploaded to table {}".format(table.table_id))
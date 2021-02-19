import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# fetch database and table metadata

# --- using spark sql example ---
# NOTE: `--enable-glue-datacatalog` must be enabled for job
def spark_sql_example():
    spark.sql("use default")
    tables = spark.sql("show tables").rdd.collect()
    output = ''
    for table in tables:
        output += f"-- schema for {table.tableName} ---\n"
        tableDescribe = spark.sql(f"describe `{table.tableName}`").rdd.collect()
        for column in tableDescribe:
            output += f"column name: {column['col_name']}, type: {column['data_type']}\n"
        output +="\n\n\n"

    print(output)

# --- using boto3 example ---

import boto3
import json

def glue_client_example():
    client = boto3.client('glue')
    databases_resp = client.get_databases()
    for database in databases_resp['DatabaseList']:
        database_name = database['Name']
        tables_resp = client.get_tables(DatabaseName=database_name)
        for table in tables_resp['TableList']:
            table_name = table['Name']
            table_resp = client.get_table(DatabaseName=database_name, Name=table_name)
            print(json.dumps(table_resp, indent=2, sort_keys=True, default=str))

def main():
    spark_sql_example()
    glue_client_example()

main()


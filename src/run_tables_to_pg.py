from environs import Env

from pyspark.sql import SparkSession
from src.spark_session import InitSpark

from src.apps.tables_to_pg import (
    extract_raw_data,
    transform_dimension_table,
    transform_fact_table,
)

from src.modules.schemas.dimensions import D_INIT_COL_NAMES

from src.modules.schemas.facts import (
    F_INIT_COL_NAMES,
    F_SPEC_COL_NAMES,
    F_SCHEMA,
)


# initialise Env
env: Env = Env()

# read env variables
env.read_env()

# read env variables
warehouse_location = env('RAW_TO_WAREHOUSE_LOCATION')
app_name = env('WRITE_TO_PG_APP')
user = env('DB_USER')
postgres_db_name = env('DB_NAME')
postgres_db_password = env('DB_PASSWORD')
schema = env('SCHEMA')
d_table = env('DIMENSION_TABLE')
f_table = env('FACT_TABLE')
raw_bucket = env('RAW_DATA_S3_BUCKET')
aws_endpoint_url = env('AWS_ENDPOINT_URL')
aws_access_key_id = env('AWS_ACCESS_KEY_ID')
aws_secret_access_key = env('AWS_SECRET_ACCESS_KEY')
signature_version = env('SIGNATURE_VERSION')

init_spark_session = InitSpark(
    app_name=app_name,
    warehouse_location='spark-warehouse',
    aws_endpoint_url=aws_endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

default_spark_session: SparkSession = init_spark_session.spark_init()

raw_df = extract_raw_data(
    sc=default_spark_session,
    raw_s3_bucket=raw_bucket,
    aws_endpoint_url=aws_endpoint_url,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    signature_version=signature_version,
)

f_revenue = transform_fact_table(
    sc=default_spark_session,
    df=raw_df,
    installed_at_col=F_SPEC_COL_NAMES.get('install_ts'),
    created_at_col=F_SPEC_COL_NAMES.get('created_at'),
    fact_column_names=F_INIT_COL_NAMES,
    schema=F_SCHEMA,
)


d_session = transform_dimension_table(
    sc=default_spark_session,
    df=raw_df,
    dimension_column_names=D_INIT_COL_NAMES,
)

#jdbc_dataset_example(
#    spark_session=default_spark_session,
#    user=user,
#    postgres_db_name=postgres_db_name,
#    postgres_db_password=postgres_db_password,
#    schema=schema,
#    table=d_table,
#)

default_spark_session.stop()



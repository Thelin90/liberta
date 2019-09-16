from environs import Env

from pyspark.sql import SparkSession, DataFrame
from src.spark_session import InitSpark

from src.apps.tables_to_pg import (
    extract_raw_data,
    transform_dimension_table,
    transform_fact_table,
    load_table_to_postgres,
)

from src.modules.schemas.dimensions import (
    D_SESSION_INIT_COL_NAMES,
    D_USER_INIT_COL_NAMES,
    D_SESSION_SCHEMA,
    D_USER_SCHEMA,
)

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
d_user_table = env('DIMENSION_USER_TABLE')
d_session_table = env('DIMENSION_SESSION_TABLE')
f_revenue_table = env('FACT_REVENUE_TABLE')
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

d_session: DataFrame = transform_dimension_table(
    sc=default_spark_session,
    df=raw_df,
    timestamp_columns=['arrival_ts'],
    dimension_column_names=D_SESSION_INIT_COL_NAMES,
    schema=D_SESSION_SCHEMA,
    surrogate_key_name='surr_session_id',
)

d_user: DataFrame = transform_dimension_table(
    sc=default_spark_session,
    df=raw_df,
    timestamp_columns=[
        "client_ts",
        "cohort_month",
        "cohort_week",
        "install_hour",
        "install_ts"
    ],
    dimension_column_names=D_USER_INIT_COL_NAMES,
    schema=D_USER_SCHEMA,
    surrogate_key_name='surr_user_id',
)

f_revenue = transform_fact_table(
    sc=default_spark_session,
    df=raw_df,
    dimension_tables=(d_user, d_session),
    installed_at_col=F_SPEC_COL_NAMES.get('install_ts'),
    created_at_col=F_SPEC_COL_NAMES.get('created_at'),
    fact_column_names=F_INIT_COL_NAMES,
    schema=F_SCHEMA,
)

# print(f_revenue.rdd.count())  # since I added the surrogate key, this get a bit weird, but I guess it will do
# print(d_user.rdd.count())
# print(d_session.rdd.count())

load_table_to_postgres(
    df=f_revenue,
    user=user,
    postgres_db_name=postgres_db_name,
    postgres_db_password=postgres_db_password,
    schema=schema,
    table=f_revenue_table,
)

load_table_to_postgres(
    df=d_session,
    user=user,
    postgres_db_name=postgres_db_name,
    postgres_db_password=postgres_db_password,
    schema=schema,
    table=d_session_table,
)

load_table_to_postgres(
    df=d_user,
    user=user,
    postgres_db_name=postgres_db_name,
    postgres_db_password=postgres_db_password,
    schema=schema,
    table=d_user_table,
)


default_spark_session.stop()



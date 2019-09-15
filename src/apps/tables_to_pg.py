"""
Small script to perform an ETL for the star schema
"""
from typing import Tuple
import pyspark.sql.functions as F

from pyspark import RDD
from pyspark.sql.types import DateType, StructType
from pyspark.sql import SparkSession, DataFrame
from src.helpers.distributed_fetch_executors import DistributedS3Reader

from src.modules.schemas.raw import (
    R_INIT_COL_NAMES,
    R_FINAL_COL_NAMES,
)


def extract_raw_data(
    sc: SparkSession,
    raw_s3_bucket: str,
    aws_endpoint_url: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    signature_version: str,
) -> DataFrame:
    """Method to extract dataset distributed, generic

    :return: Raw format of data as a PySpark Dataframe
    :param sc:
    :param raw_s3_bucket:
    :param aws_endpoint_url:
    :param aws_access_key_id:
    :param aws_secret_access_key:
    :param signature_version:
    :return:
    """
    dist_s3_reader: DistributedS3Reader = DistributedS3Reader(
        spark_context=sc.sparkContext
    )

    raw_rdd: RDD = dist_s3_reader.distributed_read_from_s3(
        s3_bucket=raw_s3_bucket,
        endpoint_url=aws_endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        signature_version=signature_version,
    )

    # explode raw rdd to a DataFrame
    raw_df: DataFrame = sc.read.json(raw_rdd)
    # explode internal values and struct to one single dataframe (fully denormalised)
    raw_df: DataFrame = raw_df.select(*R_INIT_COL_NAMES)
    raw_df = raw_df.select(*R_FINAL_COL_NAMES)

    return raw_df


def transform_fact_table(
    sc: SparkSession,
    df: DataFrame,
    installed_at_col: str,
    created_at_col: str,
    fact_column_names: Tuple[str],
    schema: StructType,
) -> DataFrame:
    """Function transform Fact table, every row is unique

    Regarding if to have or not to have: surrogate key for fact_table
    https://www.kimballgroup.com/2006/07/design-tip-81-fact-table-surrogate-key/

    Quote: Remember, surrogate keys for dimension tables are a great idea. Surrogate keys for fact tables are
    not logically required but can be very helpful in the back room ETL processing.

    -- FACT revenue --
    session_id: str FK
    user_id: FK
    AUD: long
    CZK: long
    CAD: long
    HUF: long
    EUR: long
    GBP: long
    NOK: long
    PHP: long
    RUB: long
    SEK: long
    USD: long
    ZAR: long
    created_at:  # when user installed first time (I think?) (install_ts)

    :param sc: spark session
    :param df: DataFrame,
    :param installed_at_col: str,
    :param created_at_col: str,
    :param fact_column_names: Tuple[str]
    :param schema: the final schema with data types and nullables
    :return
    """

    # I assume that the values in revenue are a sum of all revenue for a given user/session.
    # Example that I found: session_id = c38f0557-92a2-4ddc-9b2a-b4d466be72c9
    # have "SEK": 9900 repeated 57 times in the data set, this to me seems to be
    # the latest updated revenue value. Since fact tables should have unique
    # rows, I therefore drop duplicates where all rows match.
    #
    # Hence, a user can have many different sessions, but the revenue should still be the same
    # this could lead to duplicate rows of the revenue but the session_id (FK) would still be
    # unique, fair I say.
    fact_revenue_df: DataFrame = df.select(*fact_column_names).dropDuplicates()

    # time of first installation of app, can be useful in the fact table, since every row is unique
    # I am thinking to use this for DAU
    fact_revenue_df = fact_revenue_df.withColumn(
        created_at_col,
        F.from_unixtime(installed_at_col).cast(DateType())
    ).drop('install_ts')

    # fast way of changing nullable of columns
    fact_revenue_df = sc.createDataFrame(fact_revenue_df.rdd, schema=schema)

    return fact_revenue_df


def transform_dimension_table(
    sc: SparkSession,
    df: DataFrame,
    dimension_column_names: str,

):
    #df.createOrReplaceTempView("dfTable")
    #sc.sql("SELECT pay_ft from dfTable where pay_ft is not null LIMIT 20").show()
    fact_revenue_df: DataFrame = df.select(*dimension_column_names)

    fact_revenue_df.printSchema()


def jdbc_dataset_example(
        spark_session: SparkSession,
        user: str,
        postgres_db_name: str,
        postgres_db_password: str,
        schema: str,
        table: str,
) -> None:
    jdbcDF = spark_session.read \
        .format('jdbc') \
        .option('url', f'jdbc:postgresql:{postgres_db_name}') \
        .option('dbtable', f'{schema}.{table}') \
        .option("user", f'{user}') \
        .option("password", f'{postgres_db_password}') \
        .load()

    jdbcDF.show(10)

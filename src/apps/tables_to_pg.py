"""
Small script to perform an ETL for the star schema
"""
from typing import Tuple, List
import pyspark.sql.functions as F

from pyspark import RDD
from pyspark.sql.types import TimestampType, StructType
from pyspark.sql import SparkSession, DataFrame, Window
from src.helpers.distributed_fetch_executors import DistributedS3Reader

from src.modules.schemas.raw import (
    R_INIT_COL_NAMES,
    R_MID_COL_NAMES,
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

    # select all given columns, mid stage
    raw_df = raw_df.select(*R_MID_COL_NAMES)

    # select all given columns, final stage, clean redudant data
    raw_df = raw_df.select(*R_FINAL_COL_NAMES).dropDuplicates()

    # surrogate keys for dimensions
    # not sure what to partition by for best performance atm
    raw_df = add_incrementral_id(
        df=raw_df,
        id_column='surr_user_id',
        partitionby='user_id',
    )

    # surrogate keys for dimensions
    # not sure what to partition by for best performance atm
    raw_df = add_incrementral_id(
        df=raw_df,
        id_column='surr_session_id',
        partitionby='session_id',
    )

    # rename id columns, will create surrogate keys instead for better performance with fact table
    #raw_df = raw_df.withColumnRenamed('user_id', 'user')
    #raw_df = raw_df.withColumnRenamed('session_id', 'session')

    # print schema of raw extracted data
    #raw_df.printSchema()

    return raw_df


def add_incrementral_id(df: DataFrame, id_column: str, partitionby: str) -> DataFrame:
    """Will create a surrogate key that starts from 1 and is ordered
    since monotonically_increasing_id will ve unique but not ordered
    OVER (PARTITION BY . . . ORDER BY . . .)

    :param df: pyspark Dataframe
    :param id_column: name of column
    :param partitionby: column to partition by
    :return: new column with a unique and consistent surrogate key
    """
    return df.withColumn(
        id_column,
        #F.monotonically_increasing_id(),
        F.row_number().over(
            Window
            .partitionBy(partitionby)
            .orderBy(
                F.monotonically_increasing_id()
            )
        )
    )


def transform_fact_table(
    sc: SparkSession,
    df: DataFrame,
    dimension_tables: Tuple[DataFrame, DataFrame],
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

    #fact_df: DataFrame = df.select(*fact_column_names).dropDuplicates()

    #fact_df = fact_df.join(
    ##    dimension_tables[0],
    #    fact_df['user_id'] == dimension_tables[0]['user_id']
    #).select(fact_df["*"], dimension_tables[0]["surr_user_id"])

    #fact_df = fact_df.join(
    #    dimension_tables[1],
    #    fact_df['session_id'] == dimension_tables[1]['session_id']
    #).select(fact_df["*"], dimension_tables[1]["surr_session_id"])

    # time of first installation of app, can be useful in the fact table, since every row is unique
    # I am thinking to use this for DAU
    fact_df = df.withColumn(
        created_at_col,
        F.from_unixtime(installed_at_col).cast(TimestampType())
    ).drop('install_ts')

    fact_column_names = ('surr_session_id', 'surr_user_id') + fact_column_names[2:]
    fact_column_names = fact_column_names[:14] + (created_at_col, )
    fact_df = fact_df.select(*fact_column_names)

    # fast way of changing nullable of columns
    fact_df = sc.createDataFrame(fact_df.rdd, schema=schema)

    fact_df = fact_df.dropDuplicates()

    fact_df.printSchema()

    return fact_df


def convert_unix_to_ts(df: DataFrame, timestamp_columns: List[str]):
    """Function to convert unix to timestamp type, not the most performant
    but it works.

    :param df:
    :param timestamp_columns:
    :return:
    """
    for ts in timestamp_columns:
        df = df.withColumn(
            F'{ts}_from_unix',
            F.from_unixtime(ts).cast(TimestampType())
        ).drop(ts)

    return df


def transform_dimension_table(
    sc: SparkSession,
    df: DataFrame,
    dimension_column_names: Tuple[str],
    timestamp_columns: List[str],
    surrogate_key_name: str,
    schema: StructType,
):
    """

    :param sc:
    :param df:
    :param dimension_column_names:
    :param timestamp_columns:
    :param schema:
    :return:
    """
    # convert unix ts to proper format
    dimension_df = convert_unix_to_ts(df, timestamp_columns)

    # modify order, for a primary key
    dimension_column_names = (surrogate_key_name,) + dimension_column_names

    dimension_df: DataFrame = dimension_df.select(*dimension_column_names)

    # recreate dataframe with correct nullables and data types
    dimension_df = sc.createDataFrame(dimension_df.rdd, schema=schema).dropDuplicates()

    # print schema of dimension table
    dimension_df.printSchema()

    return dimension_df


def load_table_to_postgres(
        df: DataFrame,
        user: str,
        postgres_db_name: str,
        postgres_db_password: str,
        schema: str,
        table: str,
) -> None:

    df.repartition(100).write.mode("overwrite") \
        .format('jdbc') \
        .option('url', f'jdbc:postgresql:{postgres_db_name}') \
        .option('dbtable', f'{schema}.{table}') \
        .option("user", f'{user}') \
        .option("password", f'{postgres_db_password}') \
        .option("batchsize", 10000) \
        .option("cascadeTruncate", "true") \
        .save()

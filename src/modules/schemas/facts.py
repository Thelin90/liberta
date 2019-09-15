from pyspark.sql.types import StructField, StructType, IntegerType, LongType, TimestampType

# surrogate key for fact_table https://www.kimballgroup.com/2006/07/design-tip-81-fact-table-surrogate-key/
#
# Quote: Remember, surrogate keys for dimension tables are a great idea. Surrogate keys for fact tables are
# not logically required but can be very helpful in the back room ETL processing.

F_SPEC_COL_NAMES = {
    'created_at': 'created_at',
    'install_ts': 'install_ts',
}

F_INIT_COL_NAMES = (
    "session_id",
    "user_id",
    "AUD",
    "CZK",
    "CAD",
    "HUF",
    "EUR",
    "GBP",
    "NOK",
    "PHP",
    "RUB",
    "SEK",
    "USD",
    "ZAR",
    "install_ts",  # time of first installation of app, can be useful in the fact table, since every row is unique
)

F_SCHEMA = StructType(
    [
        StructField("surr_session_id", IntegerType(), False),
        StructField("surr_user_id", IntegerType(), False),
        StructField("AUD", LongType(), True),
        StructField("CZK", LongType(), True),
        StructField("CAD", LongType(), True),
        StructField("HUF", LongType(), True),
        StructField("EUR", LongType(), True),
        StructField("GBP", LongType(), True),
        StructField("NOK", LongType(), True),
        StructField("PHP", LongType(), True),
        StructField("RUB", LongType(), True),
        StructField("SEK", LongType(), True),
        StructField("USD", LongType(), True),
        StructField("ZAR", LongType(), True),
        StructField("created_at", TimestampType(), False),
    ]
)





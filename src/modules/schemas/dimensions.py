from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    BooleanType,
    DoubleType,
)

D_USER_INIT_COL_NAMES = (
    "user_id",
    "country_code",
    "is_converting",
    "is_paying",
    "origin",
    "pay_ft",
    "first_build",
    "client_ts_from_unix",
    "cohort_month_from_unix",
    "cohort_week_from_unix",
    "install_hour_from_unix",
    "install_ts_from_unix",
)

D_SESSION_INIT_COL_NAMES = (
    "session_id",  # this will later just be session, will add a surrogate key as session_id for performance
    "session_num",
    "arrival_ts_from_unix",
    "amount",
    "attempt_num",
    "category",
    "connection_type",
    "jailbroken",
    "length",
    "limited_ad_tracking",
    "manufacturer",
    "platform",
    "android_id",
    "score",
    "value",
    "v",
)

D_SESSION_SCHEMA = StructType(
    [
        StructField("surr_session_id", IntegerType(), False),
        StructField("session_id", StringType(), False),
        StructField("session_num", LongType(), True),
        StructField("arrival_ts_from_unix", TimestampType(), True),
        StructField("amount", LongType(), True),
        StructField("attempt_num", LongType(), True),
        StructField("category", StringType(), True),
        StructField("connection_type", StringType(), True),
        StructField("jailbroken", BooleanType(), True),
        StructField("length", LongType(), True),
        StructField("limited_ad_tracking", BooleanType(), True),
        StructField("manufacturer", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("android_id", StringType(), True),
        StructField("score", LongType(), True),
        StructField("value", DoubleType(), True),
        StructField("v", LongType(), True),
    ]
)

D_USER_SCHEMA = StructType(
    [
        StructField("surr_user_id", IntegerType(), False),
        StructField("user_id", StringType(), False),
        StructField("country_code", StringType(), True),
        StructField("is_converting", StringType(), True),
        StructField("is_paying", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("pay_ft", LongType(), True),
        StructField("first_build", StringType(), True),
        StructField("client_ts_from_unix", TimestampType(), True),
        StructField("cohort_month_from_unix", TimestampType(), True),
        StructField("cohort_week_from_unix", TimestampType(), True),
        StructField("install_hour_from_unix", TimestampType(), True),
        StructField("install_ts_from_unix", TimestampType(), True),
    ]
)

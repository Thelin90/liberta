from pyspark.sql.types import StructField, StructType, StringType, LongType, DateType

"""

User
----
user_id
country_code:
client_ts
cohort_month
cohort_week
install_hour
install_ts
is_converting: 
is_paying
origin
pay_ft
first_build
----

Session
----
session_id: 
session_num
arrival_ts
amount
attempt_num
category
connection_type
jailbroken
length
limited_ad_tracking
manufacturer
platform
android_id
score
value
v
----

"""

D_INIT_COL_NAMES = (
    "session_id",
    "session_num",
    "arrival_ts",
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

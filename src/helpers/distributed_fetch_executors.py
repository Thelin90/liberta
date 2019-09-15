import logging
from json import dumps, loads
from typing import List

from src.helpers.s3 import get_bucket_files, distributed_fetch
from pyspark import SparkContext, RDD

logger = logging.getLogger(__name__)


class DistributedS3Reader(object):

    def __init__(self, spark_context: SparkContext):
        self.spark_context = spark_context

    def distributed_read_from_s3(
        self,
        s3_bucket: str,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        signature_version: str,
    ) -> RDD:
        """Function fetches s3 files in a distributed fashion, since S3 does not act as HDFS textFile can't
        be trusted. Returns the data as a pyspark RDD.
        :param s3_bucket:
        :param endpoint_url:
        :param aws_access_key_id:
        :param aws_secret_access_key:
        :param signature_version:
        :return: pyspark RDD[Dict[str, Any]]
        """
        try:

            logging.info('get file list of buckets available')
            files: List[str] = get_bucket_files(
                endpoint_url=endpoint_url,
                s3_bucket=s3_bucket,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                signature_version=signature_version
            )

            # Force the reading of files to be distributed among the
            # executors. It will basically take the file, split it up into
            # subparts and speed up the read process.
            #
            # Article about the subject: https://tech.kinja.com/how-not-to-pull-from-s3-using-apache-spark-1704509219
            logging.info('read s3 files distributed')
            raw_rdd = self.spark_context.parallelize(files).flatMap(
                lambda filepath: distributed_fetch(
                    filepath=filepath,
                    s3_bucket=s3_bucket,
                    endpoint_url=endpoint_url,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    signature_version=signature_version
                )
            )

            # convert each dict to valid JSON [Dict[str, Any], Dict[str, Any], Dict[str, Any]]
            raw_rdd = raw_rdd.map(lambda x: dumps(loads(x)))

            return raw_rdd

        except ValueError as err:
            logger.warning(err)

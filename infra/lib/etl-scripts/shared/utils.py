"""Utils class."""
import sys

from pyspark.sql import SparkSession


class Util:
    """Utils class."""

    @staticmethod
    def get_spark_and_glue_session(app_name: str):
        """Generate spark and glue session."""
        from awsglue.context import GlueContext

        spark = SparkSession.builder.appName(app_name).getOrCreate()
        glue = GlueContext(spark)
        return glue, spark

    @staticmethod
    def get_job_args(job_args: [str]) -> dict[str, str]:
        """
        Get the job arguments from the glue job.

        :param job_args: job args
        :return job arguments passed to the glue job
        """
        from awsglue.utils import getResolvedOptions

        return getResolvedOptions(
            sys.argv,
            job_args,
        )

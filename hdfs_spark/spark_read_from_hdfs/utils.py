"""
This module contains utility functions for the Spark applications.
"""

import locale
import os
import re
import subprocess
import platform
from enum import Enum

from pyspark import SparkConf
from pyspark.sql import SparkSession

# Set the locale to the user's default setting
locale.setlocale(locale.LC_ALL, '')

# Get the preferred encoding
preferred_encoding = locale.getencoding()

FS: str = "hdfs://namenode:9000/"

# Determine the appropriate hostname or IP address
try:
    if platform.system() == "Windows":
        # On Windows, get the hostname
        SPARK_DRIVER_HOST = (
            subprocess.check_output(["hostname"]).decode(encoding="utf-8").strip()
        )
    else:
        # On Unix/Linux, use `hostname -i` to get the IP address
        SPARK_DRIVER_HOST = (
            subprocess.check_output(["hostname", "-i"]).decode(encoding="utf-8").strip()
        )

    # Remove any loopback address or whitespace from the hostname
    SPARK_DRIVER_HOST = re.sub(rf"\s*127.0.0.1\s*", "", SPARK_DRIVER_HOST)

    # Fall back to `127.0.0.1` if no valid host is found
    if not SPARK_DRIVER_HOST:
        SPARK_DRIVER_HOST = "127.0.0.1"
except subprocess.CalledProcessError as e:
    # Handle errors when running the subprocess
    SPARK_DRIVER_HOST = "127.0.0.1"
    print(f"Error retrieving hostname: {e}")

# Set the SPARK_LOCAL_IP environment variable
os.environ["SPARK_LOCAL_IP"] = SPARK_DRIVER_HOST


class SPARK_ENV(Enum):
    LOCAL = [
        ("spark.master", "local"),
        ("spark.driver.host", SPARK_DRIVER_HOST),
    ]
    K8S = [
        ("spark.master", "spark://spark-master-svc:7077"),
        ("spark.driver.bindAddress", "0.0.0.0"),
        ("spark.driver.host", SPARK_DRIVER_HOST),
        ("spark.driver.port", "7078"),
        ("spark.submit.deployMode", "client"),
    ]


def get_spark_context(app_name: str, config: SPARK_ENV) -> SparkSession:
    """Get a Spark context with the given configuration."""
    spark_conf = SparkConf().setAll(config.value).setAppName(app_name)
    return SparkSession.builder.config(conf=spark_conf).getOrCreate()

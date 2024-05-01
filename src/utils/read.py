from pyspark.sql import SparkSession, DataFrame
from delta import *

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_delta_table(path: str) -> DataFrame:
    """
    Reads a Delta Table folder into a PySpark DataFrame.
    
    Parameters:
    - path: str, the folder path to DeltaTable.
    
    Returns:
    - DataFrame: The loaded PySpark DataFrame.
    """

    # Initialize SparkSession with Delta Lake support
    builder = SparkSession.builder.appName("DeltaLakeExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    logger.info(f"Reading Delta Table from path: {path}")
    df = spark.read.format("delta").load(path)
    logger.info("Delta Table loaded successfully.")
    
    return df

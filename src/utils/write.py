from pyspark.sql import DataFrame

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def write_parquet(df: DataFrame, path: str) -> None:
    """
    Writes a DataFrame as a parquet file to the specified path.
    
    Parameters:
    - df: DataFrame, the PySpark DataFrame to be written as a parquet file.
    - path: str, the target path where parquet file DeltaTable will be written.
    """
    
    logger.info(f"Starting to write DataFrame to Parquet at path: {path}")
    df.write.mode('overwrite').parquet(path)
    logger.info("DataFrame successfully written to Parquet.")

def write_delta_table(df: DataFrame, path: str) -> None:
    """
    Writes a DataFrame as a Delta table to the specified path.

    Parameters:
    - df: DataFrame, the PySpark DataFrame to be written as a Delta table.
    - path: str, the target path where the Delta table will be written.
    """

    logger.info(f"Starting to write DataFrame to Delta table at path: {path}")
    df.write.mode("overwrite").format('delta').save(path)
    logger.info("DataFrame successfully written to Delta table.")
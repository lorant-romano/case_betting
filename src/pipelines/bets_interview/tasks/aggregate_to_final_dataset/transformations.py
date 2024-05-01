import logging
from pyspark.sql import DataFrame

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def bet_trans_join(bets_df: DataFrame, trans_df: DataFrame) -> DataFrame:
    """
    Joins betting information DataFrame with transactions DataFrame using a left join on 'sportsbook_id'.

    This function merges the betting and transaction dataframes on the 'sportsbook_id' field,
    retaining all rows from the betting dataframe and including matching rows from the transactions dataframe.
    If there's no matching transaction for a betting entry, the corresponding fields from the transactions dataframe
    will be filled with nulls.

    Parameters:
    - bets_df (DataFrame): A PySpark DataFrame containing betting information with at least a 'sportsbook_id' column.
    - trans_df (DataFrame): A PySpark DataFrame containing transactions information with at least a 'sportsbook_id' column.

    Returns:
    - DataFrame: A PySpark DataFrame resulting from the left join of `bets_df` and `trans_df` on 'sportsbook_id'.
    """
    
    logger.info("Joining betting and transaction DataFrames on 'sportsbook_id'.")
    try:
        joined_df = bets_df.join(trans_df, "sportsbook_id", "left")
        logger.info("Join operation completed successfully.")
        return joined_df
    except Exception as e:
        logger.error(f"Failed to join DataFrames: {e}")
        raise

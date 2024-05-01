from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def group_transactions(trans_df: DataFrame) -> DataFrame:
    """
    Groups transactions by sportsbook_id and aggregates relevant details into a list.
    
    Parameters:
    - trans_df: DataFrame, the DataFrame containing transaction records.
    
    Returns:
    - DataFrame: A DataFrame grouped by sportsbook_id with aggregated transaction details.
    """
    trans_grouped_df = trans_df.groupBy("sportsbook_id").agg(F.collect_list(F.struct(
        "trans_uuid", "transType", "config", "deltaCash"
    )).alias("transactions"))

    return trans_grouped_df
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def aggregate_final_outcomes(combined_df: DataFrame) -> DataFrame:
    """
    Aggregates legs and markets into the final DataFrame with outcome column.

    Parameters:
    - combined_df: DataFrame, combined legs and markets DataFrame.

    Returns:
    - DataFrame: The aggregated final DataFrame.
    """

    return combined_df.groupBy("sportsbook_id", "account_id").agg(F.collect_list(F.struct(
        F.col("price"),
        F.col("result"),
        F.col("outcomeRef"),
        F.col("marketRef"),
        F.col("eventRef"),
        F.col("outcomeName"),
        F.col("marketName"),
        F.col("eventName"),
        F.col("eventTypeName"),
        F.col("eventStartTime")
    )).alias("outcomes"))

def join_legs_markets(legs_df, markets_df):
    
    return legs_df.join(markets_df, ["sportsbook_id", "account_id","outcomeRef","marketRef","eventRef"])


def explode_bets_legs(bets_df: DataFrame) -> DataFrame:
    """
    Explodes the legs DataFrame.

    Parameters:
    - bets_df: DataFrame, the bets DataFrame.

    Returns:
    - DataFrame
    """

    legs_exploded_df = bets_df \
        .withColumn("leg", F.explode("legs")) \
        .select("sportsbook_id", "account_id", "leg.price","leg.result",
                'leg.legPart.outcomeRef','leg.legPart.marketRef','leg.legPart.eventRef')

    return legs_exploded_df

def explode_bets_markets(bets_df: DataFrame) -> DataFrame:
    """
    Explodes the markets array from the bets DataFrame to facilitate further processing.

    Parameters:
    - bets_df: DataFrame, the DataFrame containing an array of markets per bet.

    Returns:
    - DataFrame: A DataFrame where each row represents a market from the original array,
                 including the sportsbook_id, account_id, and all columns under 'market'.
    """
    
    markets_exploded_df = bets_df \
        .withColumn("market", F.explode("markets")) \
        .select("sportsbook_id", "account_id", "market.*")

    return markets_exploded_df
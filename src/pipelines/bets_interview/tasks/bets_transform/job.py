import os

os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk'

from src.utils.read import read_delta_table
from src.utils.write import write_delta_table
from src.pipelines.bets_interview.config import BetsInterviewTransformConfig
from src.pipelines.bets_interview.tasks.bets_transform.transformations import (
    aggregate_final_outcomes, join_legs_markets, explode_bets_legs, explode_bets_markets)

# Configuration for the data transformation pipeline
config = BetsInterviewTransformConfig()

# Define the paths for input and output data
bets_df_path_bronze = config.config_bronze.bets_v1_path
bets_df_path_silver = config.config_silver.bets_v1_path

# Read the initial betting data from the bronze path
bets_df = read_delta_table(bets_df_path_bronze)

# Explode legs and markets from the betting data for further processing
legs_exploded = explode_bets_legs(bets_df)
market_exploded = explode_bets_markets(bets_df)

# Join the exploded legs and markets dataframes on their common keys
joined_df = join_legs_markets(legs_exploded, market_exploded)

# Aggregate the joined dataframe to prepare the final outcomes
aggregated_df = aggregate_final_outcomes(joined_df)

# Write the aggregated data to the silver path as a delta table
write_delta_table(aggregated_df, bets_df_path_silver)
"""
Script to join betting data with transaction data and write the aggregated result to a Parquet file.

This script reads betting and transaction data from Delta tables, joins them based on the 'sportsbook_id',
and writes the resulting DataFrame to a Parquet file for further analysis or consumption.
"""

# Configuration loading
from src.utils.read import read_delta_table
from src.utils.write import write_parquet
from src.pipelines.bets_interview.config import BetsInterviewTransformConfig
from src.pipelines.bets_interview.tasks.aggregate_to_final_dataset.transformations import bet_trans_join

# Load configurations
config = BetsInterviewTransformConfig()

# Paths setup
bets_df_path_silver = config.config_silver.bets_v1_path
trans_df_path_silver = config.config_silver.trans_v1_path
bets_interview_completed = config.config_gold.bets_interview_completed_path

# Data reading
trans_df = read_delta_table(trans_df_path_silver)
bets_df = read_delta_table(bets_df_path_silver)

# Data transformation
bets_interview_completed_df = bet_trans_join(bets_df, trans_df)

# Data writing
write_parquet(bets_interview_completed_df, bets_interview_completed)
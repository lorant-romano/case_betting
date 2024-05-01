from src.utils.read import read_delta_table
from src.utils.write import write_delta_table
from src.pipelines.bets_interview.config import BetsInterviewTransformConfig
from src.pipelines.bets_interview.tasks.transactions_transform.transformations import group_transactions

def main():
    """
    Script to aggregate transactions by sportsbook_id and write the result.
    
    Reads transaction data from a specified bronze path, groups it by sportsbook_id,
    and writes the aggregated data to a silver path.
    """
    config = BetsInterviewTransformConfig()

    trans_df_path_bronze = config.config_bronze.trans_v1_path
    trans_df_path_silver = config.config_silver.trans_v1_path

    try:
        trans_df = read_delta_table(trans_df_path_bronze)
        grouped_trans = group_transactions(trans_df)
        write_delta_table(grouped_trans, trans_df_path_silver)
        print("Successfully aggregated and wrote transactions data.")
    except Exception as e:
        print(f"Error encountered: {e}")

if __name__ == "__main__":
    main()
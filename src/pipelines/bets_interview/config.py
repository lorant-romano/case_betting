from pydantic import BaseModel
from pydantic_settings import BaseSettings

class ConfigBronze(BaseModel):
    """
    Configuration settings for the bronze stage of the pipeline.
    """
    bets_v1_path: str = "data/bronze/bets_v1"
    trans_v1_path: str = "data/bronze/trans_v1"

class ConfigSilver(BaseModel):
    """
    Configuration settings for the silver stage of the pipeline.
    """
    bets_v1_path: str = "data/silver/bets_v1"
    trans_v1_path: str = "data/silver/trans_v1"

class ConfigGold(BaseModel):
    """
    Configuration settings for the gold stage of the pipeline.
    """
    bets_interview_completed_path: str = "data/gold/bets_interview_completed.parquet"

class BetsInterviewTransformConfig(BaseSettings):
    """
    Aggregate configuration model incorporating bronze, silver, and gold stage settings.
    """
    config_bronze: ConfigBronze = ConfigBronze()
    config_silver: ConfigSilver = ConfigSilver()
    config_gold: ConfigGold = ConfigGold()

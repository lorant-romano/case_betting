import pytest
from chispa.dataframe_comparer import *

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


from src.pipelines.bets_interview.tasks.bets_transform.transformations import (
    explode_bets_markets
)

@pytest.fixture(scope="session")
def spark_session():
    """
    PySpark session fixture that is created once for the test session.
    """
    return SparkSession.builder \
        .appName("pytest-pyspark-testing") \
        .getOrCreate()


def test_explode_bets_markets(spark_session):

    schema_input = StructType([
        StructField("sportsbook_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("markets", ArrayType(StructType([
            StructField("outcomeRef", StringType(), True),
            StructField("marketRef", StringType(), True),
            StructField("eventRef", StringType(), True),
            StructField("categoryRef", StringType(), True),
            StructField("outcomeName", StringType(), True),
            StructField("eventTypeName", StringType(), True),
            StructField("className", StringType(), True),
            StructField("marketName", StringType(), True),
            StructField("eventName", StringType(), True),
            StructField("eventStartTime", StringType(), True),
        ]), True), True)
    ])
    
    # Mock data to match the schema
    data_input = [
        ("1", "A", [{"outcomeRef": "R1", "marketRef": "M1", "eventRef": "E1", 
                     "categoryRef": "C1", "outcomeName": "Win", "eventTypeName": "Type1", 
                     "className": "Class1", "marketName": "Market1", "eventName": "Event1", 
                     "eventStartTime": "StartTime1"},
                    {"outcomeRef": "R2", "marketRef": "M2", "eventRef": "E2", 
                     "categoryRef": "C2", "outcomeName": "-", "eventTypeName": "Type2", 
                     "className": "Class2", "marketName": "Market2", "eventName": "Event2", 
                     "eventStartTime": "StartTime2"}]),
        ("2", "B", [{"outcomeRef": "R1", "marketRef": "M1", "eventRef": "E1", 
                     "categoryRef": "C1", "outcomeName": "Win", "eventTypeName": "Type1", 
                     "className": "Class1", "marketName": "Market1", "eventName": "Event1", 
                     "eventStartTime": "StartTime1"},
                    {"outcomeRef": "R2", "marketRef": "M2", "eventRef": "E2", 
                     "categoryRef": "C2", "outcomeName": "-", "eventTypeName": "Type2", 
                     "className": "Class2", "marketName": "Market2", "eventName": "Event2", 
                     "eventStartTime": "StartTime2"}])
    ]
    
    input_df = spark_session.createDataFrame(data_input, schema_input)

    schema_expected = StructType([
        StructField("sportsbook_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("outcomeRef", StringType(), True),
        StructField("marketRef", StringType(), True),
        StructField("eventRef", StringType(), True),
        StructField("categoryRef", StringType(), True),
        StructField("outcomeName", StringType(), True),
        StructField("eventTypeName", StringType(), True),
        StructField("className", StringType(), True),
        StructField("marketName", StringType(), True),
        StructField("eventName", StringType(), True),
        StructField("eventStartTime", StringType(), True)
    ])
    
    # Mock data to match the schema
    data_expected = [
        ("1", "A", "R1", "M1", "E1", "C1", "Win", "Type1", "Class1", "Market1", "Event1", "StartTime1"),
        ("1", "A", "R2", "M2", "E2", "C2", "-", "Type2", "Class2", "Market2", "Event2", "StartTime2"),
        ("2", "B", "R1", "M1", "E1", "C1", "Win", "Type1", "Class1", "Market1", "Event1", "StartTime1"),
        ("2", "B", "R2", "M2", "E2", "C2", "-", "Type2", "Class2", "Market2", "Event2", "StartTime2"),
    ]
    
    df_expected = spark_session.createDataFrame(data_expected, schema_expected)

    df_result = explode_bets_markets(input_df)



    assert_df_equality(df_result, df_expected)
 
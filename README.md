# PySpark Data Pipeline for Betting Interview

This project is a PySpark-based data processing pipeline designed to solve the case. It includes scripts for transforming, aggregating, and writing data to Parquet and Delta Lake formats, simulating a Medallion Architecture.

The final Parquet file is stored in the `data/gold` directory.

## Project Structure

The project is organized as follows:

- `eda`: Contains Jupyter notebooks for exploratory data analysis.
- `data`: Simulates a Medallion Architecture with directories for Bronze, Silver, and Gold layers.
- `src`: Contains all scripts.
  - `pipelines`: Core directory for pipeline scripts.
    - `bets_interview`: Contains the main job and configuration for the bets interview pipeline.
      - `tasks`: Contains individual tasks for different stages of the pipeline, including:
        - `aggregate_to_final_dataset`: Task for aggregating data into the final dataset.
        - `bets_transform`: Task for transforming bets interview data.
          - `tests`: Contains unit tests for ensuring code quality and correctness.
        - `transactions_transform`: Task for transforming transactions data.
  - `utils`: Utilities used across the project, including functions for reading and writing data.
- `README.md`: This project documentation.
- `requirements.txt`: Lists all project dependencies.
- `case`: Contains case study or problem statement details.

### Main Components

- `config.py`: Configuration settings for the pipeline.
- `job.py`: The main script that orchestrates the running of the pipeline tasks.
- `transformations.py`: Contains the transformation logic for each task.
- `read.py` & `write.py`: Utility scripts for reading from and writing to data sources.

### How to Run

1. Ensure Java is installed.
2. Install all requirements by running `pip install -r requirements.txt`.
3. export PYTHONPATH="/NLO:$PYTHONPATH" 
4. export JAVA_HOME=/opt/homebrew/opt/openjdk@11
5. export PATH=$JAVA_HOME/bin:$PATH
6. Run the main job script: python src/pipelines/bets_interview/job.py
7. Run unit tests: pytest src/


This script executes the data pipeline, processing data through various stages defined in the tasks directory, adhering to the Medallion Architecture principles.

## Requirements
### Ensure the following are installed:

- openjdk 11
- Python 3.7 or later
- Apache Spark
- Delta Lake (for Delta table operations)
- Refer to requirements.txt for Python packages dependencies.

### Development
- The eda directory is intended for Jupyter notebooks used in exploratory data analysis. Notebooks should be named descriptively based on their analysis purpose.
- The utils directory contains helper functions that facilitate reading and writing data and can be extended with additional utilities as needed.

# Testing

To ensure the reliability and performance of the data pipeline, a rigorous testing strategy has been implemented. While only one unit test is provided as a demonstrative example, it is crucial to understand that all functions should undergo thorough testing to identify any issues that may arise from future changes.

## Unit Testing
- A unit test has been created using pytest and chispa to demonstrate how to effectively test PySpark applications.
- The test is focused on the `explode_bets_markets` function, showing how to mock Spark DataFrames, apply transformations, and verify outcomes using assertions.
- Due to time constraints, this serves as a demonstrative example. However, it is recommended to develop comprehensive tests for all pipeline functions to safeguard against regression and ensure data integrity.
- Run unit tests by executing `pytest src/` from the project root. This will automatically discover and run tests defined in the tests directory, adhering to the convention of naming test files as `test_<module>.py`.
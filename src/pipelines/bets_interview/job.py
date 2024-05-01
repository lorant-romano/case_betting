import subprocess
import os

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_script(script_path):
    """
    Runs a Python script using subprocess and prints the output.

    Parameters:
    - script_path: The path to the Python script to run.
    """
    logger.info(f"Running script: {script_path}")
    result = subprocess.run(['python', script_path], text=True, capture_output=True)
    
    if result.stdout:
        logger.info(f"Output from {script_path}: {result.stdout}")
    if result.stderr:
        logger.error(f"Errors from {script_path}: {result.stderr}")

def main():
    """
    Main function to run the scripts in sequence, simulating a Databricks workflow.
    """
    base_path = "src/pipelines/bets_interview/tasks"

    scripts = [
        "bets_transform/job.py",
        "transactions_transform/job.py",
        "aggregate_to_final_dataset/job.py"
    ]

    for script in scripts:
        script_path = os.path.join(base_path, script)
        run_script(script_path)

if __name__ == "__main__":
    main()
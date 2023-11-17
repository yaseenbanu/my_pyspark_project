import sys
import os 

base_path = os.environ['base_project_path']
sys.path.append(f'{base_path}\my_pyspark_project\src\main')

import app.validations as validations
from app_libraries.config_loader import load_config, get_configs
from app_libraries import spark_utils,log_utils
import app_libraries.log_utils as log_ut
from app_libraries.log_utils import LoggerConfig
from app_libraries.spark_utils import (
    create_spark_session, load_data, save_data, standardize_column_names, 
    fill_missing_values, get_logger, validate_schema, check_duplicates
)

logger = LoggerConfig.get_logger(__name__)

# Load the configuration at the start of your application
load_config(rf'{base_path}\my_pyspark_project\src\bin\configs')


# Later in your application, when you need to access the configuration
db_host = get_configs('input.yml', 'database', 'host')


def main():
    # Setup
    spark = create_spark_session(app_name="MySparkJob")

    # # Load Data
    input_path = get_configs('input.yml', 'Properties', 'input_location')
    df = load_data(spark, f"{base_path}{input_path}", format="csv")
    logger.info("Data loaded successfully")

    # # Data Processing
    # # Example: Standardize column names and fill missing values
    df = standardize_column_names(df)
    df = fill_missing_values(df)
    logger.info("Data processing completed")

    validations.start_validations(df)

    # # Save Results
    output_loc = get_configs('output.yml', 'Properties', 'output_location')
    save_data(df, f"{base_path}{output_loc}", format="csv")
    logger.info("Processed data saved successfully")

if __name__ == "__main__":
    log_ut.log_header("Starting the process")
    main()

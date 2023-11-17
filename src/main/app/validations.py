from app_libraries.log_utils import LoggerConfig
from app_libraries.spark_utils import ( check_duplicates)

logger = LoggerConfig.get_logger(__name__)
def start_validations(df):
    # # Check for duplicates
    if check_duplicates(df):
        logger.error("Duplicate records found")
        return 
    else:
        logger.info("Validation passed: No Duplicates found")
import logging

class LoggerConfig:
    @staticmethod
    def get_logger(name, level=logging.DEBUG):
        """Set up and return a logger with the specified name and level."""

        # Create or get the logger
        logger = logging.getLogger(name)

        # Check if the logger already has handlers set up
        if not logger.handlers:
            # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            logger.setLevel(level)

            # Create a handler that outputs log messages to the console
            console_handler = logging.StreamHandler()

            # Set the logging level for the handler
            console_handler.setLevel(level)

            # Optionally, you can format the log messages
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)

            # Add the handler to the logger
            logger.addHandler(console_handler)

        return logger

def log_header(message):
    logger = LoggerConfig.get_logger(__name__)
    """Log a special header message."""
    header = '*' * 30
    formatted_message = f"\n{header}\n* {message}\n{header}"
    logger.info(formatted_message)
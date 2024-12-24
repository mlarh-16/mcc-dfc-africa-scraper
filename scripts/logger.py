import logging


def setup_logger(name) -> logging.Logger:
    """Set up the logger.

    Args:
        name (str): The name of the logger.

    Returns:
        logging.Logger: The logger.

    """
    logger_ = logging.getLogger(name)
    logger_.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    logger_.addHandler(console_handler)

    return logger_


logger = setup_logger("research_project")

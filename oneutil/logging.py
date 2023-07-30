import logging

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a console handler and set the log level
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a formatter and attach it to the console handler
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)

# Attach the console handler to the logger
logger.addHandler(console_handler)


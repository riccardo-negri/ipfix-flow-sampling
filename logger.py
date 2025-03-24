"""Logger for the flow sampling methods."""

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)-70s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)

# Create a logger object
logger = logging.getLogger("ipfix_flow_sampling")

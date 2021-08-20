import logging
import sys
from typing import Final

app_logger = logging.getLogger("TABLEDANCER")
app_logger.setLevel(logging.DEBUG)

logger_format: Final[str] = "{asctime}::{name}::{levelname}::{message}"
stream_formatter = logging.Formatter(logger_format, style="{")

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.setFormatter(stream_formatter)

stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.ERROR)
stderr_handler.setFormatter(stream_formatter)

app_logger.addHandler(stdout_handler)
app_logger.addHandler(stderr_handler)

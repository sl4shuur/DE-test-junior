import logging
import sys
import colorama
from colorama import Fore, Style

colorama.init()

# Success level for logging (custom level)
SUCCESS_LEVEL = 69
logging.addLevelName(SUCCESS_LEVEL, "SUCCESS")


def success(self, message, *args, stacklevel=3, **kwargs):
    if self.isEnabledFor(SUCCESS_LEVEL):
        self.log(SUCCESS_LEVEL, message, *args, stacklevel=stacklevel, **kwargs)


# Add the success method to the Logger class
setattr(logging.Logger, "success", success)


# Adding a module-level function for logging
def logging_success(message, *args, **kwargs):
    logging.getLogger().success(message, *args, **kwargs)


logging.success = logging_success


def hex_to_ansi(hex_color: str) -> str:
    hex_color = hex_color.lstrip("#")
    r, g, b = tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))
    return f"\033[38;2;{r};{g};{b}m"


COLORS = {
    "DEBUG": hex_to_ansi("#3ACEFF"),  # Sky Blue
    "INFO": hex_to_ansi("#A1F7FF"),  # Light Blue
    "SUCCESS": hex_to_ansi("#69FE69"),  # Bright Green
    "WARNING": hex_to_ansi("#FDF32F"),  # Yellow
    "ERROR": hex_to_ansi("#F61C1C"),  # Red
    "CRITICAL": hex_to_ansi("#FF6EFF"),  # Magenta
}


class ColoredFormatter(logging.Formatter):
    """Colored formatter with file path, line number, and function name support"""

    def __init__(self, include_function=False):
        super().__init__()
        self.include_function = include_function

    def format(self, record):
        log_color = COLORS.get(record.levelname, Fore.WHITE)
        colored_time = f"{log_color}{self.formatTime(record, '%Y-%m-%d %H:%M:%S')}{Style.RESET_ALL}"
        colored_level = f"{log_color}{record.levelname}{Style.RESET_ALL}"

        # Build the formatted message
        message_parts = []

        # Add timestamp
        message_parts.append(f"[{colored_time}]")

        # Add file path, line number, and function name if enabled
        if self.include_function:
            file_info = f"{record.pathname}:{record.lineno} -> {record.funcName}"
            function_info = f"{hex_to_ansi('#FF9500')}[{file_info}]{Style.RESET_ALL}"
            message_parts.append(function_info)

        # Combine all parts
        header = " ".join(message_parts)
        formatted_record = f"{header}\n{colored_level}: {record.getMessage()}"

        return formatted_record


class FullColoredFormatter(logging.Formatter):
    """Full colored formatter with file path, line number, and function name support"""

    def __init__(self, include_function=False):
        super().__init__()
        self.include_function = include_function

    def format(self, record):
        log_color = COLORS.get(record.levelname, Fore.WHITE)

        # Build the formatted message
        message_parts = []
        message_parts.append(f"[{self.formatTime(record, '%Y-%m-%d %H:%M:%S')}]")

        # Add file path, line number, and function name if enabled
        if self.include_function:
            file_info = f"{record.pathname}:{record.lineno} -> {record.funcName}"
            message_parts.append(f"[{file_info}]")

        # Combine all parts
        header = " ".join(message_parts)
        formatted_record = f"\n{header}\n{record.levelname}: {record.getMessage()}"

        return f"{log_color}{formatted_record}{Style.RESET_ALL}"


def setup_logging(
    level: int = logging.INFO,
    full_color: bool = False,
    include_function: bool = False,
) -> None:
    """
    Setup logging configuration with colored output and optional function names.

    Args:
        level (int, optional): Logging level, by default logging.INFO
        full_color (bool, optional): Enable full color output, by default False
        include_function (bool, optional): Include function name in logs, by default False

    Returns:
        None

    Example:
        ```python
        import logging
        from utils.logging_config import setup_logging
        setup_logging(level=logging.DEBUG, full_color=True, include_function=True)
        logger = logging.getLogger(__name__)
        logger.debug("This is a debug message.")
        ```
    """
    handler = logging.StreamHandler(sys.stdout)

    if full_color:
        handler.setFormatter(FullColoredFormatter(include_function=include_function))
    else:
        handler.setFormatter(ColoredFormatter(include_function=include_function))

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    # Reduce noise from external libraries
    logging.getLogger("aio_pika").setLevel(logging.WARNING)
    logging.getLogger("opentelemetry").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    if include_function:
        logging.success("🎨 Colored logging configured with function names")
    else:
        logging.success("🎨 Colored logging configured")


def logging_test_func():
    """
    Test function to demonstrate logging setup.
    This function will log messages at various levels.
    """
    logging.debug("This is a debug message.")
    logging.info("This is an info message.")
    logging.success("This is a success message!")
    logging.warning("This is a warning message.")
    logging.error("This is an error message.")
    logging.critical("This is a critical message.")


if __name__ == "__main__":
    print("Regular color logging:")
    setup_logging(level=logging.DEBUG)
    logging_test_func()

    print("\nFull color logging:")
    logging.getLogger().handlers.clear()
    setup_logging(level=logging.DEBUG, full_color=True)
    logging_test_func()

    print("\nFull color logging with tracing and function names:")
    setup_logging(level=logging.DEBUG, full_color=True, include_function=True)
    logging_test_func()

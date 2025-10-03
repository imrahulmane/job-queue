import logging
import sys
import json
from datetime import datetime
from pathlib import Path


class LogFormatter(logging.Formatter):
    """Custom formatter that resembles Laravel logs"""

    COLORS = {
        'DEBUG': '\033[36m',  # Cyan
        'INFO': '\033[32m',  # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',  # Red
        'CRITICAL': '\033[35m',  # Purple
        'RESET': '\033[0m'  # Reset
    }

    def format(self, record):
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')

        # Add color to log level for console output
        level_name = record.levelname
        colored_level = f"{self.COLORS.get(level_name, '')}{level_name}{self.COLORS['RESET']}"

        # Format the message like Laravel
        log_entry = f"[{timestamp}] {colored_level}: {record.getMessage()}"

        # Add exception info if present
        if record.exc_info:
            log_entry += f"\nException: {record.exc_info[1]}"

        # Add stack trace if available
        if hasattr(record, 'stack') and record.stack:
            log_entry += f"\nStack trace:\n{record.stack}"

        # Add context data if available (similar to Laravel's context array)
        if hasattr(record, 'context') and record.context:
            try:
                context_str = json.dumps(record.context, indent=2)
                log_entry += f"\nContext: {context_str}"
            except:
                log_entry += f"\nContext: {record.context}"

        return log_entry


def setup_logging(log_level=logging.INFO, log_file=None):
    """Configure application-wide logging"""
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(LogFormatter())
    logger.addHandler(console_handler)

    # Add file handler if log file is specified
    if log_file:
        log_dir = Path(log_file).parent
        log_dir.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(LogFormatter())
        logger.addHandler(file_handler)

    return logger


# Create a function to add context to log records
def log_with_context(logger, level, message, context=None):
    """Log a message with additional context data"""
    extra = {'context': context} if context else {}
    logger.log(level, message, extra=extra)


# Create convenience methods
def debug(logger, message, context=None):
    log_with_context(logger, logging.DEBUG, message, context)


def info(logger, message, context=None):
    log_with_context(logger, logging.INFO, message, context)


def warning(logger, message, context=None):
    log_with_context(logger, logging.WARNING, message, context)


def error(logger, message, context=None):
    log_with_context(logger, logging.ERROR, message, context)


def critical(logger, message, context=None):
    log_with_context(logger, logging.CRITICAL, message, context)
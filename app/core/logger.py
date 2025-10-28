import logging
import sys
import json
from datetime import datetime
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler


class ColoredLogFormatter(logging.Formatter):
    """Custom formatter with colors for console output"""

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

        # Add color to log level
        level_name = record.levelname
        colored_level = f"{self.COLORS.get(level_name, '')}{level_name}{self.COLORS['RESET']}"

        # Format like Laravel: [timestamp] LEVEL: message
        log_entry = f"[{timestamp}] {colored_level}: {record.getMessage()}"

        # Add exception info if present
        if record.exc_info:
            log_entry += f"\n{self.formatException(record.exc_info)}"

        # Add stack trace if available
        if hasattr(record, 'stack') and record.stack:
            log_entry += f"\nStack trace:\n{record.stack}"

        # Add context data if available
        if hasattr(record, 'context') and record.context:
            try:
                context_str = json.dumps(record.context, indent=2)
                log_entry += f"\nContext: {context_str}"
            except Exception:
                log_entry += f"\nContext: {record.context}"

        return log_entry


class FileLogFormatter(logging.Formatter):
    """Plain formatter for file output (no colors)"""

    def format(self, record):
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')

        # Plain text without colors
        log_entry = f"[{timestamp}] {record.levelname}: {record.getMessage()}"

        # Add exception info if present
        if record.exc_info:
            log_entry += f"\n{self.formatException(record.exc_info)}"

        # Add stack trace if available
        if hasattr(record, 'stack') and record.stack:
            log_entry += f"\nStack trace:\n{record.stack}"

        # Add context data if available
        if hasattr(record, 'context') and record.context:
            try:
                context_str = json.dumps(record.context, indent=2)
                log_entry += f"\nContext: {context_str}"
            except Exception:
                log_entry += f"\nContext: {record.context}"

        return log_entry


def setup_logging(
        log_level=logging.INFO,
        log_dir='logs',
        app_name='job-queue',
        backup_count=30
):
    """
    Configure application-wide logging with daily rotation

    """
    # Create a named logger (not root logger)
    logger_instance = logging.getLogger(app_name)
    logger_instance.setLevel(log_level)

    # Prevent duplicate handlers if called multiple times
    if logger_instance.handlers:
        return logger_instance

    # Create console handler with colors
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColoredLogFormatter())
    logger_instance.addHandler(console_handler)

    # Create log directory if it doesn't exist
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # filename with date: app-name-YYYY-MM-DD.log
    today = datetime.now().strftime('%Y-%m-%d')
    log_file = log_path / f"{app_name}-{today}.log"

    # Create rotating file handler
    file_handler = TimedRotatingFileHandler(
        filename=str(log_file),
        when='midnight',  # Rotate at midnight
        interval=1,  # Every 1 day
        backupCount=backup_count,  # Keep X days of logs
        encoding='utf-8'
    )

    # Custom namer to maintain naming after rotation
    def namer(default_name):
        # Extract the date from the rotated file
        # TimedRotatingFileHandler adds .YYYY-MM-DD to the filename
        # We want to keep it as app-name-YYYY-MM-DD.log
        return default_name.replace(f"{app_name}-{today}.log.", f"{app_name}-")

    file_handler.namer = namer

    # Use plain formatter for files (no colors)
    file_handler.setFormatter(FileLogFormatter())
    logger_instance.addHandler(file_handler)

    return logger_instance

# Create a function to add context to log records
def log_with_context(logger, level, message, context=None):
    """Log a message with additional context data"""
    extra = {'context': context} if context else {}
    logger.log(level, message, extra=extra)


# Convenience methods for logging with context
def debug(logger, message, context=None):
    """Log debug message with optional context"""
    log_with_context(logger, logging.DEBUG, message, context)


def info(logger, message, context=None):
    """Log info message with optional context"""
    log_with_context(logger, logging.INFO, message, context)


def warning(logger, message, context=None):
    """Log warning message with optional context"""
    log_with_context(logger, logging.WARNING, message, context)


def error(logger, message, context=None):
    """Log error message with optional context"""
    log_with_context(logger, logging.ERROR, message, context)


def critical(logger, message, context=None):
    """Log critical message with optional context"""
    log_with_context(logger, logging.CRITICAL, message, context)
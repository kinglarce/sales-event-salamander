"""
Advanced Logging Configuration
Provides structured logging with proper formatting, rotation, and context.
"""

import logging
import logging.handlers
import os
import sys
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path
import json
import traceback

from .config import LoggingConfig


class StructuredFormatter(logging.Formatter):
    """Structured JSON formatter for logs"""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON"""
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info)
            }
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in {
                'name', 'msg', 'args', 'levelname', 'levelno', 'pathname',
                'filename', 'module', 'exc_info', 'exc_text', 'stack_info',
                'lineno', 'funcName', 'created', 'msecs', 'relativeCreated',
                'thread', 'threadName', 'processName', 'process', 'getMessage'
            }:
                log_data[key] = value
        
        return json.dumps(log_data, default=str)


class ContextFilter(logging.Filter):
    """Add context information to log records"""
    
    def __init__(self, context: Optional[Dict[str, Any]] = None):
        super().__init__()
        self.context = context or {}
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add context to log record"""
        for key, value in self.context.items():
            setattr(record, key, value)
        return True


class LoggerManager:
    """Manages application logging configuration"""
    
    def __init__(self, config: LoggingConfig):
        self.config = config
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Setup logging configuration"""
        # Create log directory if it doesn't exist
        log_dir = Path(self.config.log_directory)
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, self.config.level.upper()))
        
        # Clear existing handlers
        root_logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
        
        # File handler with rotation
        if self.config.enable_file_logging:
            file_handler = logging.handlers.RotatingFileHandler(
                filename=log_dir / f"application_{datetime.now().strftime('%Y%m%d')}.log",
                maxBytes=self.config.max_file_size,
                backupCount=self.config.backup_count,
                encoding='utf-8'
            )
            file_handler.setLevel(logging.DEBUG)
            file_formatter = StructuredFormatter()
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)
    
    def get_logger(self, name: str, context: Optional[Dict[str, Any]] = None) -> logging.Logger:
        """Get logger with optional context"""
        logger = logging.getLogger(name)
        
        if context:
            context_filter = ContextFilter(context)
            logger.addFilter(context_filter)
        
        return logger
    
    def add_file_handler(self, name: str, filename: str, level: int = logging.DEBUG) -> None:
        """Add file handler for specific logger"""
        logger = logging.getLogger(name)
        
        # Create file handler
        file_handler = logging.handlers.RotatingFileHandler(
            filename=filename,
            maxBytes=self.config.max_file_size,
            backupCount=self.config.backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(level)
        file_formatter = StructuredFormatter()
        file_handler.setFormatter(file_formatter)
        
        logger.addHandler(file_handler)
        logger.setLevel(level)


class PerformanceLogger:
    """Logger for performance metrics"""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._start_times: Dict[str, float] = {}
    
    def start_timer(self, operation: str) -> None:
        """Start timing an operation"""
        self._start_times[operation] = datetime.now().timestamp()
        self.logger.debug(f"Started operation: {operation}")
    
    def end_timer(self, operation: str, **kwargs) -> None:
        """End timing an operation and log duration"""
        if operation in self._start_times:
            duration = datetime.now().timestamp() - self._start_times[operation]
            self.logger.info(
                f"Completed operation: {operation}",
                extra={
                    "operation": operation,
                    "duration_seconds": duration,
                    **kwargs
                }
            )
            del self._start_times[operation]
        else:
            self.logger.warning(f"Timer not found for operation: {operation}")
    
    def log_metric(self, metric_name: str, value: float, **kwargs) -> None:
        """Log a performance metric"""
        self.logger.info(
            f"Performance metric: {metric_name}",
            extra={
                "metric_name": metric_name,
                "metric_value": value,
                **kwargs
            }
        )


class APILogger:
    """Specialized logger for API operations"""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def log_request(self, method: str, url: str, **kwargs) -> None:
        """Log API request"""
        self.logger.debug(
            f"API Request: {method} {url}",
            extra={
                "request_method": method,
                "request_url": url,
                **kwargs
            }
        )
    
    def log_response(self, method: str, url: str, status_code: int, duration: float, **kwargs) -> None:
        """Log API response"""
        level = logging.INFO if status_code < 400 else logging.WARNING
        self.logger.log(
            level,
            f"API Response: {method} {url} - {status_code} ({duration:.2f}s)",
            extra={
                "request_method": method,
                "request_url": url,
                "response_status": status_code,
                "response_duration": duration,
                **kwargs
            }
        )
    
    def log_error(self, method: str, url: str, error: Exception, **kwargs) -> None:
        """Log API error"""
        self.logger.error(
            f"API Error: {method} {url} - {error}",
            extra={
                "request_method": method,
                "request_url": url,
                "error_type": type(error).__name__,
                "error_message": str(error),
                **kwargs
            },
            exc_info=True
        )


def setup_logging(config: LoggingConfig) -> LoggerManager:
    """Setup application logging"""
    return LoggerManager(config)


def get_logger(name: str, context: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """Get logger instance"""
    # This would be called after setup_logging has been called
    return logging.getLogger(name)

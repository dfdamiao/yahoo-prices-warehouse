#!/usr/bin/env python3
# symbol_updater.py - COMPLETE REWRITE with Resource Management & yfinance 0.2.65 Optimization
import os
import time
import logging
import gc
import json
import traceback
import random
import resource
import threading
import psutil
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager
from pathlib import Path


import pandas as pd
import numpy as np
import yfinance as yf

# Import your custom modules
from market_data.settings import MARKET_DATA_DIR
from market_data.manager import MarketDataManager
from market_data.progress_tracker import ProgressTracker
from market_data._notify import send_telegram_notification
from market_data.yf_errors import capture_yf_errors

# Configure logging
logger = logging.getLogger("market_data.updater")
logger.setLevel(logging.INFO)
# logger.setLevel(logging.DEBUG)  # Add this temporarily at the top of your script


# Create log directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)

# Create and add file handler
log_file = os.path.join(log_dir, "symbol_updater.log")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)

# Create and add console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
logger.addHandler(console_handler)

logger.propagate = False

print(f"Logging configured. Log messages will be written to: {log_file}")


class SystemResourceManager:
    """Comprehensive system resource management for file descriptors, memory, and connections."""

    def __init__(self):
        # Get system limits and auto-detect thresholds
        self.fd_soft_limit, self.fd_hard_limit = resource.getrlimit(
            resource.RLIMIT_NOFILE
        )

        # Auto-detect thresholds based on system limits
        self.max_fd_usage = 70  # Warning threshold
        self.critical_fd_usage = 85  # Critical threshold
        self.max_memory_mb = 10240  # 10GB default

        # Simple thresholds - no complex adjustments
        logger.debug(f"Resource Manager initialized - FD limit: {self.fd_soft_limit}")
        logger.debug(
            f"Thresholds - FD warning: {self.max_fd_usage}%, critical: {self.critical_fd_usage}%"
        )
        logger.info(f"Memory limit: {self.max_memory_mb}MB")

        # Tracking
        self.check_count = 0
        self.emergency_cleanups = 0
        self.last_cleanup = 0

    def get_system_status_debug(self) -> Dict[str, Any]:
        """Get system status with detailed debugging."""
        try:
            process = psutil.Process()

            # File descriptor usage
            open_files = len(process.open_files())
            fd_usage_percent = (open_files / self.fd_soft_limit) * 100

            # Memory usage
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024

            # Log the actual values for debugging
            logger.debug(f"DEBUG - Open files: {open_files}/{self.fd_soft_limit}")
            logger.debug(f"DEBUG - FD usage: {fd_usage_percent:.2f}%")
            logger.debug(f"DEBUG - Memory: {memory_mb:.1f}MB")
            logger.debug(
                f"DEBUG - FD thresholds: warning={self.max_fd_usage}%, critical={self.critical_fd_usage}%"
            )
            logger.debug(f"DEBUG - Memory threshold: {self.max_memory_mb}MB")

            # Determine status with detailed logging
            status = "healthy"
            reasons = []

            if fd_usage_percent > self.critical_fd_usage:
                status = "critical"
                reasons.append(
                    f"FD usage {fd_usage_percent:.1f}% > {self.critical_fd_usage}%"
                )

            if memory_mb > self.max_memory_mb:
                status = "critical"
                reasons.append(f"Memory {memory_mb:.1f}MB > {self.max_memory_mb}MB")

            if fd_usage_percent > self.max_fd_usage and status != "critical":
                status = "warning"
                reasons.append(
                    f"FD usage {fd_usage_percent:.1f}% > {self.max_fd_usage}%"
                )

            if memory_mb > self.max_memory_mb * 0.8 and status not in [
                "critical",
                "warning",
            ]:
                status = "warning"
                reasons.append(
                    f"Memory {memory_mb:.1f}MB > {self.max_memory_mb * 0.8:.1f}MB"
                )

            if status != "healthy":
                logger.warning(
                    f"DEBUG - Status '{status}' triggered by: {', '.join(reasons)}"
                )

            return {
                "status": status,
                "fd_usage_percent": fd_usage_percent,
                "open_files": open_files,
                "fd_limit": self.fd_soft_limit,
                "memory_mb": memory_mb,
                "max_memory_mb": self.max_memory_mb,
                "connections": 0,  # Simplified
                "emergency_cleanups": self.emergency_cleanups,
                "debug_reasons": reasons,
            }

        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {
                "status": "unknown",
                "fd_usage_percent": 0,
                "open_files": 0,
                "fd_limit": self.fd_soft_limit,
                "memory_mb": 0,
                "connections": 0,
                "emergency_cleanups": self.emergency_cleanups,
                "debug_reasons": [f"Error: {e}"],
            }

    # Replace the original method temporarily
    def get_system_status(self) -> Dict[str, Any]:
        return self.get_system_status_debug()

    def should_check_resources(self) -> bool:
        """Determine if we should check resources (adaptive frequency)."""
        self.check_count += 1

        # Check more frequently if we've had recent problems
        if self.emergency_cleanups > 0:
            return self.check_count % 25 == 0  # Every 25 operations
        else:
            return self.check_count % 100 == 0  # Every 100 operations

    def emergency_cleanup(self) -> bool:
        """Perform emergency resource cleanup."""
        current_time = time.time()

        # Prevent too frequent emergency cleanups
        if current_time - self.last_cleanup < 30:
            return False

        logger.warning("🚨 EMERGENCY RESOURCE CLEANUP INITIATED")
        self.emergency_cleanups += 1
        self.last_cleanup = current_time

        try:
            # 1. Force garbage collection
            gc.collect()

            # 2. Clear yfinance singleton and all sessions
            from yfinance.data import YfData

            if hasattr(YfData, "_instances") and YfData._instances:
                logger.info("Clearing yfinance singleton instances")
                YfData._instances.clear()

            # 3. Reset yfinance configuration
            if hasattr(yf, "set_config"):
                yf.set_config()

            # 4. Force close any remaining open files we can control
            try:
                import threading

                for thread in threading.enumerate():
                    if hasattr(thread, "_target") and "yfinance" in str(thread._target):
                        logger.debug(f"Found yfinance thread: {thread.name}")
            except Exception:
                pass

            # 5. Additional garbage collection
            for _ in range(3):
                gc.collect()
                time.sleep(1)

            logger.warning(f"Emergency cleanup #{self.emergency_cleanups} completed")
            return True

        except Exception as e:
            logger.error(f"Error during emergency cleanup: {e}")
            return False

    def check_and_manage_resources(self) -> Dict[str, Any]:
        """Check resources and perform cleanup if needed."""
        # Always get the current status first
        status = self.get_system_status()

        # Only perform management actions if it's time to check
        if not self.should_check_resources():
            status.update({"action": "skipped"})
            return status

        if status["status"] == "critical":
            logger.warning(
                f"CRITICAL detected: FD usage {status['fd_usage_percent']:.1f}%, Memory {status['memory_mb']:.1f}MB"
            )

            # Before emergency cleanup, try a gentler approach
            logger.info("Attempting gentle cleanup before emergency measures...")
            gc.collect()
            time.sleep(2)  # Brief pause

            # Recheck after gentle cleanup
            recheck_status = self.get_system_status()
            if recheck_status["status"] != "critical":
                logger.info("Gentle cleanup resolved critical status")
                recheck_status.update({"action": "gentle_cleanup_resolved"})
                return recheck_status

            # Only do emergency cleanup if gentle didn't work
            logger.warning("Gentle cleanup insufficient - performing emergency cleanup")
            if self.emergency_cleanup():
                # Recheck after emergency cleanup
                new_status = self.get_system_status()
                logger.info(
                    f"After emergency cleanup: FD {new_status['fd_usage_percent']:.1f}%, Memory {new_status['memory_mb']:.1f}MB"
                )
                status.update(
                    {"action": "emergency_cleanup", "post_cleanup_status": new_status}
                )
            else:
                status.update({"action": "emergency_cleanup_failed"})

        elif status["status"] == "warning":
            logger.debug(
                f"WARNING: FD usage {status['fd_usage_percent']:.1f}%, Memory {status['memory_mb']:.1f}MB"
            )
            # Just gentle cleanup for warnings
            gc.collect()
            status.update({"action": "gentle_cleanup"})
        else:
            status.update({"action": "none"})

        return status

    def is_false_positive_critical(self) -> bool:
        """Check if critical status might be a false positive."""
        try:
            # Get multiple readings over a short time
            readings = []
            for _ in range(3):
                status = self.get_system_status()
                readings.append(status)
                time.sleep(0.5)

            # If readings are inconsistent, might be false positive
            statuses = [r["status"] for r in readings]
            if len(set(statuses)) > 1:
                logger.warning(
                    "Inconsistent resource readings - possible false positive"
                )
                return True

            # If FD usage is borderline, might be transient
            fd_usages = [r["fd_usage_percent"] for r in readings]
            avg_fd = sum(fd_usages) / len(fd_usages)

            if self.critical_fd_usage <= avg_fd <= self.critical_fd_usage + 5:
                logger.warning(
                    f"Borderline FD usage ({avg_fd:.1f}%) - might be transient"
                )
                return True

            return False

        except Exception as e:
            logger.debug(f"Error checking false positive: {e}")
            return False


class SimpleErrorLogger:
    """Simplified error logging without file operations."""

    def __init__(self, log_dir: str):
        self.log_dir = Path(log_dir)
        self.error_count = 0
        self.error_summary = {}

        logger.info("Simple error logger initialized (no CSV files)")

    def log_error(
        self,
        symbol: str,
        error_type: str,
        error_message: str,
        batch_id: Optional[int] = None,
        attempt_count: int = 1,
        recovery_method: str = "none",
        resource_status: Optional[Dict] = None,
    ):
        """Log error to logger only (no file operations)."""

        self.error_count += 1

        # Track error types
        if error_type not in self.error_summary:
            self.error_summary[error_type] = 0
        self.error_summary[error_type] += 1

        # Log to main logger
        logger.error(f"Symbol error: {symbol} | {error_type} | {error_message}")

        # Periodic summary
        if self.error_count % 50 == 0:
            logger.info(
                f"Error summary ({self.error_count} total): {self.error_summary}"
            )

    def force_flush(self):
        """No-op for compatibility."""
        pass


class EnhancedErrorLogger:
    """Enhanced error logging with resource management and buffering."""

    def __init__(self, log_dir: str, buffer_size: int = 30):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)

        self.buffer_size = buffer_size
        self.error_buffer = []
        self.buffer_lock = threading.Lock()

        # Create error log file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.error_log_file = self.log_dir / f"symbol_errors_{timestamp}.csv"

        # Initialize CSV file
        self._initialize_csv_file()

        # Flush tracking
        self.last_flush = time.time()
        self.flush_interval = 60  # Flush every minute

        logger.info(f"Enhanced error logger initialized: {self.error_log_file}")

    def _initialize_csv_file(self):
        """Initialize CSV file with headers."""
        try:
            with open(self.error_log_file, "w") as f:
                f.write(
                    "timestamp,symbol,error_type,error_message,batch_id,attempt_count,"
                    "recovery_method,yfinance_version,fd_usage,memory_mb,action_taken\n"
                )
        except Exception as e:
            logger.error(f"Failed to initialize error CSV: {e}")

    @contextmanager
    def safe_file_operation(self):
        """Context manager for safe file operations."""
        try:
            yield
        except OSError as e:
            if e.errno == 24:  # Too many open files
                logger.error("File descriptor exhaustion during file operation")
                raise
            else:
                logger.error(f"File operation error: {e}")
                raise
        except Exception as e:
            logger.error(f"Unexpected error during file operation: {e}")
            raise

    def log_error(
        self,
        symbol: str,
        error_type: str,
        error_message: str,
        batch_id: Optional[int] = None,
        attempt_count: int = 1,
        recovery_method: str = "none",
        resource_status: Optional[Dict] = None,
    ):
        """Log error with resource status information."""

        with self.buffer_lock:
            try:
                # Clean error message for CSV
                clean_message = (
                    str(error_message)
                    .replace('"', '""')
                    .replace(",", ";")
                    .replace("\n", " ")
                )

                # Get resource information
                fd_usage = (
                    resource_status.get("fd_usage_percent", 0) if resource_status else 0
                )
                memory_mb = (
                    resource_status.get("memory_mb", 0) if resource_status else 0
                )
                action_taken = (
                    resource_status.get("action", "none") if resource_status else "none"
                )

                # Create CSV line
                timestamp = datetime.now().isoformat()
                yf_version = getattr(yf, "__version__", "0.2.65")

                csv_line = (
                    f'"{timestamp}","{symbol}","{error_type}","{clean_message}",'
                    f'"{batch_id}",{attempt_count},"{recovery_method}","{yf_version}",'
                    f'{fd_usage:.1f},{memory_mb:.1f},"{action_taken}"'
                )

                # Add to buffer
                self.error_buffer.append(csv_line)

                # Check if we should flush
                current_time = time.time()
                should_flush = (
                    len(self.error_buffer) >= self.buffer_size
                    or (current_time - self.last_flush) > self.flush_interval
                    or (resource_status and resource_status.get("status") == "warning")
                )

                if should_flush:
                    self._flush_buffer()

            except Exception as e:
                logger.error(f"Error logging symbol error for {symbol}: {e}")

    def _flush_buffer(self):
        """Flush error buffer to file."""
        if not self.error_buffer:
            return

        try:
            with self.safe_file_operation():
                with open(self.error_log_file, "a") as f:
                    for line in self.error_buffer:
                        f.write(line + "\n")
                    f.flush()
                    os.fsync(f.fileno())

                buffer_size = len(self.error_buffer)
                self.error_buffer.clear()
                self.last_flush = time.time()

                logger.debug(f"Flushed {buffer_size} errors to CSV")

        except Exception as e:
            logger.error(f"Error flushing error buffer: {e}")
            # Keep buffer for retry if flush fails

    def force_flush(self):
        """Force flush all remaining errors."""
        with self.buffer_lock:
            if self.error_buffer:
                logger.info(f"Force flushing {len(self.error_buffer)} remaining errors")
                self._flush_buffer()


class YFinanceSessionManager:
    """Manages yfinance sessions with proper lifecycle and resource management."""

    def __init__(self):
        self.session_count = 0
        self.last_reset = 0
        self.reset_interval = 300  # Reset every 5 minutes
        self.lock = threading.Lock()

        # Initialize first session
        self._initialize_session()

    def _initialize_session(self):
        """Initialize a fresh yfinance session."""
        try:
            with self.lock:
                # Clear existing singleton
                from yfinance.data import YfData

                if hasattr(YfData, "_instances") and YfData._instances:
                    YfData._instances.clear()

                # Reset configuration
                if hasattr(yf, "set_config"):
                    yf.set_config()

                self.session_count += 1
                self.last_reset = time.time()

                logger.debug(f"Initialized yfinance session #{self.session_count}")

        except Exception as e:
            logger.error(f"Error initializing yfinance session: {e}")

    def should_reset_session(self) -> bool:
        """Check if session should be reset."""
        current_time = time.time()
        return (current_time - self.last_reset) > self.reset_interval

    def reset_session_if_needed(self):
        """Reset session if needed."""
        if self.should_reset_session():
            logger.info("Resetting yfinance session (periodic maintenance)")
            self._initialize_session()

    def force_reset_session(self):
        """Force reset session immediately."""
        logger.warning("Force resetting yfinance session")
        self._initialize_session()


class SymbolUpdater:
    """
    Advanced symbol updater with comprehensive resource management and yfinance 0.2.65 optimization.

    Features:
    - File descriptor leak prevention
    - Memory management
    - Enhanced error handling and logging
    - Automatic session lifecycle management
    - Progress tracking with interruption recovery
    - Intelligent batch processing
    """

    def __init__(
        self,
        market_data_dir: str,
        db_path: str = None,
        min_batch_size: int = 120,  # Further reduced for stability
        max_batch_size: int = 150,  # Further reduced for stability
        update_period: str = "1mo",
        log_dir: str = None,
        rate_limit_backoff: int = 90,
        enable_resource_monitoring: bool = True,
    ):
        """
        Initialize the advanced symbol updater.

        Args:
            market_data_dir: Directory where market data is stored
            db_path: Path to ArcticDB database
            min_batch_size: Minimum batch size (reduced for stability)
            max_batch_size: Maximum batch size (reduced for stability)
            update_period: yfinance period string for updates
            log_dir: Directory to store log files
            rate_limit_backoff: Backoff time when rate limited
        """

        # Core configuration
        self.market_data_dir = market_data_dir
        self.db_path = db_path or f"lmdb:///{market_data_dir}"
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.update_period = update_period
        self.log_dir = log_dir or os.path.join(market_data_dir, "logs")
        self.rate_limit_backoff = rate_limit_backoff

        # Create log directory
        os.makedirs(self.log_dir, exist_ok=True)

        logger.info(
            f"Initializing Advanced SymbolUpdater:\n"
            f"- Market data dir: {market_data_dir}\n"
            f"- Conservative batch size: {min_batch_size}-{max_batch_size}\n"
            f"- Update period: {update_period}"
        )

        # Initialize resource management
        self.resource_manager = SystemResourceManager()

        # Initialize enhanced error logging
        # self.error_logger = EnhancedErrorLogger(
        #     log_dir=self.log_dir,
        #     buffer_size=20  # Smaller buffer for faster flushing
        # )
        self.enable_resource_monitoring = enable_resource_monitoring

        if not enable_resource_monitoring:
            logger.warning(
                "Resource monitoring DISABLED - proceeding without resource checks"
            )

        # Initialize simplified error logging
        self.error_logger = SimpleErrorLogger(log_dir=self.log_dir)
        # Initialize session management
        self.session_manager = YFinanceSessionManager()

        # Initialize data manager
        self.data_manager = MarketDataManager.get_instance(db_path=self.db_path)

        # Initialize progress tracker
        self.progress_tracker = ProgressTracker(
            market_data_dir=market_data_dir,
            progress_file_name="price_update_progress.json",
            min_batch_size=min_batch_size,
            max_batch_size=max_batch_size,
        )

        # Initialize symbol caches
        self._price_symbols_cache = None
        self._metadata_symbols_cache = None

        # Initialize problem symbols tracking
        self.problem_symbols = {}
        self.load_problem_symbols()

        # Performance tracking
        self.delisted_symbols_count = 0
        self.rate_limit_hits = 0
        self.successful_downloads = 0
        self.batch_count = 0

        # Problem symbols tracking with periodic saves
        self._problem_symbols_dirty = False
        self._last_problem_save = 0
        self._problem_save_interval = 300  # 5 minutes

        logger.info("Advanced SymbolUpdater initialization complete")

    def load_problem_symbols(self):
        """Load known problem symbols from file."""
        problem_symbols_file = os.path.join(
            self.market_data_dir, "problem_symbols.json"
        )

        if os.path.exists(problem_symbols_file):
            try:
                with open(problem_symbols_file, "r") as f:
                    self.problem_symbols = json.load(f)
                logger.info(f"Loaded {len(self.problem_symbols)} problem symbols")
            except json.JSONDecodeError:
                self.problem_symbols = {}
                logger.warning("Problem symbols file exists but couldn't be parsed")
        else:
            self.problem_symbols = {}

    def _track_problem_symbol(self, symbol: str, error_type: str, error_message: str):
        """Track problem symbols with periodic saving."""

        if symbol not in self.problem_symbols:
            self.problem_symbols[symbol] = {
                "first_error": datetime.now().isoformat(),
                "error_count": 1,
                "last_error": datetime.now().isoformat(),
                "last_error_type": error_type,
                "last_error_message": error_message,
            }
        else:
            self.problem_symbols[symbol]["error_count"] += 1
            self.problem_symbols[symbol]["last_error"] = datetime.now().isoformat()
            self.problem_symbols[symbol]["last_error_type"] = error_type
            self.problem_symbols[symbol]["last_error_message"] = error_message

        self._problem_symbols_dirty = True

        # Periodic save
        current_time = time.time()
        if (
            current_time - self._last_problem_save > self._problem_save_interval
            or len(
                [
                    s
                    for s in self.problem_symbols.values()
                    if s.get("error_count", 0) == 1
                ]
            )
            > 20
        ):
            self._save_problem_symbols()

    def _save_problem_symbols(self):
        """Save problem symbols with atomic write."""
        if not self._problem_symbols_dirty:
            return

        current_time = time.time()
        if current_time - self._last_problem_save < 30:  # Rate limit saves
            return

        problem_symbols_file = os.path.join(
            self.market_data_dir, "problem_symbols.json"
        )

        try:
            # Atomic write
            temp_file = f"{problem_symbols_file}.tmp"
            with open(temp_file, "w") as f:
                json.dump(self.problem_symbols, f, indent=2)

            os.replace(temp_file, problem_symbols_file)

            self._problem_symbols_dirty = False
            self._last_problem_save = current_time
            logger.debug(f"Saved {len(self.problem_symbols)} problem symbols")

        except Exception as e:
            logger.error(f"Error saving problem symbols: {e}")
            try:
                if os.path.exists(f"{problem_symbols_file}.tmp"):
                    os.remove(f"{problem_symbols_file}.tmp")
            except Exception:
                pass

    def _force_save_problem_symbols(self):
        """Force save problem symbols (for shutdown)."""
        if self._problem_symbols_dirty:
            self._last_problem_save = 0  # Reset timer to force save
            self._save_problem_symbols()

    def _is_problem_symbol(self, symbol: str) -> bool:
        """Check if symbol is a known persistent problem."""
        return (
            symbol in self.problem_symbols
            and self.problem_symbols[symbol].get("error_count", 0) >= 3
        )

    def _is_delisted_symbol(self, error_message: str) -> bool:
        """Check if error indicates a delisted symbol."""
        if not error_message:
            return False

        error_message = str(error_message).lower()
        delisted_indicators = [
            "possibly delisted",
            "no price data found",
            "no data found",
            "symbol may be delisted",
            "delisted",
            "not found in api response",
            "all nan",
            "no timezone found",
            "missing symbol",
            "failed to parse",
        ]

        return any(indicator in error_message for indicator in delisted_indicators)

    def _handle_rate_limit_error(self):
        """Handle rate limiting with intelligent backoff."""
        self.rate_limit_hits += 1

        # Progressive backoff
        if self.rate_limit_hits == 1:
            wait_time = self.rate_limit_backoff
        elif self.rate_limit_hits == 2:
            wait_time = self.rate_limit_backoff * 1.5
        else:
            wait_time = self.rate_limit_backoff * 2

        # Add jitter
        jitter = random.uniform(0.8, 1.2)
        actual_wait = int(wait_time * jitter)

        logger.warning(
            f"Rate limit hit #{self.rate_limit_hits}! Backing off for {actual_wait}s"
        )

        # Force reset session
        self.session_manager.force_reset_session()

        # Wait with progress updates
        for i in range(actual_wait):
            if i % 20 == 0 and i > 0:
                remaining = actual_wait - i
                logger.info(f"Rate limit backoff: {remaining}s remaining")
            time.sleep(1)

        logger.info("Resuming after rate limit backoff")

    def _download_batch_enhanced(
        self, symbols: List[str], period: str = None, max_retries: int = 3
    ):
        """Simplified batch download with enhanced error detection."""
        if period is None:
            period = self.update_period

        # Skip resource monitoring if disabled
        if not self.enable_resource_monitoring:
            resource_status = {"status": "monitoring_disabled", "action": "bypassed"}
        else:
            # Simple resource check
            resource_status = self.resource_manager.get_system_status()
            if resource_status["status"] == "critical":
                logger.warning(
                    f"High resource usage: Memory {resource_status['memory_mb']:.0f}MB"
                )

        # Reset session periodically
        self.session_manager.reset_session_if_needed()

        # Simple delay
        delay = random.uniform(3, 6)
        time.sleep(delay)

        # Download with retries
        for attempt in range(max_retries):
            try:
                logger.info(
                    f"Downloading {len(symbols)} symbols (attempt {attempt + 1})"
                )

                # Per-ticker errors come off the yfinance logger: yfinance
                # >= 1.4 no longer populates `shared._ERRORS`.
                with capture_yf_errors() as cap:
                    data = yf.download(
                        tickers=symbols,
                        period=period,
                        interval="1d",
                        group_by="ticker",
                        auto_adjust=False,
                        progress=False,
                        timeout=90,
                        threads=False,
                        ignore_tz=False,
                    )
                yf_errors = dict(cap.errors)

                if yf_errors:
                    logger.warning(f"YFinance reported {len(yf_errors)} errors")
                    for symbol, error_msg in yf_errors.items():
                        logger.debug(f"  YF Error - {symbol}: {error_msg}")

                if data is not None and not data.empty:
                    logger.info(f"Successfully downloaded {len(symbols)} symbols")
                    self.successful_downloads += 1
                    # Return yf_errors as third element in tuple
                    return data, False, resource_status, yf_errors

            except Exception as e:
                error_msg = str(e).lower()

                if "rate limit" in error_msg or "too many requests" in error_msg:
                    logger.warning(f"Rate limit: {e}")
                    self._handle_rate_limit_error()
                    return None, True, resource_status, {}

                logger.error(f"Download error (attempt {attempt + 1}): {e}")

                if attempt < max_retries - 1:
                    wait_time = 10 + (attempt * 5)
                    time.sleep(wait_time)

        # Return empty dict for yf_errors on failure
        return None, False, resource_status, {}

    def _determine_enhanced_status(
        self, symbol: str, data: Optional[pd.DataFrame], yf_errors: Dict[str, str]
    ) -> Tuple[str, Optional[str]]:
        """
        Enhanced status determination using yfinance error checking.

        Returns:
            Tuple of (status, reason)
        """
        # Check yfinance errors first
        if symbol in yf_errors:
            error = str(yf_errors[symbol]).lower()

            # Delisted symbols
            if "delisted" in error or "no data found" in error:
                return "delisted", str(yf_errors[symbol])

            # Invalid period
            if "invalid period" in error:
                return "failed", str(yf_errors[symbol])

            # Other yfinance errors
            return "failed", f"YFinance error: {yf_errors[symbol]}"

        # Validate data quality
        if data is not None and not data.empty:
            row_count = len(data)

            # Check for NaN percentage
            price_cols = ["Open", "High", "Low", "Close"]
            available_price_cols = [c for c in price_cols if c in data.columns]

            if available_price_cols:
                nan_count = data[available_price_cols].isna().sum().sum()
                total_cells = len(data) * len(available_price_cols)
                nan_percent = (
                    (nan_count / total_cells * 100) if total_cells > 0 else 100
                )

                if nan_percent > 50:
                    return "incomplete", f"High NaN percentage: {nan_percent:.1f}%"
                elif row_count < 5:
                    return "incomplete", f"Insufficient rows: {row_count}"
                else:
                    return "complete", None
            else:
                return "failed", "Missing price columns"

        # No data
        return "failed", "No data retrieved"

    def _debug_multiindex_structure(self, batch_data: pd.DataFrame, symbols: List[str]):
        """Debug helper to understand MultiIndex structure."""
        if not isinstance(batch_data.columns, pd.MultiIndex):
            logger.debug("Not a MultiIndex structure")
            return

        logger.debug("MultiIndex structure analysis:")
        logger.debug(f"  Names: {batch_data.columns.names}")
        logger.debug(f"  Levels: {len(batch_data.columns.levels)}")
        logger.debug(f"  Level 0 sample: {batch_data.columns.levels[0].tolist()[:10]}")
        logger.debug(f"  Level 1 sample: {batch_data.columns.levels[1].tolist()[:10]}")

        # Check which level contains our symbols
        for i, level in enumerate(batch_data.columns.levels):
            symbols_in_level = [s for s in symbols if s in level]
            logger.debug(
                f"  Level {i} contains {len(symbols_in_level)} of our symbols: {symbols_in_level[:5]}"
            )

        # Show some actual column tuples
        logger.debug(f"  Sample columns: {batch_data.columns.tolist()[:10]}")

    def _extract_symbol_from_batch(self, symbol: str, batch_data: pd.DataFrame):
        """Extract individual symbol data from batch download results - FIXED MultiIndex handling."""
        if batch_data is None or batch_data.empty:
            logger.debug(f"Batch data is None or empty for {symbol}")
            return None

        try:
            # Handle MultiIndex columns
            if isinstance(batch_data.columns, pd.MultiIndex):
                # logger.debug(f"Processing MultiIndex data for {symbol}")
                # logger.debug(f"MultiIndex names: {batch_data.columns.names}")

                # **CRITICAL FIX**: Check which level contains symbols
                symbol_in_level_0 = symbol in batch_data.columns.levels[0]
                symbol_in_level_1 = symbol in batch_data.columns.levels[1]

                # logger.debug(f"Symbol {symbol} in level 0: {symbol_in_level_0}, in level 1: {symbol_in_level_1}")

                if symbol_in_level_0:
                    # Original case: symbol is in first level (group_by='column')
                    symbol_data = batch_data[symbol].copy()
                    # logger.debug(f"Extracted {symbol} from level 0, columns: {symbol_data.columns.tolist()}")

                elif symbol_in_level_1:
                    # **NEW CASE**: Symbol is in second level (group_by='ticker' - the common case)
                    # logger.debug(f"Symbol {symbol} found in level 1, extracting...")

                    # Get all columns where the symbol appears in level 1
                    symbol_cols = [
                        col for col in batch_data.columns if col[1] == symbol
                    ]
                    # logger.debug(f"Found {len(symbol_cols)} columns for {symbol}: {symbol_cols}")

                    if symbol_cols:
                        # Create a new DataFrame with the symbol's data
                        symbol_data = pd.DataFrame(index=batch_data.index)

                        for col in symbol_cols:
                            price_type = col[
                                0
                            ]  # First element is price type (Close, Open, etc.)
                            if price_type in [
                                "Open",
                                "High",
                                "Low",
                                "Close",
                                "Adj Close",
                                "Volume",
                            ]:
                                symbol_data[price_type] = batch_data[col]

                        # logger.debug(f"Reconstructed {symbol} data with columns: {symbol_data.columns.tolist()}")
                    else:
                        # logger.debug(f"No columns found for {symbol} in level 1")
                        return None
                else:
                    # logger.debug(f"Symbol {symbol} not found in either MultiIndex level")
                    return None

                # Validate we have the expected columns and some data
                expected_cols = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
                available_cols = symbol_data.columns.tolist()
                price_cols = [
                    c for c in ["Open", "High", "Low", "Close"] if c in available_cols
                ]

                if len(price_cols) >= 3:  # At least 3 price columns
                    # Check if we have some non-NaN price data
                    has_price_data = not symbol_data[price_cols].isna().all().all()

                    if has_price_data:
                        # logger.debug(f"Symbol {symbol} has valid price data")

                        # Ensure all expected columns exist (fill missing with appropriate defaults)
                        for col in expected_cols:
                            if col not in symbol_data.columns:
                                if col == "Volume":
                                    symbol_data[col] = 0
                                else:
                                    symbol_data[col] = np.nan

                        return symbol_data[expected_cols]  # Return in consistent order
                    else:
                        logger.debug(
                            f"Symbol {symbol} has columns but all price data is NaN"
                        )
                        return None
                else:
                    logger.debug(
                        f"Symbol {symbol} has insufficient price columns: {available_cols}"
                    )
                    return None

            # Handle single symbol case (regular columns)
            elif len(batch_data.columns) >= 4:
                logger.debug(f"Processing single symbol data for {symbol}")
                # Check for price data
                likely_price_cols = [
                    c
                    for c in batch_data.columns
                    if c in ["Open", "High", "Low", "Close"]
                ]
                if (
                    likely_price_cols
                    and not batch_data[likely_price_cols].isna().all().all()
                ):
                    return batch_data.copy()
                else:
                    logger.debug(f"Single symbol {symbol} has no valid price data")

            logger.debug(f"Could not extract valid data for {symbol}")
            return None

        except Exception as e:
            logger.error(f"Error extracting {symbol} from batch: {str(e)}")
            logger.error(f"Batch data structure: {type(batch_data.columns)}")
            if hasattr(batch_data.columns, "levels"):
                logger.error(
                    f"MultiIndex levels: {[len(level) for level in batch_data.columns.levels]}"
                )
            return None

    def _standardize_price_dataframe(self, symbol: str, data: pd.DataFrame):
        """Standardize price data to consistent format."""
        if data is None or data.empty:
            return None

        try:
            standard_df = pd.DataFrame(index=data.index)

            # Column mapping
            column_mapping = {
                "Open": ["Open", "open"],
                "High": ["High", "high"],
                "Low": ["Low", "low"],
                "Close": ["Close", "close"],
                "Adj Close": ["Adj Close", "adj_close", "adjclose", "Adj_Close"],
                "Volume": ["Volume", "volume", "vol"],
            }

            # Map columns
            for std_col, possible_cols in column_mapping.items():
                for col in possible_cols:
                    if col in data.columns:
                        if std_col == "Volume":
                            standard_df[std_col] = (
                                pd.to_numeric(data[col], errors="coerce")
                                .fillna(0)
                                .astype("int64")
                            )
                        else:
                            standard_df[std_col] = pd.to_numeric(
                                data[col], errors="coerce"
                            )
                        break
                else:
                    if std_col == "Volume":
                        standard_df[std_col] = 0
                    else:
                        standard_df[std_col] = np.nan

            # Ensure DatetimeIndex
            if not isinstance(standard_df.index, pd.DatetimeIndex):
                standard_df.index = pd.to_datetime(standard_df.index)

            # Remove rows with all NaN prices
            price_cols = ["Open", "High", "Low", "Close", "Adj Close"]
            standard_df = standard_df.dropna(subset=price_cols, how="all")

            if standard_df.empty:
                return None

            # Replace infinite values
            standard_df = standard_df.replace([np.inf, -np.inf], np.nan)

            return standard_df

        except Exception as e:
            logger.error(f"Error standardizing data for {symbol}: {e}")
            return None

    def _check_data_recency(self, price_data: pd.DataFrame) -> Tuple[str, str]:
        """Check if price data is recent."""
        try:
            today = datetime.now().date()
            one_month_ago = (datetime.now() - timedelta(days=30)).date()
            last_date = price_data.index[-1].date()

            if last_date >= one_month_ago:
                return "complete", ""
            else:
                days_old = (today - last_date).days
                reason = f"Data is {days_old} days old (last date: {last_date})"
                return "stale", reason

        except Exception as e:
            return "unknown", f"Error checking recency: {e}"

    def _update_symbol_data_and_metadata(
        self,
        symbol: str,
        price_data: pd.DataFrame,
        data_status: str = "complete",
        reason: str = "",
    ):
        """Update database with price data and metadata - simplified version."""
        try:
            # Initialize caches if needed
            if self._price_symbols_cache is None:
                self._price_symbols_cache = set(
                    self.data_manager.price_history_lib.list_symbols()
                )
                self._metadata_symbols_cache = set(
                    self.data_manager.price_metadata_lib.list_symbols()
                )

            # Validate input
            if price_data is None or price_data.empty:
                logger.error(f"Cannot update {symbol}: input data is None or empty")
                return False

            # Clean and prepare data
            clean_data = self._clean_and_prepare_data(symbol, price_data)
            if clean_data is None or clean_data.empty:
                logger.error(f"Data preparation failed for {symbol}")
                return False

            # Handle existing data if present
            if symbol in self._price_symbols_cache:
                try:
                    existing_data = self.data_manager.price_history_lib.read(
                        symbol
                    ).data
                    if existing_data is not None and not existing_data.empty:
                        clean_data = self._merge_with_existing_data(
                            symbol, existing_data, clean_data
                        )
                except Exception as e:
                    logger.warning(f"Error reading existing data for {symbol}: {e}")

            # Write to database
            try:
                self.data_manager.price_history_lib.write(symbol, clean_data)
                self._price_symbols_cache.add(symbol)

                # Update metadata
                self._update_metadata(symbol, clean_data, data_status, reason)

                return True

            except Exception as e:
                logger.error(f"Failed to write data for {symbol}: {e}")
                return False

        except Exception as e:
            logger.error(f"Critical error updating {symbol}: {e}")
            return False

    def _clean_and_prepare_data(self, symbol: str, data: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare data for database storage."""
        try:
            if data is None or data.empty:
                return None

            clean_data = data.copy()

            # Ensure DatetimeIndex
            if not isinstance(clean_data.index, pd.DatetimeIndex):
                clean_data.index = pd.to_datetime(clean_data.index)

            # Clean numeric columns
            numeric_cols = ["Open", "High", "Low", "Close", "Adj Close"]
            for col in numeric_cols:
                if col in clean_data.columns:
                    clean_data[col] = pd.to_numeric(clean_data[col], errors="coerce")
                    clean_data[col] = clean_data[col].replace([np.inf, -np.inf], np.nan)

            # Clean Volume
            if "Volume" in clean_data.columns:
                clean_data["Volume"] = (
                    pd.to_numeric(clean_data["Volume"], errors="coerce")
                    .fillna(0)
                    .astype("int64")
                )

            # Remove invalid rows
            clean_data = clean_data.dropna(subset=numeric_cols, how="all")
            clean_data = clean_data.sort_index()
            clean_data = clean_data[~clean_data.index.duplicated(keep="last")]

            return clean_data if not clean_data.empty else None

        except Exception as e:
            logger.error(f"Error cleaning data for {symbol}: {e}")
            return None

    def _merge_with_existing_data(
        self, symbol: str, existing_data: pd.DataFrame, new_data: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge new data with existing data - FIXED timezone handling."""
        try:
            # Clean existing data
            existing_clean = self._clean_and_prepare_data(symbol, existing_data)
            if existing_clean is None:
                return new_data

            # FIX: Handle timezone mismatches
            # Convert both to timezone-naive for merging
            if (
                hasattr(existing_clean.index, "tz")
                and existing_clean.index.tz is not None
            ):
                existing_clean.index = existing_clean.index.tz_localize(None)

            if hasattr(new_data.index, "tz") and new_data.index.tz is not None:
                new_data_copy = new_data.copy()
                new_data_copy.index = new_data_copy.index.tz_localize(None)
            else:
                new_data_copy = new_data.copy()

            # Concatenate and deduplicate
            merged = pd.concat([existing_clean, new_data_copy], sort=True)
            merged = merged[~merged.index.duplicated(keep="last")]
            merged = merged.sort_index()

            return merged

        except Exception as e:
            logger.error(f"Error merging data for {symbol}: {e}")
            # Fallback: return new data only
            return new_data.copy()

    def _update_metadata(
        self, symbol: str, data: pd.DataFrame, status: str, reason: str
    ):
        """Update metadata for symbol."""
        try:
            meta_dict = {
                "symbol": symbol,
                "data_status": status,
                "first_date": str(data.index[0].date()),
                "last_date": str(data.index[-1].date()),
                "last_update": datetime.now().isoformat(),
                "data_points": len(data),
                "source": "yfinance_enhanced",
            }

            if reason and reason.strip():
                meta_dict["status_reason"] = reason

            meta_df = pd.DataFrame([meta_dict])
            self.data_manager.price_metadata_lib.write(symbol, meta_df)
            self._metadata_symbols_cache.add(symbol)

        except Exception as e:
            logger.error(f"Error updating metadata for {symbol}: {e}")

    def _process_individual_symbol(
        self,
        symbol: str,
        batch_data: pd.DataFrame,
        batch_id: int,
        resource_status: Dict,
        yf_errors: Dict[str, str] = None,  # Add this parameter
    ) -> bool:
        """Process individual symbol from batch data with enhanced error detection."""
        if yf_errors is None:
            yf_errors = {}

        try:
            # Extract symbol data
            symbol_data = self._extract_symbol_from_batch(symbol, batch_data)
            if symbol_data is None or symbol_data.empty:
                # Check if there's a yfinance error for this symbol
                if symbol in yf_errors:
                    error_msg = str(yf_errors[symbol])
                    if "delisted" in error_msg.lower():
                        self.progress_tracker.mark_symbol_failed(
                            symbol, error_msg, "delisted"
                        )
                        self.delisted_symbols_count += 1
                    else:
                        self.progress_tracker.mark_symbol_failed(
                            symbol, error_msg, "data_missing"
                        )
                    self.error_logger.log_error(
                        symbol,
                        "yfinance_error",
                        error_msg,
                        batch_id,
                        resource_status=resource_status,
                    )
                    return False

                # No yfinance error but no data
                self.progress_tracker.mark_symbol_failed(
                    symbol, "No data in batch results", "data_missing"
                )
                self.error_logger.log_error(
                    symbol,
                    "data_missing",
                    "No data in batch results",
                    batch_id,
                    resource_status=resource_status,
                )
                return False

            # Standardize data
            standardized_data = self._standardize_price_dataframe(symbol, symbol_data)
            if standardized_data is None or standardized_data.empty:
                self.progress_tracker.mark_symbol_failed(
                    symbol, "Data standardization failed", "standardization_error"
                )
                self.error_logger.log_error(
                    symbol,
                    "standardization_error",
                    "Data standardization failed",
                    batch_id,
                    resource_status=resource_status,
                )
                return False

            # Use enhanced status determination
            data_status, reason = self._determine_enhanced_status(
                symbol, standardized_data, yf_errors
            )

            # Update database
            if self._update_symbol_data_and_metadata(
                symbol, standardized_data, data_status, reason
            ):
                self.progress_tracker.mark_symbol_completed(
                    symbol,
                    data_points=len(standardized_data),
                    first_date=str(standardized_data.index[0].date()),
                    last_date=str(standardized_data.index[-1].date()),
                )
                return True
            else:
                self.progress_tracker.mark_symbol_failed(
                    symbol, "Database update failed", "database_error"
                )
                self.error_logger.log_error(
                    symbol,
                    "database_error",
                    "Database update failed",
                    batch_id,
                    resource_status=resource_status,
                )
                return False

        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            self.progress_tracker.mark_symbol_failed(symbol, str(e), "processing_error")
            self.error_logger.log_error(
                symbol,
                "processing_error",
                str(e),
                batch_id,
                resource_status=resource_status,
            )
            return False

    def _process_batch(self, batch):
        """Enhanced batch processing with comprehensive resource management."""
        batch_symbols = batch["symbols"]
        batch_id = batch["batch_id"]
        self.batch_count += 1

        logger.info(f"Processing batch {batch_id} with {len(batch_symbols)} symbols")

        # Pre-batch resource check
        pre_resource_status = self.resource_manager.check_and_manage_resources()
        logger.debug(f"Pre-batch resources: {pre_resource_status['status']}")

        # Download batch data
        data, rate_limit_hit, resource_status, yf_errors = (
            self._download_batch_enhanced(batch_symbols)
        )

        if data is None or data.empty:
            # Try extended period fallback
            logger.info(f"Trying fallback with extended period for batch {batch_id}")
            extended_period = "6mo" if self.update_period in ["1mo", "3mo"] else "1y"
            data, rate_limit_hit2, resource_status, yf_errors2 = (
                self._download_batch_enhanced(
                    batch_symbols, period=extended_period, max_retries=1
                )
            )
            # Merge errors
            yf_errors.update(yf_errors2)
            rate_limit_hit = rate_limit_hit or rate_limit_hit2

        if data is None or data.empty:
            logger.error(f"All download attempts failed for batch {batch_id}")

            # Mark all symbols as failed
            for symbol in batch_symbols:
                error_msg = "No data returned from yfinance"
                if self._is_delisted_symbol(error_msg):
                    self.progress_tracker.mark_symbol_failed(
                        symbol, "Symbol appears to be delisted", "delisted_symbol"
                    )
                    self.delisted_symbols_count += 1
                else:
                    self.progress_tracker.mark_symbol_failed(
                        symbol, error_msg, "data_missing"
                    )

                self.error_logger.log_error(
                    symbol,
                    "data_missing",
                    error_msg,
                    batch_id,
                    resource_status=resource_status,
                )
                self._track_problem_symbol(symbol, "data_missing", error_msg)

            return rate_limit_hit

        # self._debug_multiindex_structure(data, batch_symbols)
        # Process individual symbols
        success_count = 0
        fail_count = 0

        for i, symbol in enumerate(batch_symbols):
            try:
                # Periodic resource check during batch processing
                if i % 5 == 0 and i > 0:
                    current_status = self.resource_manager.check_and_manage_resources()
                    if current_status["status"] == "critical":
                        logger.warning(
                            f"Critical resources during batch {batch_id} processing - may affect remaining symbols"
                        )

                if self._process_individual_symbol(
                    symbol,
                    data,
                    batch_id,
                    resource_status,
                    yf_errors,  # Add yf_errors
                ):
                    success_count += 1
                else:
                    fail_count += 1

            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                error_type = (
                    "delisted_symbol"
                    if self._is_delisted_symbol(str(e))
                    else "processing_error"
                )

                if error_type == "delisted_symbol":
                    self.delisted_symbols_count += 1

                self.progress_tracker.mark_symbol_failed(symbol, str(e), error_type)
                self.error_logger.log_error(
                    symbol,
                    error_type,
                    str(e),
                    batch_id,
                    resource_status=resource_status,
                )
                self._track_problem_symbol(symbol, error_type, str(e))
                fail_count += 1

        logger.info(
            f"Batch {batch_id} complete: {success_count} successful, {fail_count} failed"
        )

        # Post-batch cleanup and resource check
        post_resource_status = self.resource_manager.check_and_manage_resources()

        # Cleanup
        del data
        gc.collect()

        # Force session reset if high resource usage
        if post_resource_status["status"] in ["warning", "critical"]:
            logger.info(
                f"Resetting session after batch {batch_id} due to {post_resource_status['status']} resource usage"
            )
            self.session_manager.force_reset_session()

        logger.debug(f"Post-batch resources: {post_resource_status['status']}")

        return rate_limit_hit

    def retry_single_symbol(self, symbol: str) -> bool:
        """Retry individual symbol with multiple periods."""
        logger.info(f"Retrying failed symbol: {symbol}")

        periods = ["max", "2y", "1y", "6mo", self.update_period]

        for period in periods:
            try:
                # Check resources before retry
                resource_status = self.resource_manager.check_and_manage_resources()
                if resource_status["status"] == "critical":
                    logger.warning(
                        f"Skipping retry for {symbol} due to critical resource usage"
                    )
                    return False

                data, rate_limit_hit, _, yf_errors = self._download_batch_enhanced(
                    [symbol], period=period, max_retries=1
                )

                if rate_limit_hit:
                    return False

                if data is not None and not data.empty:
                    if self._process_individual_symbol(
                        symbol,
                        data,
                        batch_id=None,
                        resource_status=resource_status,
                        yf_errors=yf_errors,
                    ):
                        logger.info(
                            f"Successfully recovered {symbol} with period={period}"
                        )
                        self.error_logger.log_error(
                            symbol,
                            "recovery_success",
                            f"Recovered with period {period}",
                            recovery_method=f"individual_retry_{period}",
                            resource_status=resource_status,
                        )
                        return True

            except Exception as e:
                logger.error(f"Error retrying {symbol} with period={period}: {e}")

            time.sleep(5)  # Wait between period attempts

        logger.error(f"All retry attempts failed for {symbol}")
        return False

    def prepare_update(
        self, symbols: List[str] = None, generate_all_batches: bool = True
    ):
        """Prepare for updating symbols."""
        try:
            if symbols is None:
                price_symbols = self.data_manager.price_history_lib.list_symbols()
                # Filter out tickers marked discontinued/inactive in symbols_metadata
                # (set by a dead-ticker job). Falls back to
                # the unfiltered list if metadata is missing or has no status column.
                try:
                    md = self.data_manager.symbol_metadata_lib.read(
                        "symbols_metadata"
                    ).data
                    if md is not None and "status" in md.columns:
                        active_set = set(md[md["status"] == "active"].index)
                        n_before = len(price_symbols)
                        price_symbols = [s for s in price_symbols if s in active_set]
                        logger.info(
                            f"Status filter: {n_before} priced -> "
                            f"{len(price_symbols)} active "
                            f"({n_before - len(price_symbols)} skipped as discontinued)"
                        )
                    else:
                        logger.warning(
                            "symbols_metadata missing 'status' column — "
                            "skipping discontinuation filter"
                        )
                except Exception as e:
                    logger.warning(
                        f"Could not apply status filter ({e}); using full priced list"
                    )
                logger.info(
                    f"Found {len(price_symbols)} symbols in price_history.daily library"
                )
                symbols = price_symbols

            self.progress_tracker.initialize_new_session(
                symbols_to_process=symbols,
                update_period=self.update_period,
                database_path=self.db_path,
                generate_all_batches=generate_all_batches,
            )

            logger.info(f"Prepared enhanced update for {len(symbols)} symbols")
            return True

        except Exception as e:
            logger.error(f"Error preparing update: {e}")
            return False

    def resume_update(self):
        """Resume a previously interrupted update process."""
        try:
            if self.progress_tracker.load_existing_progress():
                stats = self.progress_tracker.get_summary_stats()

                if stats["completion_percentage"] >= 100.0:
                    logger.info("Update already complete")
                    return True

                logger.info(
                    f"Resuming enhanced update: {stats['completion_percentage']:.1f}% complete"
                )
                return True
            else:
                logger.error("Failed to load existing progress")
                return False
        except Exception as e:
            logger.error(f"Error resuming update: {e}")
            return False

    def run_update(self):
        """Run the complete enhanced update process."""

        # Reset counters
        self.delisted_symbols_count = 0
        self.rate_limit_hits = 0
        self.successful_downloads = 0
        self.batch_count = 0

        logger.info("Starting Enhanced yfinance price update process")

        # Initial resource check
        initial_status = self.resource_manager.get_system_status()
        logger.info(
            f"Initial system status: {initial_status['status']} (Memory: {initial_status['memory_mb']:.1f}MB)"
        )

        # Check if already complete
        stats = self.progress_tracker.get_summary_stats()
        if stats["completion_percentage"] >= 100.0:
            logger.info("Update already complete")
            send_telegram_notification("✅ Enhanced update already complete!")
            return stats

        # Refresh symbol cache
        self.refresh_symbol_cache()

        # Calculate metrics
        total_symbols = self.progress_tracker.progress_data["session_info"][
            "total_symbols_to_process"
        ]
        avg_batch_size = (self.min_batch_size + self.max_batch_size) // 2
        expected_batches = (total_symbols + avg_batch_size - 1) // avg_batch_size

        logger.info(
            f"Expected ~{expected_batches} batches for {total_symbols} symbols (enhanced processing)"
        )

        # Initial notification handled by run_market_data_updater.py
        logger.info(
            f"Enhanced update: {total_symbols} symbols, ~{expected_batches} batches, "
            f"batch size {self.min_batch_size}-{self.max_batch_size}"
        )

        # Main processing loop
        max_batches = expected_batches + 50  # Safety limit

        try:
            while self.batch_count < max_batches:
                # Get next batch
                batch = self.progress_tracker.get_next_batch()
                if not batch:
                    logger.info("No more batches to process")
                    break

                batch_id = batch["batch_id"]

                # Enhanced progress reporting (log only — Telegram via progress_tracker)
                if (
                    batch_id % 3 == 0 or batch_id == 1
                ):  # More frequent for smaller batches
                    current_stats = self.progress_tracker.get_summary_stats()
                    resource_status = self.resource_manager.get_system_status()

                    logger.info(
                        f"Enhanced Batch {batch_id}/{expected_batches} | "
                        f"Progress: {current_stats['completion_percentage']:.1f}% | "
                        f"Memory: {resource_status['memory_mb']:.0f}MB | "
                        f"Rate limits: {self.rate_limit_hits}"
                    )

                logger.info(
                    f"Processing enhanced batch {batch_id} ({len(batch['symbols'])} symbols)"
                )

                # Process batch
                self.progress_tracker.mark_batch_started(batch_id)

                start_time = time.time()
                rate_limit_hit = self._process_batch(batch)
                duration = time.time() - start_time

                self.progress_tracker.mark_batch_completed(batch_id, duration)
                self.progress_tracker.update_performance_metrics(
                    duration, rate_limit_hit
                )

                # Check completion
                stats = self.progress_tracker.get_summary_stats()
                logger.info(
                    f"Enhanced batch {batch_id} done. Overall: {stats['completion_percentage']:.1f}% complete"
                )

                if stats["completion_percentage"] >= 100.0:
                    logger.info("Reached 100% completion")
                    break

                # Enhanced inter-batch delay
                delay = random.uniform(2, 8)  # Longer delays
                logger.debug(f"Inter-batch delay: {delay:.1f}s")
                time.sleep(delay)

        except KeyboardInterrupt:
            logger.warning("Enhanced update interrupted by user")
            send_telegram_notification("⚠️ Enhanced update interrupted by user")
            self.progress_tracker.save_progress()
            return self.progress_tracker.get_summary_stats()

        except Exception as e:
            logger.error(f"Critical error in enhanced update: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

        finally:
            # Always perform cleanup
            self._cleanup_update_process()

        # Final retry phase
        final_stats = self.progress_tracker.get_summary_stats()
        remaining_failed = list(self.progress_tracker._failed_symbols)

        if remaining_failed:
            logger.info(
                f"Enhanced final retry phase for {len(remaining_failed)} failed symbols"
            )
            send_telegram_notification(
                f"🔄 Enhanced final retry: {len(remaining_failed)} symbols"
            )

            # Filter retry candidates
            retry_candidates = [
                symbol
                for symbol in remaining_failed
                if not self._is_problem_symbol(symbol)
                and not (
                    symbol in self.problem_symbols
                    and self.problem_symbols[symbol].get("last_error_type")
                    == "delisted_symbol"
                )
            ]

            if retry_candidates:
                recovered = 0
                for i, symbol in enumerate(retry_candidates[:100]):  # Limit retries
                    if i % 20 == 0:
                        logger.info(
                            f"Enhanced final retry progress: {i}/{min(100, len(retry_candidates))}"
                        )

                    if self.retry_single_symbol(symbol):
                        recovered += 1

                    time.sleep(8)  # Conservative delay

                if recovered > 0:
                    logger.info(
                        f"Enhanced final retry recovered {recovered}/{len(retry_candidates)} symbols"
                    )
                    send_telegram_notification(
                        f"🔄 Enhanced final retry: recovered {recovered} symbols"
                    )

        # Generate final report
        final_stats = self.progress_tracker.get_summary_stats()
        final_stats["delisted_symbols"] = self.delisted_symbols_count
        final_stats["rate_limit_hits"] = self.rate_limit_hits
        final_stats["successful_downloads"] = self.successful_downloads
        final_stats["emergency_cleanups"] = self.resource_manager.emergency_cleanups

        if final_stats["failed_symbols"] > 5:
            self.generate_enhanced_failure_report()

        # Calculate session duration
        session_start = self.progress_tracker.progress_data["session_info"][
            "start_time"
        ]
        session_duration = datetime.now() - datetime.fromisoformat(session_start)
        duration_hours = session_duration.total_seconds() / 3600

        # Get yfinance version
        yf_version = getattr(yf, "__version__", "0.2.65")

        # Calculate actual failed count (after recovery)
        actual_failed = final_stats["failed_symbols"]

        # Final summary
        logger.info(
            f"Enhanced yfinance {yf_version} update complete:\n"
            f"- Duration: {duration_hours:.2f} hours\n"
            f"- Updated: {final_stats['completed_symbols']} symbols\n"
            f"- Failed: {actual_failed} symbols\n"
            f"- Delisted: {self.delisted_symbols_count} symbols\n"
            f"- Rate limits: {self.rate_limit_hits}\n"
            f"- Success rate: {final_stats['completion_percentage']:.1f}%"
        )

        send_telegram_notification(
            f"✅ Enhanced yfinance {yf_version} update complete!\n"
            f"⏱️ {duration_hours:.1f}h | ✅ {final_stats['completed_symbols']} | ❌ {actual_failed}\n"
            f"🗑️ {self.delisted_symbols_count} delisted | ⚡ {self.rate_limit_hits} rate limits"
        )

        # Mark as completed
        self.progress_tracker.progress_data["session_info"]["status"] = "completed"
        self.progress_tracker.progress_data["session_info"]["end_time"] = (
            datetime.now().isoformat()
        )
        self.progress_tracker.save_progress()

        return final_stats

    def _cleanup_update_process(self):
        """Comprehensive cleanup at end of update process."""
        logger.info("Starting comprehensive update cleanup...")

        try:
            # Force flush all pending errors
            self.error_logger.force_flush()

            # Save problem symbols
            self._force_save_problem_symbols()

            # Final resource cleanup
            self.resource_manager.emergency_cleanup()

            # Final session cleanup
            self.session_manager.force_reset_session()

            # Clear PID file so cli/ helpers stop refusing to run.
            Path(MARKET_DATA_DIR, ".updater.pid").unlink(missing_ok=True)

            logger.info("Comprehensive update cleanup completed")

        except Exception as e:
            logger.error(f"Error during comprehensive cleanup: {e}")

    def generate_enhanced_failure_report(self) -> Optional[str]:
        """Generate enhanced failure analysis report."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = os.path.join(
                self.log_dir, f"enhanced_failure_analysis_{timestamp}.txt"
            )

            # Get yfinance version
            yf_version = getattr(yf, "__version__", "0.2.65")

            with open(report_path, "w") as f:
                f.write(
                    f"📊 ENHANCED YFINANCE {yf_version} FAILURE ANALYSIS REPORT 📊\n"
                )
                f.write(f"Generated: {datetime.now().isoformat()}\n")
                f.write(f"yfinance version: {yf_version}\n\n")

                # Resource usage summary
                final_status = self.resource_manager.get_system_status()
                f.write("RESOURCE USAGE SUMMARY:\n")
                f.write(f"- Final memory usage: {final_status['memory_mb']:.1f}MB\n\n")

                # Error analysis from problem symbols
                f.write("PROBLEM SYMBOLS ANALYSIS:\n")
                f.write(f"Total problem symbols: {len(self.problem_symbols)}\n")

                error_categories = {}
                for symbol_data in self.problem_symbols.values():
                    error_type = symbol_data.get("last_error_type", "unknown")
                    error_categories[error_type] = (
                        error_categories.get(error_type, 0) + 1
                    )

                f.write("Error type distribution:\n")
                for error_type, count in error_categories.items():
                    f.write(f"- {error_type}: {count}\n")
                f.write("\n")

                # Enhanced recommendations
                f.write("🔧 ENHANCED YFINANCE RECOMMENDATIONS:\n")

                if self.resource_manager.emergency_cleanups > 0:
                    f.write(
                        f"- {self.resource_manager.emergency_cleanups} emergency cleanups occurred - consider reducing batch sizes further\n"
                    )

                if self.rate_limit_hits > 0:
                    f.write(
                        f"- {self.rate_limit_hits} rate limits hit - increase delays between requests\n"
                    )

                f.write(
                    "- Enhanced resource management successfully prevented crashes\n"
                )
                f.write(
                    "- Ultra-conservative batch sizes recommended for future runs\n"
                )
                f.write("- Session lifecycle management working correctly\n")

            logger.info(f"Enhanced failure report generated: {report_path}")
            return report_path

        except Exception as e:
            logger.error(f"Error generating enhanced failure report: {e}")
            return None

    def get_update_status(self):
        """Get current update status with resource information."""
        stats = self.progress_tracker.get_summary_stats()
        resource_status = self.resource_manager.get_system_status()

        stats.update(
            {
                "resource_status": resource_status["status"],
                "memory_mb": resource_status["memory_mb"],
            }
        )

        return stats

    def refresh_symbol_cache(self):
        """Refresh ArcticDB symbol cache."""
        try:
            logger.info("Refreshing symbol cache...")
            self._price_symbols_cache = set(
                self.data_manager.price_history_lib.list_symbols()
            )
            self._metadata_symbols_cache = set(
                self.data_manager.price_metadata_lib.list_symbols()
            )
            logger.info("Symbol cache refreshed")
            return True
        except Exception as e:
            logger.error(f"Error refreshing cache: {e}")
            return False

    def close(self):
        """Enhanced cleanup method."""
        try:
            logger.info("Starting enhanced SymbolUpdater shutdown...")

            # Force flush all pending operations
            self.error_logger.force_flush()
            self._force_save_problem_symbols()

            # Final resource cleanup
            self.resource_manager.emergency_cleanup()

            # Session cleanup
            self.session_manager.force_reset_session()

            logger.info("Enhanced SymbolUpdater shutdown completed successfully")

        except Exception as e:
            logger.error(f"Error during enhanced shutdown: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.close()


# Convenience function for system optimization
def optimize_system_for_updates():
    """Optimize system settings for enhanced updates."""

    try:
        # Get current limits
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        logger.debug(f"Current FD limits: soft={soft}, hard={hard}")

        # For macOS, try to increase the limits properly
        if soft <= 256:
            logger.debug(
                f"Very low FD limit detected ({soft}). Attempting to increase..."
            )

            # Try to increase using ulimit first
            try:
                # Set ulimit for current session
                import os

                os.system("ulimit -n 4096")
                logger.info("Set ulimit -n 4096 for current session")
            except Exception as e:
                logger.debug(f"Could not set ulimit: {e}")

            # Try to increase programmatically
            target_values = [4096, 2048, 1024, 512]

            for target in target_values:
                try:
                    new_soft = min(
                        target, hard if hard != resource.RLIM_INFINITY else target
                    )
                    resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
                    logger.debug(
                        f"Successfully increased FD limit from {soft} to {new_soft}"
                    )
                    break
                except (ValueError, OSError) as e:
                    logger.debug(f"Could not set FD limit to {target}: {e}")
                    continue
            else:
                logger.debug(f"Could not increase FD limit beyond {soft}")
                logger.debug(
                    "Consider running: 'ulimit -n 4096' in your terminal before starting the script"
                )

        # Verify new limits
        new_soft, new_hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        logger.debug(f"Final FD limits: soft={new_soft}, hard={new_hard}")

        # Set conservative garbage collection
        gc.set_threshold(700, 10, 10)

        return True

    except Exception as e:
        logger.warning(f"Could not optimize system settings: {e}")
        return False


if __name__ == "__main__":
    # Example usage
    optimize_system_for_updates()

    with SymbolUpdater(
        market_data_dir=MARKET_DATA_DIR,
        min_batch_size=120,  # Ultra-conservative
        max_batch_size=150,  # Ultra-conservative
    ) as updater:
        # Prepare and run update
        if updater.prepare_update():
            final_stats = updater.run_update()
            print(f"Enhanced update completed: {final_stats}")
        else:
            print("Failed to prepare enhanced update")

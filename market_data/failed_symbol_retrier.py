# failed_symbol_retrier.py
import os
import json
import logging
import gc
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple


import pandas as pd
import yfinance as yf

from market_data.settings import DB_PATH
from market_data.manager import MarketDataManager
from market_data._notify import notification_decorator, send_telegram_notification

logger = logging.getLogger("market_data.retrier")

# Set the log level
logger.setLevel(logging.INFO)  # Can use DEBUG for more verbose output

# Create log directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)

# Create and add file handler (for logging to file)
log_file = os.path.join(log_dir, "symbol_retrier.log")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(file_handler)

# Create and add console handler (for logging to console)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
logger.addHandler(console_handler)

# Prevent log propagation to avoid duplicate logs if parent loggers are configured
logger.propagate = False

print(f"Logging configured. Log messages will be written to: {log_file}")


class FailedSymbolRetrier:
    """
    Retries downloading data for symbols that previously failed,
    applying specialized strategies based on error types.

    Features:
    - Extracts failed symbols from progress files
    - Applies targeted retry strategies based on error type
    - Uses single-symbol downloads when batch downloads fail
    - Validates data recency (1-month rule)
    - Updates database with results
    - Tracks progress and generates reports
    """

    def __init__(
        self,
        market_data_dir: str,
        db_path: str = None,
        progress_file_path: str = None,
        retry_progress_file_path: str = None,
        batch_size: int = 100,
        max_retry_attempts: int = 3,
        excluded_exchanges: List[str] = None,
        output_report_path: str = None,
    ):
        """
        Initialize the retrier with configuration parameters.

        Args:
            market_data_dir: Directory where market data is stored
            db_path: Path to ArcticDB database
            progress_file_path: Path to original progress file
            retry_progress_file_path: Path for retry-specific progress
            batch_size: Number of symbols per batch
            max_retry_attempts: Maximum retries per symbol
            excluded_exchanges: List of exchange suffixes to exclude
            output_report_path: Path for final report
        """
        self.market_data_dir = market_data_dir
        self.db_path = db_path or DB_PATH
        self.batch_size = batch_size
        self.max_retry_attempts = max_retry_attempts
        self.excluded_exchanges = excluded_exchanges or [".CBT", ".NYM", ".CME"]

        # Set default file paths if not provided
        self.progress_file_path = progress_file_path or os.path.join(
            market_data_dir, "price_update_progress.json"
        )
        self.retry_progress_file_path = retry_progress_file_path or os.path.join(
            market_data_dir, "retry_progress.json"
        )
        self.output_report_path = output_report_path or os.path.join(
            market_data_dir, "retry_report.json"
        )

        # Initialize components
        logger.info(
            f"Initializing FailedSymbolRetrier with market_data_dir={market_data_dir}"
        )

        # Initialize data manager
        self.data_manager = MarketDataManager.get_instance(db_path=self.db_path)

        # Initialize tracking structures
        self.retry_progress = {
            "session_info": {
                "start_time": datetime.now().isoformat(),
                "last_update_time": datetime.now().isoformat(),
                "status": "initialized",
                "total_symbols": 0,
                "completed_symbols": 0,
                "failed_symbols": 0,
                "skipped_symbols": 0,
                "recovery_rate": 0.0,
                "memory_usage": {},
            },
            "batches": {
                "total": 0,
                "completed": 0,
                "current_batch_index": 0,
                "details": [],
            },
            "symbols": {"successful": [], "failed": [], "skipped": []},
            "error_categories": {},
            "performance_metrics": {
                "average_symbol_time": 0,
                "symbols_per_minute": 0,
                "rate_limit_hits": 0,
                "individual_downloads": 0,
            },
        }

        # Track symbols internally
        self.failed_symbols = []  # List of failed symbols with details
        self.current_batch = []  # Current batch being processed

        # Symbol metadata caches to reduce database queries
        self._price_symbols_cache = None
        self._metadata_symbols_cache = None

        logger.info("FailedSymbolRetrier initialization complete")

        # Try to load existing progress
        self.load_retry_progress()

    def extract_failed_symbols(self) -> List[Dict[str, Any]]:
        """
        Extract failed symbols from the original progress file.

        Returns:
            List of dictionaries with failed symbol details
        """
        try:
            # Check if progress file exists
            if not os.path.exists(self.progress_file_path):
                logger.error(f"Progress file not found: {self.progress_file_path}")
                return []

            # Load the progress file
            with open(self.progress_file_path, "r") as f:
                progress_data = json.load(f)

            # Extract failed symbols
            failed_symbols = []

            if (
                "symbol_status" in progress_data
                and "failed" in progress_data["symbol_status"]
            ):
                # Extract from the list of failed symbols
                failed_list = progress_data["symbol_status"]["failed"]
                logger.info(f"Found {len(failed_list)} failed symbols in progress file")

                # Process each failed symbol
                for symbol_info in failed_list:
                    symbol = symbol_info.get("symbol")
                    if not symbol:
                        continue

                    # Skip symbols from excluded exchanges
                    if any(
                        symbol.endswith(suffix) for suffix in self.excluded_exchanges
                    ):
                        logger.info(f"Skipping symbol {symbol} from excluded exchange")
                        self.retry_progress["symbols"]["skipped"].append(
                            {
                                "symbol": symbol,
                                "reason": "excluded_exchange",
                                "exchange_suffix": next(
                                    (
                                        suffix
                                        for suffix in self.excluded_exchanges
                                        if symbol.endswith(suffix)
                                    ),
                                    None,
                                ),
                            }
                        )
                        self.retry_progress["session_info"]["skipped_symbols"] += 1
                        continue

                    # Build record for retry
                    retry_info = {
                        "symbol": symbol,
                        "error": symbol_info.get("error", "Unknown error"),
                        "error_category": symbol_info.get("error_category", "unknown"),
                        "batch_id": symbol_info.get("batch_id", 0),
                        "attempts": symbol_info.get("attempts", 0),
                        "last_attempt_time": symbol_info.get("last_attempt_time", ""),
                        "retry_strategy": self._determine_retry_strategy(
                            symbol_info.get("error", ""),
                            symbol_info.get("error_category", "unknown"),
                        ),
                        "status": "pending",
                    }
                    failed_symbols.append(retry_info)

            # Update session info
            self.retry_progress["session_info"]["total_symbols"] = (
                len(failed_symbols)
                + self.retry_progress["session_info"]["skipped_symbols"]
            )

            logger.info(f"Extracted {len(failed_symbols)} failed symbols for retry")
            return failed_symbols

        except Exception as e:
            logger.error(f"Error extracting failed symbols: {str(e)}")
            return []

    def _determine_retry_strategy(self, error: str, error_category: str) -> str:
        """
        Determine the appropriate retry strategy based on error type.

        Args:
            error: Error message
            error_category: Category of error

        Returns:
            Strategy name as string
        """
        error_lower = error.lower() if error else ""

        # Rate limit errors
        if "rate limit" in error_lower or error_category == "rate_limit":
            return "backoff_retry"

        # Timezone errors
        if (
            "tzinfo" in error_lower
            or "timezone" in error_lower
            or "YFTzMissingError" in error
        ):
            return "individual_download"

        # Bad parameter errors
        if "bad parameter" in error_lower or "api misuse" in error_lower:
            return "individual_download_extended_timeout"

        # No data errors
        if (
            "no data" in error_lower
            or "not found" in error_lower
            or error_category == "data_missing"
            or error_category == "empty_data"
        ):
            return "individual_download_period_max"

        # Batch errors (might be individual symbol issues)
        if error_category == "batch_error":
            return "individual_download"

        # Default strategy
        return "standard_retry"

    def create_retry_batches(self, failed_symbols: List[Dict[str, Any]]) -> bool:
        """
        Create batches of symbols for retry processing.

        Args:
            failed_symbols: List of failed symbols with details

        Returns:
            True if batches were created successfully
        """
        try:
            if not failed_symbols:
                logger.warning("No failed symbols to process")
                return False

            # Store the full list
            self.failed_symbols = failed_symbols

            # Calculate number of batches needed
            batch_count = (len(failed_symbols) + self.batch_size - 1) // self.batch_size

            # Create batch details
            batch_details = []
            for i in range(batch_count):
                start_idx = i * self.batch_size
                end_idx = min(start_idx + self.batch_size, len(failed_symbols))

                batch_symbols = failed_symbols[start_idx:end_idx]
                batch = {
                    "batch_id": i + 1,
                    "size": len(batch_symbols),
                    "symbols": [s["symbol"] for s in batch_symbols],
                    "symbol_details": batch_symbols,
                    "status": "pending",
                }
                batch_details.append(batch)

            # Update progress data
            self.retry_progress["batches"]["total"] = batch_count
            self.retry_progress["batches"]["details"] = batch_details

            logger.info(f"Created {batch_count} batches for retry processing")
            self._save_retry_progress()
            return True

        except Exception as e:
            logger.error(f"Error creating retry batches: {str(e)}")
            return False

    def download_single_symbol(
        self,
        symbol: str,
        period: str = "max",
        retries: int = 3,
        backoff_factor: float = 2.0,
    ) -> Optional[pd.DataFrame]:
        """
        Download data for a single symbol with advanced retry logic.

        Args:
            symbol: The symbol to download
            period: The time period to download (default: "max")
            retries: Number of retry attempts
            backoff_factor: Exponential backoff factor for retries

        Returns:
            DataFrame with price data or None if all attempts fail
        """
        logger.info(f"Starting dedicated download for single symbol: {symbol}")

        # Track this as an individual download
        self.retry_progress["performance_metrics"]["individual_downloads"] += 1

        # Initialize attempt counter and wait time
        attempt = 0
        wait_time = 1.0

        while attempt < retries:
            try:
                # Use yfinance to download just this one symbol
                # We don't pass proxy or session here because we're using the global config
                data = yf.download(
                    tickers=symbol,
                    period=period,
                    interval="1d",
                    auto_adjust=False,
                    progress=False,
                    timeout=30,
                )

                # Check if data is empty
                if data.empty:
                    logger.warning(
                        f"Empty data returned for {symbol} on attempt {attempt+1}"
                    )
                    attempt += 1
                    time.sleep(wait_time)
                    wait_time *= backoff_factor
                    continue

                # Clean data (remove NaN rows)
                clean_data = data.dropna(how="all")

                if clean_data.empty:
                    logger.warning(
                        f"All rows were NaN for {symbol} on attempt {attempt+1}"
                    )
                    attempt += 1
                    time.sleep(wait_time)
                    wait_time *= backoff_factor
                    continue

                # Success - return the clean data
                logger.info(
                    f"Successfully downloaded {len(clean_data)} data points for {symbol}"
                )
                return clean_data

            except Exception as e:
                logger.error(
                    f"Error downloading {symbol} (attempt {attempt+1}): {str(e)}"
                )
                attempt += 1

                # Check if this might be a rate limit issue
                if "rate limit" in str(e).lower():
                    logger.warning(
                        f"Possible rate limit hit, waiting longer for {symbol}"
                    )
                    wait_time = max(
                        wait_time * backoff_factor, 10.0
                    )  # At least 10 seconds for rate limits
                    self.retry_progress["performance_metrics"]["rate_limit_hits"] += 1
                else:
                    wait_time *= backoff_factor

                time.sleep(wait_time)

        # If we get here, all attempts failed
        logger.error(f"Failed to download {symbol} after {retries} attempts")
        return None

    def retry_single_symbol(self, symbol: str) -> Dict[str, Any]:
        """
        Retry downloading and updating data for a single symbol.
        This method can be called directly for manual recovery of specific symbols.

        Args:
            symbol: Symbol to retry

        Returns:
            Dictionary with retry results
        """
        result = {
            "symbol": symbol,
            "success": False,
            "timestamp": datetime.now().isoformat(),
            "data_points": 0,
            "data_status": None,
            "error": None,
        }

        start_time = time.time()

        try:
            # First try with maximum period
            data = self.download_single_symbol(symbol, period="max")

            if data is None:
                # Try with a shorter period as fallback
                logger.info(f"Retrying {symbol} with shorter period")
                data = self.download_single_symbol(symbol, period="1y")

            if data is None:
                result["error"] = "Failed to download after multiple attempts"
                return result

            # Check data recency
            data_status, reason = self.check_data_recency(data)

            # Update database
            success = self.update_database(symbol, data, data_status, reason)

            if success:
                result["success"] = True
                result["data_points"] = len(data)
                result["data_status"] = data_status
                result["first_date"] = str(data.index[0].date())
                result["last_date"] = str(data.index[-1].date())

                # Add to successful symbols in progress tracking
                self._mark_symbol_completed(
                    symbol,
                    data_points=len(data),
                    first_date=str(data.index[0].date()),
                    last_date=str(data.index[-1].date()),
                    data_status=data_status,
                )

                # Update the performance metrics
                duration = time.time() - start_time
                self._update_performance_metrics(duration)
            else:
                result["error"] = "Failed to update database"
                self._mark_symbol_failed(
                    symbol, "Failed to update database", "database_error"
                )

        except Exception as e:
            logger.error(f"Error in single symbol retry for {symbol}: {str(e)}")
            result["error"] = str(e)
            self._mark_symbol_failed(symbol, str(e), "processing_error")

        return result

    def process_batch(self, batch: Dict[str, Any]) -> Tuple[int, int, bool]:
        """
        Process a batch of symbols for retry.

        Args:
            batch: Batch dictionary with symbols to process

        Returns:
            Tuple of (successful count, failed count, rate limit hit)
        """
        batch_id = batch["batch_id"]
        symbols = batch["symbols"]
        successful_count = 0
        failed_count = 0
        rate_limit_hit = False

        logger.info(f"Processing batch {batch_id} with {len(symbols)} symbols")
        send_telegram_notification(
            f"▶️ Retrying batch {batch_id} with {len(symbols)} symbols"
        )

        # Mark batch as started
        batch["status"] = "in_progress"
        batch["start_time"] = datetime.now().isoformat()
        self._save_retry_progress()

        # Try to download data for batch
        try:
            # Use yfinance to download data for all symbols in the batch
            # We don't pass proxy or session here because we're using the global config
            logger.debug(f"Downloading data for {len(symbols)} symbols")
            data = yf.download(
                tickers=symbols,
                period="max",  # Use max period for retries to ensure we get enough history
                interval="1d",
                group_by="ticker",  # Group by ticker to ensure proper MultiIndex structure
                auto_adjust=False,
                progress=True,
                timeout=30,
            )

            # Force garbage collection to free memory after download
            gc.collect()

            # Verify data structure for first symbol to catch potential issues early
            if not data.empty and len(symbols) > 0:
                first_symbol = symbols[0]
                try:
                    if len(symbols) > 1:
                        # Multi-symbol result is MultiIndex DataFrame
                        if first_symbol in data.columns.levels[0]:
                            first_symbol_data = data.xs(first_symbol, level=0, axis=1)
                            # Verify column structure
                            self._verify_column_structure(
                                first_symbol, first_symbol_data
                            )
                        else:
                            logger.warning(
                                f"First symbol {first_symbol} not found in batch results"
                            )
                    else:
                        # Single symbol result
                        self._verify_column_structure(first_symbol, data)
                except ValueError as e:
                    error_msg = f"Column structure verification failed: {str(e)}"
                    logger.error(error_msg)
                    send_telegram_notification(f"❌ ERROR: {error_msg}")
                    raise  # Re-raise to trigger batch-level error handling

            # Process individual symbols from batch result
            for symbol in symbols:
                # Find the corresponding symbol details
                symbol_detail = next(
                    (s for s in batch["symbol_details"] if s["symbol"] == symbol), None
                )
                if not symbol_detail:
                    logger.warning(f"Symbol detail not found for {symbol}")
                    continue

                retry_strategy = symbol_detail["retry_strategy"]
                error_category = symbol_detail["error_category"]

                # Process using the batch data
                symbol_start_time = time.time()
                try:
                    symbol_success = self.handle_individual_symbol(
                        symbol,
                        data,
                        batch_download=True,
                        retry_strategy=retry_strategy,
                        error_category=error_category,
                    )

                    if symbol_success:
                        successful_count += 1
                    else:
                        failed_count += 1

                    # Update performance metrics
                    symbol_duration = time.time() - symbol_start_time
                    self._update_performance_metrics(symbol_duration)

                except Exception as e:
                    logger.error(
                        f"Error processing symbol {symbol} from batch: {str(e)}"
                    )
                    self._mark_symbol_failed(symbol, str(e), "processing_error")
                    failed_count += 1

                # Clean up after each symbol to manage memory
                if successful_count % 10 == 0:
                    gc.collect()

        except Exception as batch_error:
            logger.error(f"Batch download error: {str(batch_error)}")

            # Check for rate limiting
            if "rate limit" in str(batch_error).lower():
                logger.warning("Rate limit detected during batch processing")
                send_telegram_notification(
                    "⚠️ Rate limit hit during retry batch processing"
                )
                rate_limit_hit = True
                self.retry_progress["performance_metrics"]["rate_limit_hits"] += 1

                # Mark all symbols for individual retry
                for symbol in symbols:
                    symbol_detail = next(
                        (s for s in batch["symbol_details"] if s["symbol"] == symbol),
                        None,
                    )
                    if symbol_detail and symbol_detail.get("status") != "completed":
                        symbol_detail["retry_strategy"] = "individual_download"
                        symbol_detail["status"] = "pending"

            # Try individual downloads for each symbol in the batch
            logger.info("Attempting individual downloads for batch symbols")
            for symbol in symbols:
                symbol_detail = next(
                    (s for s in batch["symbol_details"] if s["symbol"] == symbol), None
                )
                if symbol_detail and symbol_detail.get("status") != "completed":
                    symbol_start_time = time.time()
                    try:
                        # Wait a bit between individual downloads
                        time.sleep(1.0)

                        # Try individual download
                        symbol_success = self.retry_single_symbol(symbol)

                        if symbol_success["success"]:
                            successful_count += 1
                        else:
                            failed_count += 1

                        # Update performance metrics
                        symbol_duration = time.time() - symbol_start_time
                        self._update_performance_metrics(symbol_duration)

                    except Exception as e:
                        logger.error(
                            f"Error processing individual symbol {symbol}: {str(e)}"
                        )
                        self._mark_symbol_failed(
                            symbol, str(e), "individual_processing_error"
                        )
                        failed_count += 1

                    # Clean up after each symbol
                    if (successful_count + failed_count) % 10 == 0:
                        gc.collect()

        # Mark batch as completed
        batch["status"] = "completed"
        batch["end_time"] = datetime.now().isoformat()
        batch["successful_count"] = successful_count
        batch["failed_count"] = failed_count

        # Calculate duration
        if "start_time" in batch:
            start_time = datetime.fromisoformat(batch["start_time"])
            end_time = datetime.fromisoformat(batch["end_time"])
            duration_seconds = (end_time - start_time).total_seconds()
            batch["duration_seconds"] = duration_seconds

        # Update progress
        self.retry_progress["batches"]["completed"] += 1

        # Record memory usage
        try:
            import psutil

            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            self.retry_progress["session_info"]["memory_usage"]["after_batch"] = {
                "rss_mb": memory_info.rss / (1024 * 1024),
                "vms_mb": memory_info.vms / (1024 * 1024),
                "batch_id": batch_id,
                "timestamp": datetime.now().isoformat(),
            }
        except Exception:
            pass

        # Final garbage collection
        gc.collect()

        self._save_retry_progress()

        logger.info(
            f"Batch {batch_id} completed: {successful_count} successful, {failed_count} failed"
        )
        return successful_count, failed_count, rate_limit_hit

    def handle_individual_symbol(
        self,
        symbol,
        batch_data,
        batch_download=True,
        retry_strategy="standard_retry",
        error_category="unknown",
    ):
        """
        Process a single symbol, either from batch data or individual download.

        Args:
            symbol: Symbol to process
            batch_data: Batch download data (if from batch)
            batch_download: Whether this is from a batch download
            retry_strategy: Strategy to use for retry
            error_category: Original error category

        Returns:
            True if successful, False otherwise
        """
        symbol_start_time = time.time()

        try:
            # Extract data for this symbol if from batch
            if batch_download and batch_data is not None:
                # For multi-symbol download with group_by="ticker"
                if (
                    isinstance(batch_data.columns, pd.MultiIndex)
                    and len(batch_data.columns) > 0
                ):
                    # Check if first level contains symbols (group_by="ticker" case)
                    if symbol in batch_data.columns.levels[0]:
                        # Access directly with symbol name
                        symbol_data = batch_data[symbol].copy()
                        # Columns are already standard: Open, High, Low, Close, Adj Close, Volume
                    elif (
                        len(batch_data.columns[0]) > 1
                        and batch_data.columns[0][1] == symbol
                    ):
                        # Single symbol case with (Column, Symbol) format
                        symbol_data = pd.DataFrame()
                        # Map each column to the standard name
                        for col_type in [
                            "Open",
                            "High",
                            "Low",
                            "Close",
                            "Adj Close",
                            "Volume",
                        ]:
                            if (col_type, symbol) in batch_data.columns:
                                symbol_data[col_type] = batch_data[(col_type, symbol)]
                    else:
                        logger.warning(f"Symbol {symbol} not found in batch results")
                        self._mark_symbol_failed(
                            symbol, "Symbol not found in batch results", "data_missing"
                        )
                        return False
                else:
                    # Simple DataFrame with standard columns (could be single symbol without MultiIndex)
                    symbol_data = batch_data.copy()
            else:
                # Download data individually based on retry strategy
                period = (
                    "max"
                    if retry_strategy == "individual_download_period_max"
                    else "1y"
                )
                symbol_data = self.download_single_symbol(symbol, period=period)

            # Ensure we have data
            if symbol_data is None or (
                isinstance(symbol_data, pd.DataFrame) and symbol_data.empty
            ):
                self._mark_symbol_failed(symbol, "No data returned", "data_missing")
                return False

            # Clean data (remove NaN rows)
            symbol_data = symbol_data.dropna(how="all")


            # Fix for MultiIndex columns - flatten if needed
            if isinstance(symbol_data.columns, pd.MultiIndex):
                logger.info(f"Flattening MultiIndex columns for {symbol}")
                # Take appropriate level of MultiIndex based on structure
                if symbol_data.columns.nlevels > 1:
                    symbol_data.columns = [
                        col[0] if isinstance(col, tuple) else col
                        for col in symbol_data.columns
                    ]

            # Check if we still have data after cleaning
            if symbol_data.empty:
                self._mark_symbol_failed(symbol, "All rows were NaN", "empty_data")
                return False

            # Check data recency (1-month rule)
            data_status, reason = self.check_data_recency(symbol_data)

            # Update database
            success = self.update_database(symbol, symbol_data, data_status, reason)

            if success:
                # Mark as completed with appropriate status
                self._mark_symbol_completed(
                    symbol,
                    data_points=len(symbol_data),
                    first_date=str(symbol_data.index[0].date()),
                    last_date=str(symbol_data.index[-1].date()),
                    data_status=data_status,
                )

                # Update performance metrics
                symbol_duration = time.time() - symbol_start_time
                self._update_performance_metrics(symbol_duration)

                return True
            else:
                self._mark_symbol_failed(
                    symbol, "Failed to update database", "database_error"
                )
                return False

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error processing symbol {symbol}: {error_msg}")
            self._mark_symbol_failed(symbol, error_msg, "processing_error")
            return False

    def check_data_recency(self, price_data: pd.DataFrame) -> Tuple[str, str]:
        """
        Check if the price data is recent (within 1 month of current date).

        Args:
            price_data: DataFrame with price data

        Returns:
            Tuple of (data_status, reason)
        """
        try:
            # Get today's date and 1 month ago
            today = datetime.now().date()
            one_month_ago = (datetime.now() - timedelta(days=30)).date()

            # Get the last date in the price data
            last_date = price_data.index[-1].date()

            # Check if data is recent
            if last_date >= one_month_ago:
                return "complete", ""
            else:
                # Calculate how many days old the data is
                days_old = (today - last_date).days
                reason = f"Data is {days_old} days old (last date: {last_date})"
                logger.info(f"Symbol data is stale: {reason}")
                return "stale", reason

        except Exception as e:
            logger.error(f"Error checking data recency: {str(e)}")
            return "unknown", f"Error checking recency: {str(e)}"

    def update_database(
        self, symbol: str, price_data: pd.DataFrame, data_status: str, reason: str
    ) -> bool:
        """
        Update the database with the retrieved price data and metadata.

        Args:
            symbol: Symbol identifier
            price_data: Price data DataFrame
            data_status: Status of the data (complete, stale, etc.)
            reason: Reason for the status

        Returns:
            True if successful, False otherwise
        """
        try:
            # Initialize symbol caches if not already done
            if (
                not hasattr(self, "_price_symbols_cache")
                or self._price_symbols_cache is None
            ):
                list_start = time.time()
                self._price_symbols_cache = set(
                    self.data_manager.price_history_lib.list_symbols()
                )
                self._metadata_symbols_cache = set(
                    self.data_manager.price_metadata_lib.list_symbols()
                )
                logger.debug(f"Cached symbol lists in {time.time() - list_start:.2f}s")

            # 1. Update price_history.daily with new price data
            # Check if we have existing data to merge with
            combined_data = price_data.copy()
            try:
                if symbol in self._price_symbols_cache:
                    # Read existing data
                    existing_data = self.data_manager.price_history_lib.read(
                        symbol
                    ).data

                    if not existing_data.empty:
                        # Combine existing and new data
                        combined_data = pd.concat([existing_data, price_data])
                        # Remove duplicates by date, keeping newer data
                        combined_data = combined_data[
                            ~combined_data.index.duplicated(keep="last")
                        ]
                        # Sort by date
                        combined_data = combined_data.sort_index()

                        logger.debug(
                            f"Merged {len(price_data)} new points with {len(existing_data)} existing points for {symbol}"
                        )

                # Ensure all standard columns exist
                standard_columns = [
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Adj Close",
                    "Volume",
                ]
                for col in standard_columns:
                    if col not in combined_data.columns:
                        logger.warning(
                            f"Missing column {col} for {symbol}, adding empty column"
                        )
                        combined_data[col] = pd.Series(
                            float("nan"), index=combined_data.index
                        )

                # Reorder columns to match standard order
                combined_data = combined_data[standard_columns]

                # Write the combined data
                self.data_manager.price_history_lib.write(symbol, combined_data)
                logger.info(
                    f"Updated price data for {symbol} with {len(combined_data)} data points"
                )

                # Update cache
                self._price_symbols_cache.add(symbol)

            except Exception as e:
                logger.error(f"Error updating price data for {symbol}: {str(e)}")
                return False

            # 2. Update price_history.metadata
            try:
                # Check if metadata exists
                meta_exists = symbol in self._metadata_symbols_cache

                if meta_exists:
                    # Read existing metadata
                    meta_df = self.data_manager.price_metadata_lib.read(symbol).data

                    if not meta_df.empty:
                        # Update existing metadata
                        meta_df.loc[0, "data_status"] = data_status
                        meta_df.loc[0, "first_date"] = str(
                            combined_data.index[0].date()
                        )
                        meta_df.loc[0, "last_date"] = str(
                            combined_data.index[-1].date()
                        )
                        meta_df.loc[0, "last_update"] = datetime.now().isoformat()
                        meta_df.loc[0, "data_points"] = len(combined_data)
                        meta_df.loc[0, "source"] = "yfinance"

                        # Add status reason if provided
                        if reason and "status_reason" not in meta_df.columns:
                            meta_df["status_reason"] = None

                        if reason:
                            meta_df.loc[0, "status_reason"] = reason

                        # Add recovery note
                        if "update_notes" not in meta_df.columns:
                            meta_df["update_notes"] = None

                        # Add recovery note to existing notes or create new
                        recovery_note = "Recovered by retry process"
                        if (
                            pd.isna(meta_df.loc[0, "update_notes"])
                            or meta_df.loc[0, "update_notes"] is None
                        ):
                            meta_df.loc[0, "update_notes"] = recovery_note
                        else:
                            meta_df.loc[0, "update_notes"] = (
                                f"{meta_df.loc[0, 'update_notes']}; {recovery_note}"
                            )
                    else:
                        # Create new metadata if existing is empty
                        meta_df = pd.DataFrame(
                            [
                                {
                                    "symbol": symbol,
                                    "data_status": data_status,
                                    "first_date": str(combined_data.index[0].date()),
                                    "last_date": str(combined_data.index[-1].date()),
                                    "last_update": datetime.now().isoformat(),
                                    "data_points": len(combined_data),
                                    "source": "yfinance",
                                    "status_reason": reason if reason else None,
                                    "update_notes": "Recovered by retry process",
                                }
                            ]
                        )
                else:
                    # Create new metadata
                    meta_df = pd.DataFrame(
                        [
                            {
                                "symbol": symbol,
                                "data_status": data_status,
                                "first_date": str(combined_data.index[0].date()),
                                "last_date": str(combined_data.index[-1].date()),
                                "last_update": datetime.now().isoformat(),
                                "data_points": len(combined_data),
                                "source": "yfinance",
                                "status_reason": reason if reason else None,
                                "update_notes": "Recovered by retry process",
                            }
                        ]
                    )

                # Write metadata
                self.data_manager.price_metadata_lib.write(symbol, meta_df)
                logger.info(f"Updated metadata for {symbol} with status: {data_status}")

                # Update cache
                self._metadata_symbols_cache.add(symbol)

                return True

            except Exception as e:
                logger.error(f"Error updating metadata for {symbol}: {str(e)}")
                return False

        except Exception as e:
            logger.error(f"Error in database update for {symbol}: {str(e)}")
            return False

    def _mark_symbol_completed(
        self,
        symbol: str,
        data_points: int,
        first_date: str,
        last_date: str,
        data_status: str,
    ) -> None:
        """
        Mark a symbol as successfully completed.

        Args:
            symbol: Symbol identifier
            data_points: Number of data points
            first_date: First date in the data
            last_date: Last date in the data
            data_status: Status of the data (complete, stale)
        """
        # Limit the size of successful symbols list
        if len(self.retry_progress["symbols"]["successful"]) >= 1000:
            # Keep only the most recent 1000 entries
            self.retry_progress["symbols"]["successful"] = self.retry_progress[
                "symbols"
            ]["successful"][-999:]

        # Add to successful symbols
        self.retry_progress["symbols"]["successful"].append(
            {
                "symbol": symbol,
                "update_time": datetime.now().isoformat(),
                "data_points": data_points,
                "first_date": first_date,
                "last_date": last_date,
                "data_status": data_status,
            }
        )

        # Update session info
        self.retry_progress["session_info"]["completed_symbols"] += 1
        self.retry_progress["session_info"][
            "last_update_time"
        ] = datetime.now().isoformat()

        # Calculate recovery rate
        total = self.retry_progress["session_info"]["total_symbols"]
        if total > 0:
            self.retry_progress["session_info"]["recovery_rate"] = (
                self.retry_progress["session_info"]["completed_symbols"]
                / (total - self.retry_progress["session_info"]["skipped_symbols"])
            ) * 100

        logger.info(f"Marked symbol {symbol} as completed with status: {data_status}")

    def _mark_symbol_failed(self, symbol: str, error: str, error_category: str) -> None:
        """
        Mark a symbol as failed.

        Args:
            symbol: Symbol identifier
            error: Error message
            error_category: Category of error
        """
        # Update error categories count
        if error_category not in self.retry_progress["error_categories"]:
            self.retry_progress["error_categories"][error_category] = 0
        self.retry_progress["error_categories"][error_category] += 1

        # Limit the size of failed symbols list
        if len(self.retry_progress["symbols"]["failed"]) >= 1000:
            # Keep only the most recent 1000 entries
            self.retry_progress["symbols"]["failed"] = self.retry_progress["symbols"][
                "failed"
            ][-999:]

        # Add to failed symbols
        self.retry_progress["symbols"]["failed"].append(
            {
                "symbol": symbol,
                "error": error,
                "error_category": error_category,
                "attempt_time": datetime.now().isoformat(),
            }
        )

        # Update session info
        self.retry_progress["session_info"]["failed_symbols"] += 1
        self.retry_progress["session_info"][
            "last_update_time"
        ] = datetime.now().isoformat()

        logger.info(f"Marked symbol {symbol} as failed: {error_category}")

    def _update_performance_metrics(self, symbol_duration: float) -> None:
        """
        Update performance metrics after processing a symbol.

        Args:
            symbol_duration: Time taken to process the symbol in seconds
        """
        metrics = self.retry_progress["performance_metrics"]

        # Update average symbol time with exponential moving average
        if metrics["average_symbol_time"] == 0:
            metrics["average_symbol_time"] = symbol_duration
        else:
            metrics["average_symbol_time"] = (
                0.9 * metrics["average_symbol_time"] + 0.1 * symbol_duration
            )

        # Update symbols per minute
        if symbol_duration > 0:
            symbols_per_minute = 60 / symbol_duration

            if metrics["symbols_per_minute"] == 0:
                metrics["symbols_per_minute"] = symbols_per_minute
            else:
                metrics["symbols_per_minute"] = (
                    0.9 * metrics["symbols_per_minute"] + 0.1 * symbols_per_minute
                )

    def _save_retry_progress(self) -> bool:
        """
        Save current retry progress to file.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.retry_progress_file_path), exist_ok=True)

            # Create a copy of progress data for saving to reduce file size
            save_data = self.retry_progress.copy()

            # Limit the number of symbols saved in the file
            if "symbols" in save_data:
                # Keep only the 500 most recent successful and failed symbols
                if (
                    "successful" in save_data["symbols"]
                    and len(save_data["symbols"]["successful"]) > 500
                ):
                    save_data["symbols"]["successful"] = save_data["symbols"][
                        "successful"
                    ][-500:]
                if (
                    "failed" in save_data["symbols"]
                    and len(save_data["symbols"]["failed"]) > 500
                ):
                    save_data["symbols"]["failed"] = save_data["symbols"]["failed"][
                        -500:
                    ]

            # Optimize batch data storage
            if "batches" in save_data and "details" in save_data["batches"]:
                for batch in save_data["batches"]["details"]:
                    # For completed batches, remove symbol_details to save space
                    if batch.get("status") == "completed":
                        if "symbol_details" in batch:
                            del batch["symbol_details"]

            # Save to a temporary file first
            temp_file = f"{self.retry_progress_file_path}.tmp"

            with open(temp_file, "w") as f:
                json.dump(save_data, f, indent=2)

            # Rename to the actual file (atomic operation)
            os.replace(temp_file, self.retry_progress_file_path)

            return True

        except Exception as e:
            logger.error(f"Failed to save retry progress file: {str(e)}")
            return False

    def load_retry_progress(self) -> bool:
        """
        Load existing retry progress from file.

        Returns:
            True if successfully loaded, False otherwise
        """
        try:
            # Check if progress file exists
            if not os.path.exists(self.retry_progress_file_path):
                logger.info(
                    f"No existing retry progress file found at {self.retry_progress_file_path}"
                )
                return False

            # Load the progress file
            with open(self.retry_progress_file_path, "r") as f:
                loaded_progress = json.load(f)

            # Validate basic structure
            required_keys = [
                "session_info",
                "batches",
                "symbols",
                "performance_metrics",
            ]
            if not all(key in loaded_progress for key in required_keys):
                logger.error("Retry progress file missing required keys, cannot load")
                return False

            # Update our progress data
            self.retry_progress = loaded_progress

            # Log the loaded state
            logger.info(
                f"Loaded retry progress: "
                f"{self.retry_progress['session_info'].get('completed_symbols', 0)} completed, "
                f"{self.retry_progress['session_info'].get('failed_symbols', 0)} failed"
            )

            return True

        except Exception as e:
            logger.error(f"Failed to load retry progress file: {str(e)}")
            return False

    @notification_decorator(
        message="Starting retry of failed symbols", notify_on_error=True
    )
    def run(self, specific_symbols: List[str] = None) -> Dict[str, Any]:
        """
        Run the retry process for failed symbols.

        Args:
            specific_symbols: Optional list of specific symbols to retry

        Returns:
            Final report dictionary
        """
        start_time = time.time()

        try:
            # Record initial memory usage
            try:
                import psutil

                process = psutil.Process(os.getpid())
                memory_info = process.memory_info()
                self.retry_progress["session_info"]["memory_usage"]["initial"] = {
                    "rss_mb": memory_info.rss / (1024 * 1024),
                    "vms_mb": memory_info.vms / (1024 * 1024),
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception:
                pass

            # 1. Extract failed symbols
            failed_symbols = self.extract_failed_symbols()

            # Filter to specific symbols if provided
            if specific_symbols:
                failed_symbols = [
                    s for s in failed_symbols if s["symbol"] in specific_symbols
                ]
                logger.info(f"Filtered to {len(failed_symbols)} specified symbols")
                self.retry_progress["session_info"]["total_symbols"] = len(
                    failed_symbols
                )

            if not failed_symbols:
                logger.warning("No failed symbols to retry")
                return self.generate_retry_report()

            # 2. Create batches
            if not self.create_retry_batches(failed_symbols):
                logger.error("Failed to create retry batches")
                return self.generate_retry_report()

            # 3. Process each batch
            total_successful = 0
            total_failed = 0

            for batch in self.retry_progress["batches"]["details"]:
                # Skip completed batches
                if batch["status"] == "completed":
                    logger.info(f"Skipping already completed batch {batch['batch_id']}")
                    total_successful += batch.get("successful_count", 0)
                    total_failed += batch.get("failed_count", 0)
                    continue

                # Update current batch index
                self.retry_progress["batches"]["current_batch_index"] = batch[
                    "batch_id"
                ]
                self._save_retry_progress()

                # Process batch
                successful, failed, rate_limit_hit = self.process_batch(batch)
                total_successful += successful
                total_failed += failed

                # Log progress after each batch
                logger.info(
                    f"Batch {batch['batch_id']} complete: {successful} successful, {failed} failed"
                )

                # Send notification for key batches
                if batch["batch_id"] % 5 == 0 or batch["batch_id"] == 1:
                    recovery_rate = self.retry_progress["session_info"]["recovery_rate"]
                    symbols_per_minute = self.retry_progress["performance_metrics"][
                        "symbols_per_minute"
                    ]
                    send_telegram_notification(
                        f"✅ Retry batch {batch['batch_id']} complete: {successful}✅/{failed}❌\n"
                        f"Overall: {total_successful} recovered ({recovery_rate:.1f}%)\n"
                        f"Speed: {symbols_per_minute:.1f} symbols/min"
                    )

                # Add delay between batches
                time.sleep(2.0)

                # Force garbage collection between batches
                gc.collect()

                # Check for completion
                if (
                    self.retry_progress["batches"]["current_batch_index"]
                    >= self.retry_progress["batches"]["total"]
                ):
                    logger.info("All batches completed")
                    break

            # 4. Generate final report
            self.retry_progress["session_info"]["status"] = "completed"
            duration = time.time() - start_time
            self.retry_progress["session_info"]["total_duration_seconds"] = duration

            # Record final memory usage
            try:
                import psutil

                process = psutil.Process(os.getpid())
                memory_info = process.memory_info()
                self.retry_progress["session_info"]["memory_usage"]["final"] = {
                    "rss_mb": memory_info.rss / (1024 * 1024),
                    "vms_mb": memory_info.vms / (1024 * 1024),
                    "timestamp": datetime.now().isoformat(),
                }
            except Exception:
                pass

            self._save_retry_progress()

            # Generate and save final report
            report = self.generate_retry_report()

            # Send completion notification
            recovery_rate = self.retry_progress["session_info"]["recovery_rate"]
            send_telegram_notification(
                f"🎉 Symbol retry process complete!\n"
                f"- {total_successful} symbols recovered ({recovery_rate:.1f}%)\n"
                f"- {total_failed} symbols still failed\n"
                f"- Total duration: {duration/60:.1f} minutes"
            )

            return report

        except Exception as e:
            logger.error(f"Error in retry process: {str(e)}")
            self.retry_progress["session_info"]["status"] = "error"
            self.retry_progress["session_info"]["error"] = str(e)
            self._save_retry_progress()

            # Send error notification
            send_telegram_notification(f"❌ Error in retry process: {str(e)}")

            return self.generate_retry_report()

    def generate_retry_report(self) -> Dict[str, Any]:
        """
        Generate a comprehensive report of the retry process.

        Returns:
            Report dictionary
        """
        # Create base report structure
        report = {
            "summary": {
                "total_symbols": self.retry_progress["session_info"]["total_symbols"],
                "successful": self.retry_progress["session_info"]["completed_symbols"],
                "failed": self.retry_progress["session_info"]["failed_symbols"],
                "skipped": self.retry_progress["session_info"]["skipped_symbols"],
                "recovery_rate": self.retry_progress["session_info"]["recovery_rate"],
                "start_time": self.retry_progress["session_info"]["start_time"],
                "end_time": datetime.now().isoformat(),
                "status": self.retry_progress["session_info"]["status"],
            },
            "performance": {
                "average_symbol_time": self.retry_progress["performance_metrics"][
                    "average_symbol_time"
                ],
                "symbols_per_minute": self.retry_progress["performance_metrics"][
                    "symbols_per_minute"
                ],
                "rate_limit_hits": self.retry_progress["performance_metrics"][
                    "rate_limit_hits"
                ],
                "individual_downloads": self.retry_progress["performance_metrics"][
                    "individual_downloads"
                ],
            },
            "error_categories": self.retry_progress["error_categories"],
            "data_status": {
                "complete": sum(
                    1
                    for s in self.retry_progress["symbols"]["successful"]
                    if s.get("data_status") == "complete"
                ),
                "stale": sum(
                    1
                    for s in self.retry_progress["symbols"]["successful"]
                    if s.get("data_status") == "stale"
                ),
            },
            "memory_usage": self.retry_progress["session_info"].get("memory_usage", {}),
            "batch_summary": {
                "total_batches": self.retry_progress["batches"]["total"],
                "completed_batches": self.retry_progress["batches"]["completed"],
            },
        }

        # Add stale symbols information
        report["stale_symbols"] = [
            {
                "symbol": s["symbol"],
                "last_date": s["last_date"],
            }
            for s in self.retry_progress["symbols"]["successful"][
                :100
            ]  # Limit to first 100
            if s.get("data_status") == "stale"
        ]

        # Add information about persistent failures
        error_distribution = {}
        for s in self.retry_progress["symbols"]["failed"]:
            error_cat = s.get("error_category", "unknown")
            if error_cat not in error_distribution:
                error_distribution[error_cat] = 0
            error_distribution[error_cat] += 1

        report["error_distribution"] = error_distribution

        # Add sample of persistent failures
        report["persistent_failures"] = [
            {
                "symbol": s["symbol"],
                "error_category": s.get("error_category", "unknown"),
                "error": s.get("error", "Unknown error"),
            }
            for s in self.retry_progress["symbols"]["failed"][:50]  # Limit to first 50
        ]

        # Calculate duration if available
        if "total_duration_seconds" in self.retry_progress["session_info"]:
            duration = self.retry_progress["session_info"]["total_duration_seconds"]
            report["summary"]["duration_minutes"] = duration / 60
            report["summary"]["duration_seconds"] = duration

        # Add recommendations for persistent failures
        report["recommendations"] = self._generate_recommendations(report)

        # Save report to file
        try:
            with open(self.output_report_path, "w") as f:
                json.dump(report, f, indent=2)
            logger.info(f"Saved retry report to {self.output_report_path}")
        except Exception as e:
            logger.error(f"Failed to save retry report: {str(e)}")

        return report

    def _generate_recommendations(self, report: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate recommendations based on the retry results.

        Args:
            report: The retry report data

        Returns:
            Dictionary of recommendations
        """
        recommendations = {"general": [], "by_error_category": {}}

        # Calculate recovery rate
        recovery_rate = report["summary"].get("recovery_rate", 0)

        # General recommendations
        if recovery_rate > 75:
            recommendations["general"].append(
                "High recovery rate suggests most failures were temporary. Consider implementing automatic retry for these types of failures."
            )
        elif recovery_rate > 40:
            recommendations["general"].append(
                "Moderate recovery rate. Consider improving batch processing to reduce initial failures."
            )
        else:
            recommendations["general"].append(
                "Low recovery rate suggests persistent issues with these symbols. Consider more detailed investigation."
            )

        # Check for stale data
        stale_count = report["data_status"].get("stale", 0)
        complete_count = report["data_status"].get("complete", 0)

        if stale_count > 0:
            stale_percentage = (
                (stale_count / (stale_count + complete_count)) * 100
                if (stale_count + complete_count) > 0
                else 0
            )
            if stale_percentage > 50:
                recommendations["general"].append(
                    f"High percentage of stale data ({stale_percentage:.1f}%). Consider implementing special handling for symbols with outdated data."
                )

        # Error category specific recommendations
        error_distribution = report.get("error_distribution", {})

        for category, count in error_distribution.items():
            if "rate_limit" in category:
                recommendations["by_error_category"][category] = [
                    "Implement more aggressive rate limiting and longer backoff times",
                    "Consider splitting downloads across multiple sessions or days",
                ]
            elif "data_missing" in category:
                recommendations["by_error_category"][category] = [
                    "These symbols may be delisted or have changed identifiers",
                    "Consider implementing a symbol verification step before download attempts",
                ]
            elif "timezone" in category or "tz" in category:
                recommendations["by_error_category"][category] = [
                    "Pre-cache timezone information for these symbols",
                    "Use alternative data sources for these specific exchanges",
                ]
            elif "processing_error" in category:
                recommendations["by_error_category"][category] = [
                    "Implement more robust error handling",
                    "Consider adding more specialized retry strategies",
                ]

        return recommendations

    def _verify_column_structure(self, symbol: str, data: pd.DataFrame) -> bool:
        """
        Verify that the downloaded data has the expected column structure.

        Args:
            symbol: Symbol being processed
            data: DataFrame with downloaded data

        Returns:
            True if structure is valid, raises ValueError otherwise
        """
        expected_columns = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

        # Check if we got a DataFrame
        if data is None or not isinstance(data, pd.DataFrame):
            error_msg = f"Invalid data type for {symbol}: {type(data)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Check if data is empty
        if data.empty:
            error_msg = f"Empty data for {symbol}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Check column structure
        actual_columns = list(data.columns)

        # If the columns are MultiIndex, we need to extract differently
        if isinstance(data.columns, pd.MultiIndex):
            logger.warning(
                f"Unexpected MultiIndex in data for {symbol}: {data.columns}"
            )
            raise ValueError(
                f"Unexpected column structure for {symbol}: columns are MultiIndex"
            )

        # Check if all expected columns are present
        missing_cols = set(expected_columns) - set(actual_columns)
        extra_cols = set(actual_columns) - set(expected_columns)

        if missing_cols:
            error_msg = f"Missing columns for {symbol}: {missing_cols}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # It's okay to have extra columns, but log a warning
        if extra_cols:
            logger.warning(f"Extra columns for {symbol}: {extra_cols}")

        logger.debug(f"Column structure verification passed for {symbol}")
        return True

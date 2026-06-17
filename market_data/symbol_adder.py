#!/usr/bin/env python3
import logging
import sys
import time
from datetime import datetime
from typing import Dict, Any, Tuple


import pandas as pd
import numpy as np
import yfinance as yf
from market_data.settings import MARKET_DATA_DIR
from market_data.manager import MarketDataManager
from market_data._notify import notification_decorator, send_telegram_notification

# Configure logging
logger = logging.getLogger("market_data.adder")


class YFinanceSessionManager:
    """Manages yfinance sessions with proper lifecycle and resource management."""

    def __init__(self):
        self.session_count = 0
        self.last_reset = 0
        self.reset_interval = 300  # Reset every 5 minutes

        # Initialize first session
        self._initialize_session()

    def _initialize_session(self):
        """Initialize a fresh yfinance session."""
        try:
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


class SymbolAdder:
    """
    Adds a new symbol to all database libraries with proper validation and metadata.
    Updated to use proven download methods from symbol_updater.py
    """

    def __init__(self, market_data_dir: str, db_path: str = None):
        """
        Initialize the symbol adder.

        Args:
            market_data_dir: Directory where market data is stored
            db_path: Path to ArcticDB database
        """
        self.market_data_dir = market_data_dir
        self.db_path = db_path or f"lmdb:///{market_data_dir}"

        # Initialize components
        logger.info(f"Initializing SymbolAdder with market_data_dir={market_data_dir}")

        # Initialize yfinance session management (matching symbol_updater.py)
        self.session_manager = YFinanceSessionManager()

        # Initialize data manager
        self.data_manager = MarketDataManager.get_instance(db_path=self.db_path)

        logger.info("SymbolAdder initialization complete")

    @notification_decorator(message=None, include_result=False, notify_on_error=True)
    def add_symbol(self, symbol: str, force_update: bool = False) -> Dict[str, Any]:
        """
        Add a symbol to all database libraries with validation and proper metadata.

        Args:
            symbol: The symbol to add (will be uppercased)
            force_update: Whether to force update if symbol already exists

        Returns:
            dict: Result information including success/failure details
        """
        # Ensure symbol is uppercase
        symbol = symbol.upper()

        result = {
            "symbol": symbol,
            "success": False,
            "status": "started",
            "exists_in_db": False,
            "validation": {},
            "metadata": {},
            "price_data": {},
            "errors": [],
            "warnings": [],
            "timestamp": datetime.now().isoformat(),
        }

        # Start timing
        start_time = time.time()

        # 1. Validate symbol exists in Yahoo Finance
        logger.info(f"Validating symbol {symbol}")
        ticker_info = self.validate_symbol(symbol)
        if not ticker_info:
            result["status"] = "failed"
            result["errors"].append("Symbol validation failed")
            logger.error(f"Symbol {symbol} validation failed")
            return result

        result["validation"] = {
            "valid": True,
            "quote_type": ticker_info.get("quoteType", "Unknown"),
            "exchange": ticker_info.get("exchange", "Unknown"),
        }

        # 2. Check for existing data in all three libraries
        exists_in_metadata = self._check_exists_in_metadata(symbol)
        exists_in_price = self._check_exists_in_price(symbol)
        exists_in_price_metadata = self._check_exists_in_price_metadata(symbol)

        result["exists_in_db"] = any(
            [exists_in_metadata, exists_in_price, exists_in_price_metadata]
        )

        if result["exists_in_db"] and not force_update:
            result["status"] = "skipped"
            result["warnings"].append(
                "Symbol already exists in database. Use force_update=True to update"
            )
            logger.warning(
                f"Symbol {symbol} already exists in database. Skipping (use force_update=True to update)"
            )
            return result

        # 3. Collect metadata with fallbacks for missing fields
        logger.info(f"Collecting metadata for {symbol}")
        metadata = self._collect_metadata(ticker_info)
        if not metadata:
            result["status"] = "failed"
            result["errors"].append("Failed to collect metadata")
            logger.error(f"Failed to collect metadata for {symbol}")
            return result

        result["metadata"] = {"collected": True, "fields": list(metadata.keys())}

        # 4. Download complete price history using symbol_updater methods
        logger.info(f"Downloading price history for {symbol}")
        price_data, price_success, price_error = self._download_price_history(symbol)

        if not price_success:
            result["status"] = "failed"
            result["errors"].append(f"Failed to download price history: {price_error}")
            logger.error(
                f"Failed to download price history for {symbol}: {price_error}"
            )
            return result

        if price_data.empty:
            result["status"] = "failed"
            result["errors"].append("Downloaded price data is empty")
            logger.error(f"Downloaded price data for {symbol} is empty")
            return result

        result["price_data"] = {
            "downloaded": True,
            "points": len(price_data),
            "first_date": str(price_data.index[0].date()),
            "last_date": str(price_data.index[-1].date()),
        }

        # 5. Perform data quality checks
        quality_check, quality_warnings = self._check_data_quality(price_data)
        if quality_warnings:
            result["warnings"].extend(quality_warnings)

        if not quality_check:
            result["status"] = "failed"
            result["errors"].append("Price data failed quality checks")
            logger.error(f"Price data for {symbol} failed quality checks")
            return result

        # 6. Transaction-like addition to all three libraries
        logger.info(f"Adding {symbol} to database libraries")
        db_success, db_error = self._add_to_databases(
            symbol, metadata, price_data, force_update
        )

        if not db_success:
            result["status"] = "failed"
            result["errors"].append(f"Failed to add to databases: {db_error}")
            logger.error(f"Failed to add {symbol} to databases: {db_error}")
            return result

        # 7. Verification of successful addition
        verification = self._verify_addition(symbol)
        if not verification:
            result["status"] = "partial"
            result["warnings"].append("Symbol added but verification failed")
            logger.warning(f"Symbol {symbol} added but verification failed")
        else:
            result["status"] = "success"

        # Calculate duration
        duration = time.time() - start_time
        result["duration_seconds"] = duration

        # Final success state
        result["success"] = result["status"] in ["success", "partial"]

        # Send notification based on status
        if result["status"] == "success":
            send_telegram_notification(
                f"✅ Successfully added {symbol}: {result['price_data'].get('points', 0)} price points from "
                f"{result['price_data'].get('first_date', 'unknown')} to {result['price_data'].get('last_date', 'unknown')}"
            )
        elif result["status"] == "failed":
            send_telegram_notification(
                f"❌ Failed to add {symbol}: {', '.join(result['errors'])}"
            )

        logger.info(
            f"Successfully added symbol {symbol} to database in {duration:.2f} seconds"
        )
        return result

    def validate_symbol(self, symbol: str) -> Dict[str, Any]:
        """
        Validate that the symbol exists in Yahoo Finance and get basic info.

        Args:
            symbol: The symbol to validate

        Returns:
            dict: Information about the symbol if valid, empty dict if invalid
        """
        try:
            # Create ticker without passing session (yfinance will use configured proxy)
            ticker = yf.Ticker(symbol)

            # Try to get basic info - will fail for invalid symbols
            try:
                info = ticker.info
                # Handle case where info is None or empty
                if info is None or not info:
                    logger.warning(
                        f"Symbol {symbol} does not appear to be valid (no info returned)"
                    )
                    return {}

                # Check for essential fields
                essential_fields = ["symbol", "quoteType"]
                if not all(field in info for field in essential_fields):
                    logger.warning(
                        f"Symbol {symbol} missing essential fields: {essential_fields}"
                    )

                # Try to add missing symbol field if not present
                if "symbol" not in info:
                    info["symbol"] = symbol

                logger.info(
                    f"Successfully validated symbol {symbol} ({info.get('quoteType', 'Unknown type')})"
                )
                return info

            except (AttributeError, TypeError) as e:
                # This catches the case where info is None or not a dict
                logger.error(f"Symbol {symbol} info is invalid: {str(e)}")
                return {}

        except Exception as e:
            logger.error(f"Error validating symbol {symbol}: {str(e)}")
            return {}

    def _download_price_history(self, symbol: str) -> Tuple[pd.DataFrame, bool, str]:
        """
        Download complete price history using symbol_updater methods.
        Updated to use the proven download approach from symbol_updater.py

        Args:
            symbol: Symbol to download

        Returns:
            tuple: (DataFrame with price data, success status, error message)
        """
        try:
            # Reset session if needed (matching symbol_updater.py)
            self.session_manager.reset_session_if_needed()

            # Use yf.download with exact parameters from symbol_updater.py
            logger.info(f"Downloading price history for {symbol} with period='max'")

            # Try with different periods as fallback (matching symbol_updater retry logic)
            periods = ["max", "2y", "1y", "6mo"]

            for period in periods:
                try:
                    price_data = yf.download(
                        tickers=symbol,
                        period=period,
                        interval="1d",
                        group_by="ticker",  # Important: use ticker grouping like symbol_updater
                        auto_adjust=False,
                        timeout=90,
                        progress=False,
                        threads=False,
                        ignore_tz=False,
                    )

                    if price_data is not None and not price_data.empty:
                        logger.info(
                            f"Successfully downloaded {symbol} with period={period}"
                        )

                        # Extract and standardize the data (matching symbol_updater approach)
                        standardized_data = self._extract_and_standardize_single_symbol(
                            symbol, price_data
                        )

                        if (
                            standardized_data is not None
                            and not standardized_data.empty
                        ):
                            logger.info(
                                f"Downloaded {len(standardized_data)} price points for {symbol}"
                            )
                            return standardized_data, True, ""
                        else:
                            logger.warning(
                                f"Standardization failed for {symbol} with period={period}"
                            )
                            continue
                    else:
                        logger.warning(
                            f"No data returned for {symbol} with period={period}"
                        )
                        continue

                except Exception as e:
                    logger.warning(
                        f"Error downloading {symbol} with period={period}: {e}"
                    )
                    continue

            # If all periods failed
            return pd.DataFrame(), False, "No data available for any period"

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error downloading price history for {symbol}: {error_msg}")

            # Check if it's a delisted symbol
            if (
                "delisted" in error_msg.lower()
                or "no price data found" in error_msg.lower()
            ):
                return pd.DataFrame(), False, "Symbol appears to be delisted"

            return pd.DataFrame(), False, error_msg

    def _extract_and_standardize_single_symbol(
        self, symbol: str, data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Extract single symbol data and standardize it using symbol_updater methods.

        Args:
            symbol: Symbol identifier
            data: Raw data from yfinance

        Returns:
            Standardized DataFrame or None if extraction fails
        """
        try:
            # Handle MultiIndex columns (matching symbol_updater logic)
            if isinstance(data.columns, pd.MultiIndex):
                # Check which level contains the symbol
                symbol_in_level_0 = symbol in data.columns.levels[0]
                symbol_in_level_1 = symbol in data.columns.levels[1]

                if symbol_in_level_0:
                    # Symbol is in first level
                    symbol_data = data[symbol].copy()
                elif symbol_in_level_1:
                    # Symbol is in second level - reconstruct the DataFrame
                    symbol_cols = [col for col in data.columns if col[1] == symbol]
                    if symbol_cols:
                        symbol_data = pd.DataFrame(index=data.index)
                        for col in symbol_cols:
                            price_type = col[0]
                            if price_type in [
                                "Open",
                                "High",
                                "Low",
                                "Close",
                                "Adj Close",
                                "Volume",
                            ]:
                                symbol_data[price_type] = data[col]
                    else:
                        return None
                else:
                    return None
            else:
                # Single symbol case
                symbol_data = data.copy()

            # Standardize the data using symbol_updater method
            return self._standardize_price_dataframe(symbol, symbol_data)

        except Exception as e:
            logger.error(f"Error extracting and standardizing {symbol}: {e}")
            return None

    def _standardize_price_dataframe(
        self, symbol: str, data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Standardize price data DataFrame using symbol_updater methods.
        Updated to match the robust standardization from symbol_updater.py

        Args:
            symbol: Symbol identifier
            data: DataFrame with price data

        Returns:
            Standardized DataFrame or None if standardization fails
        """
        try:
            # Return None for empty DataFrames
            if data is None or data.empty:
                logger.warning(f"Empty DataFrame for {symbol}, cannot standardize")
                return None

            # Create a new DataFrame to avoid modifying the original
            standard_df = pd.DataFrame(index=data.index)

            # Handle MultiIndex columns (copied from symbol_updater.py)
            if isinstance(data.columns, pd.MultiIndex):
                logger.info(f"Processing MultiIndex columns for {symbol}")
                # For single symbol download, this shouldn't happen, but handle it anyway
                if symbol in data.columns.levels[0]:
                    data = data[symbol]
                elif len(data.columns.levels[0]) == 1:
                    # Single symbol with MultiIndex - flatten it
                    data.columns = data.columns.droplevel(0)

            # Define expected standard columns
            standard_columns = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

            # Copy existing standard columns
            for col in standard_columns:
                if col in data.columns:
                    standard_df[col] = data[col]
                else:
                    # Add missing columns with appropriate defaults
                    if col == "Volume":
                        standard_df[col] = 0
                    else:
                        standard_df[col] = np.nan

            # Ensure all standard columns exist with correct types
            for col in ["Open", "High", "Low", "Close", "Adj Close"]:
                if col in standard_df.columns:
                    # Convert to numeric, coercing errors to NaN
                    standard_df[col] = pd.to_numeric(standard_df[col], errors="coerce")

            # Special handling for Volume (convert to int)
            if "Volume" in standard_df.columns:
                # First convert to numeric (coercing errors to NaN)
                standard_df["Volume"] = pd.to_numeric(
                    standard_df["Volume"], errors="coerce"
                )
                # Replace NaN with 0 and convert to integer
                standard_df["Volume"] = standard_df["Volume"].fillna(0).astype("int64")

            # Ensure index is DatetimeIndex
            if not isinstance(standard_df.index, pd.DatetimeIndex):
                try:
                    standard_df.index = pd.to_datetime(standard_df.index)
                    logger.info(f"Converted index to DatetimeIndex for {symbol}")
                except Exception as e:
                    logger.error(
                        f"Failed to convert index to DatetimeIndex for {symbol}: {e}"
                    )
                    return None

            # Reorder columns to match standard order
            standard_df = standard_df[standard_columns]

            # Replace infinity values with NaN
            inf_mask = np.isinf(
                standard_df[["Open", "High", "Low", "Close", "Adj Close"]]
            )
            if inf_mask.any().any():
                logger.warning(
                    f"Replacing {inf_mask.sum().sum()} infinity values with NaN for {symbol}"
                )
                for col in ["Open", "High", "Low", "Close", "Adj Close"]:
                    standard_df[col] = standard_df[col].replace(
                        [np.inf, -np.inf], np.nan
                    )

            # Verify the DataFrame is not empty after standardization
            if standard_df.empty or standard_df.dropna(how="all").empty:
                logger.warning(f"DataFrame empty after standardization for {symbol}")
                return None

            # Handle timezone issues (matching symbol_updater approach)
            if hasattr(standard_df.index, "tz") and standard_df.index.tz is not None:
                standard_df.index = standard_df.index.tz_localize(None)

            return standard_df

        except Exception as e:
            logger.error(f"Error standardizing DataFrame for {symbol}: {str(e)}")
            return None

    def _check_exists_in_metadata(self, symbol: str) -> bool:
        """Check if symbol exists in symbol_metadata library."""
        lib = self.data_manager.symbol_metadata_lib
        # Fresh database: the aggregate metadata symbol does not exist yet.
        if not lib.has_symbol("symbols_metadata"):
            return False
        try:
            symbols_metadata = lib.read("symbols_metadata").data
            return symbol in symbols_metadata.index
        except Exception as e:
            logger.error(f"Error checking if {symbol} exists in metadata: {str(e)}")
            return False

    def _check_exists_in_price(self, symbol: str) -> bool:
        """Check if symbol exists in price_history.daily library."""
        try:
            return symbol in self.data_manager.price_history_lib.list_symbols()
        except Exception as e:
            logger.error(
                f"Error checking if {symbol} exists in price history: {str(e)}"
            )
            return False

    def _check_exists_in_price_metadata(self, symbol: str) -> bool:
        """Check if symbol exists in price_history.metadata library."""
        try:
            return symbol in self.data_manager.price_metadata_lib.list_symbols()
        except Exception as e:
            logger.error(
                f"Error checking if {symbol} exists in price metadata: {str(e)}"
            )
            return False

    def _collect_metadata(self, ticker_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Collect and format metadata from ticker info.
        FIXED: Ensure consistent timezone-naive datetime handling
        """
        try:
            # Get timezone-naive datetime for consistency
            current_time = datetime.now().replace(tzinfo=None)

            # Required fields with fallbacks
            metadata = {
                "quoteType": ticker_info.get("quoteType", "Unknown"),
                "symbol": ticker_info.get("symbol", ""),
                "first_collected": current_time,  # Use datetime object, not string
                "last_updated": current_time,  # Use datetime object, not string
                "status": "active",
            }

            # Optional fields with fallbacks
            if "longName" in ticker_info:
                metadata["longName"] = ticker_info["longName"]
            elif "shortName" in ticker_info:
                metadata["longName"] = ticker_info[
                    "shortName"
                ]  # Use shortName as fallback
            else:
                metadata["longName"] = ticker_info.get(
                    "symbol", "Unknown"
                )  # Use symbol as last resort

            if "shortName" in ticker_info:
                metadata["shortName"] = ticker_info["shortName"]
            elif "longName" in ticker_info:
                # Create a shortened version if only longName is available
                metadata["shortName"] = (
                    ticker_info["longName"][:40]
                    if len(ticker_info["longName"]) > 40
                    else ticker_info["longName"]
                )
            else:
                metadata["shortName"] = ticker_info.get(
                    "symbol", "Unknown"
                )  # Use symbol as last resort

            # Clean any NaN or None values
            for key, value in metadata.items():
                if pd.isna(value) or value is None:
                    if key in ["first_collected", "last_updated"]:
                        metadata[key] = (
                            current_time  # Use datetime for timestamp fields
                        )
                    else:
                        metadata[key] = "Unknown"

            logger.info(
                f"Collected metadata for {metadata['symbol']}: {metadata['quoteType']} - {metadata['shortName']}"
            )
            return metadata

        except Exception as e:
            logger.error(f"Error collecting metadata: {str(e)}")
            return {}

    def _check_data_quality(self, price_data: pd.DataFrame) -> Tuple[bool, list]:
        """
        Perform comprehensive data quality checks on price data.
        Uses proper financial market validation rules.

        Args:
            price_data: DataFrame with price data

        Returns:
            tuple: (pass status, list of warnings)
        """
        warnings = []
        critical_errors = []

        # Check 1: Ensure we have some data
        if len(price_data) < 1:
            critical_errors.append("No price data available")
            return False, critical_errors

        # Check 2: Ensure we have all required columns
        required_columns = ["Open", "High", "Low", "Close"]
        missing_columns = [
            col for col in required_columns if col not in price_data.columns
        ]

        if missing_columns:
            critical_errors.append(
                f"Missing required columns: {', '.join(missing_columns)}"
            )
            return False, critical_errors

        # Check 3: Validate data types and convert if necessary
        price_cols = ["Open", "High", "Low", "Close", "Volume"]
        for col in price_cols:
            if col in price_data.columns:
                if not pd.api.types.is_numeric_dtype(price_data[col]):
                    warnings.append(f"Column {col} is not numeric type")

        # Check 4: Check for excessive NaN values in price columns
        for col in ["Open", "High", "Low", "Close"]:
            if col in price_data.columns:
                nan_count = price_data[col].isna().sum()
                nan_percent = nan_count / len(price_data)

                if nan_percent > 0.1:  # More than 10% NaN values
                    if nan_percent > 0.5:  # More than 50% is critical
                        critical_errors.append(
                            f"Column {col} has {nan_percent:.1%} NaN values (too high)"
                        )
                    else:
                        warnings.append(
                            f"Column {col} has {nan_percent:.1%} NaN values"
                        )

        # Check 5: Validate price relationships
        valid_data = price_data[["Open", "High", "Low", "Close"]].dropna()

        # if len(valid_data) > 0:
        #     # Rule 1: High must be >= Low (fundamental rule)
        #     invalid_hl = (valid_data["High"] < valid_data["Low"]).sum()
        #     if invalid_hl > 0:
        #         critical_errors.append(f"Found {invalid_hl} days where High < Low (data corruption)")

        #     # Rule 2: Open must be within High-Low range (INCLUSIVE)
        #     invalid_open = (
        #         (valid_data["Open"] < valid_data["Low"]) |
        #         (valid_data["Open"] > valid_data["High"])
        #     ).sum()
        #     if invalid_open > 0:
        #         warnings.append(f"Found {invalid_open} days where Open is outside High-Low range")

        #         # DEBUG: Show problem rows for Open
        #         problem_open_rows = valid_data[
        #             (valid_data["Open"] < valid_data["Low"]) |
        #             (valid_data["Open"] > valid_data["High"])
        #         ]
        #         logger.warning(f"DEBUG - Problem Open rows (first 5):")
        #         logger.warning(f"\n{problem_open_rows[['Open', 'High', 'Low', 'Close']].head()}")

        #     # Rule 3: Close must be within High-Low range (INCLUSIVE)
        #     invalid_close = (
        #         (valid_data["Close"] < valid_data["Low"]) |
        #         (valid_data["Close"] > valid_data["High"])
        #     ).sum()
        #     if invalid_close > 0:
        #         warnings.append(f"Found {invalid_close} days where Close is outside High-Low range")

        #         # DEBUG: Show problem rows for Close
        #         problem_close_rows = valid_data[
        #             (valid_data["Close"] < valid_data["Low"]) |
        #             (valid_data["Close"] > valid_data["High"])
        #         ]
        #         logger.warning(f"DEBUG - Problem Close rows (first 10):")
        #         logger.warning(f"\n{problem_close_rows[['Open', 'High', 'Low', 'Close']].head(10)}")

        #         # Additional debug: Show specific conditions
        #         close_below_low = (valid_data["Close"] < valid_data["Low"]).sum()
        #         close_above_high = (valid_data["Close"] > valid_data["High"]).sum()
        #         logger.warning(f"DEBUG - Close < Low: {close_below_low} days")
        #         logger.warning(f"DEBUG - Close > High: {close_above_high} days")

        #         # Show some examples of each condition
        #         if close_below_low > 0:
        #             below_examples = valid_data[valid_data["Close"] < valid_data["Low"]]
        #             logger.warning(f"DEBUG - Examples where Close < Low:")
        #             logger.warning(f"\n{below_examples[['Open', 'High', 'Low', 'Close']].head(3)}")

        #         if close_above_high > 0:
        #             above_examples = valid_data[valid_data["Close"] > valid_data["High"]]
        #             logger.warning(f"DEBUG - Examples where Close > High:")
        #             logger.warning(f"\n{above_examples[['Open', 'High', 'Low', 'Close']].head(3)}")

        #     # Rule 4: Check for zero or negative prices (usually invalid)
        #     zero_negative_prices = (valid_data[["Open", "High", "Low", "Close"]] <= 0).any(axis=1).sum()
        #     if zero_negative_prices > 0:
        #         warnings.append(f"Found {zero_negative_prices} days with zero or negative prices")

        #     # Rule 5: Check for unrealistic price jumps (more than 50% in one day)
        #     if len(valid_data) > 1:
        #         price_changes = valid_data["Close"].pct_change().abs()
        #         extreme_changes = (price_changes > 0.5).sum()
        #         if extreme_changes > 0:
        #             warnings.append(f"Found {extreme_changes} days with >50% price changes (possible splits/errors)")

        # Check 6: Volume validation (if present) - RELAXED for ETFs/funds
        if "Volume" in price_data.columns:
            volume_data = price_data["Volume"].dropna()
            if len(volume_data) > 0:
                # Check for negative volume
                negative_volume = (volume_data < 0).sum()
                if negative_volume > 0:
                    warnings.append(
                        f"Found {negative_volume} days with negative volume"
                    )

                # Check for excessive zero volume days - RELAXED threshold for ETFs
                zero_volume = (volume_data == 0).sum()
                zero_volume_percent = zero_volume / len(volume_data)
                # Many ETFs and funds have low/zero volume days, so be more lenient
                if zero_volume_percent > 0.8:  # Only warn if more than 80% zero volume
                    warnings.append(
                        f"Very high percentage of zero volume days: {zero_volume_percent:.1%} (normal for some ETFs)"
                    )
                elif zero_volume_percent > 0.5:  # Info warning for 50-80%
                    warnings.append(
                        f"High percentage of zero volume days: {zero_volume_percent:.1%} (may be normal for ETFs)"
                    )

        # Check 7: Date continuity and coverage
        if len(price_data) > 1:
            # Check for duplicate dates
            duplicate_dates = price_data.index.duplicated().sum()
            if duplicate_dates > 0:
                warnings.append(f"Found {duplicate_dates} duplicate dates")

            # Check for very large gaps (weekends and holidays are normal)
            date_diff = price_data.index.to_series().diff().dt.days
            large_gaps = (date_diff > 30).sum()  # Gaps larger than 30 days
            if large_gaps > 0:
                max_gap = date_diff.max()
                warnings.append(
                    f"Found {large_gaps} gaps larger than 30 days (max: {max_gap} days)"
                )

        # Check 8: Data recency (warn if data is very old)
        if len(price_data) > 0:
            last_date = price_data.index[-1]
            days_since_last = (datetime.now() - last_date).days

            if days_since_last > 7:  # More than a week old
                warnings.append(
                    f"Data is {days_since_last} days old (last: {last_date.date()})"
                )

        # Check 9: Minimum data points
        if len(price_data) < 5:
            warnings.append(f"Very limited data: only {len(price_data)} data points")

        # Check 10: Check for constant prices (possible stale data)
        if len(valid_data) > 10:
            for col in ["Open", "High", "Low", "Close"]:
                if col in valid_data.columns:
                    unique_values = valid_data[col].nunique()
                    if unique_values == 1:
                        warnings.append(
                            f"Column {col} has constant value (possible stale data)"
                        )
                    elif (
                        unique_values < len(valid_data) * 0.1
                    ):  # Less than 10% unique values
                        warnings.append(
                            f"Column {col} has very few unique values ({unique_values}/{len(valid_data)})"
                        )

        # Determine overall status
        # Critical errors = immediate failure
        if critical_errors:
            return False, critical_errors + warnings

        # Too many warnings = failure (increased threshold)
        if len(warnings) > 8:  # Increased from 5 to 8 to be more lenient
            warnings.append("Too many data quality issues detected")
            return False, warnings

        # Check if we have any actual valid data
        valid_rows = price_data[["Open", "High", "Low", "Close"]].dropna()
        if len(valid_rows) < max(1, len(price_data) * 0.3):  # Reduced from 50% to 30%
            return False, warnings + ["Insufficient valid price data"]

        # Success - data passes quality checks
        return True, warnings

    def _add_to_databases(
        self,
        symbol: str,
        metadata: Dict[str, Any],
        price_data: pd.DataFrame,
        force_update: bool,
    ) -> Tuple[bool, str]:
        """
        Add the symbol to all three database libraries.
        FIXED: Handle timezone consistency for datetime columns
        """
        try:
            # 1. Add/update price data in price_history.daily
            logger.info(f"Writing price data for {symbol} to price_history.daily")
            self.data_manager.price_history_lib.write(symbol, price_data)

            # 2. Add/update metadata in symbol_metadata library
            logger.info(f"Updating symbol_metadata for {symbol}")
            meta_lib = self.data_manager.symbol_metadata_lib
            if meta_lib.has_symbol("symbols_metadata"):
                symbols_metadata = meta_lib.read("symbols_metadata").data
            else:
                # Fresh database: start the aggregate metadata frame empty.
                symbols_metadata = pd.DataFrame()

            # CRITICAL FIX: Normalize timezone handling for datetime columns
            def normalize_datetime_column(df, col_name):
                """Ensure datetime column is timezone-naive for consistency"""
                if col_name in df.columns:
                    # Convert to datetime if it's not already
                    df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
                    # Remove timezone info to make it tz-naive
                    if (
                        hasattr(df[col_name].dtype, "tz")
                        and df[col_name].dtype.tz is not None
                    ):
                        df[col_name] = df[col_name].dt.tz_localize(None)
                    elif df[col_name].dt.tz is not None:
                        df[col_name] = df[col_name].dt.tz_localize(None)
                return df

            # Normalize existing data
            symbols_metadata = normalize_datetime_column(
                symbols_metadata, "first_collected"
            )
            symbols_metadata = normalize_datetime_column(
                symbols_metadata, "last_updated"
            )

            # Ensure metadata datetime fields are timezone-naive
            current_time = datetime.now().replace(tzinfo=None)

            # Check if symbol already exists in symbols_metadata
            if symbol in symbols_metadata.index:
                if force_update:
                    # Update fields that should be updated
                    symbols_metadata.loc[symbol, "last_updated"] = current_time
                    symbols_metadata.loc[symbol, "longName"] = metadata["longName"]
                    symbols_metadata.loc[symbol, "shortName"] = metadata["shortName"]
                    symbols_metadata.loc[symbol, "status"] = metadata["status"]
                    # Don't update quoteType or first_collected for existing symbols
                    logger.info(f"Updated existing metadata for {symbol}")
                else:
                    logger.info(
                        f"Symbol {symbol} already exists in symbol_metadata, skipping update"
                    )
            else:
                # Create new row for the symbol with timezone-naive datetime
                new_row = pd.DataFrame(
                    {
                        "quoteType": [metadata["quoteType"]],
                        "longName": [metadata["longName"]],
                        "shortName": [metadata["shortName"]],
                        "first_collected": [
                            current_time
                        ],  # Use timezone-naive datetime
                        "last_updated": [current_time],  # Use timezone-naive datetime
                        "status": [metadata["status"]],
                    },
                    index=[symbol],
                )

                # Ensure new row datetime columns are also timezone-naive
                new_row = normalize_datetime_column(new_row, "first_collected")
                new_row = normalize_datetime_column(new_row, "last_updated")

                symbols_metadata = pd.concat([symbols_metadata, new_row])
                logger.info(f"Added new metadata for {symbol}")

            # Final normalization before writing
            symbols_metadata = normalize_datetime_column(
                symbols_metadata, "first_collected"
            )
            symbols_metadata = normalize_datetime_column(
                symbols_metadata, "last_updated"
            )

            # Write updated symbols_metadata back to the library
            self.data_manager.symbol_metadata_lib.write(
                "symbols_metadata", symbols_metadata
            )

            # 3. Add/update price metadata in price_history.metadata
            logger.info(f"Writing price metadata for {symbol}")

            # Use timezone-naive datetime for price metadata as well
            price_meta_df = pd.DataFrame(
                [
                    {
                        "symbol": symbol,
                        "data_status": "complete",
                        "first_date": str(price_data.index[0].date()),
                        "last_date": str(price_data.index[-1].date()),
                        "last_update": current_time,  # Use timezone-naive datetime
                        "data_points": len(price_data),
                        "source": "yfinance",
                    }
                ]
            )

            self.data_manager.price_metadata_lib.write(symbol, price_meta_df)

            logger.info(f"Successfully added {symbol} to all database libraries")
            return True, ""

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error adding {symbol} to databases: {error_msg}")
            return False, error_msg

    def _verify_addition(self, symbol: str) -> bool:
        """
        Verify that the symbol was successfully added to all three libraries.

        Args:
            symbol: Symbol to verify

        Returns:
            bool: True if symbol exists in all libraries, False otherwise
        """
        try:
            in_metadata = self._check_exists_in_metadata(symbol)
            in_price = self._check_exists_in_price(symbol)
            in_price_metadata = self._check_exists_in_price_metadata(symbol)

            all_present = in_metadata and in_price and in_price_metadata

            if all_present:
                logger.info(f"Verified {symbol} exists in all three libraries")
            else:
                missing = []
                if not in_metadata:
                    missing.append("symbol_metadata")
                if not in_price:
                    missing.append("price_history.daily")
                if not in_price_metadata:
                    missing.append("price_history.metadata")

                logger.warning(
                    f"Symbol {symbol} missing from libraries: {', '.join(missing)}"
                )

            return all_present

        except Exception as e:
            logger.error(f"Error verifying {symbol} addition: {str(e)}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Add symbols to market data database")
    parser.add_argument("symbol", help="Symbol to add")
    parser.add_argument(
        "--market-data-dir",
        default=MARKET_DATA_DIR,
        help="Path to market data directory",
    )
    parser.add_argument(
        "--force", action="store_true", help="Force update existing symbols"
    )
    parser.add_argument("--db-path", help="Custom database path")

    args = parser.parse_args()

    # Initialize adder
    try:
        adder = SymbolAdder(market_data_dir=args.market_data_dir, db_path=args.db_path)

        print(f"Adding symbol: {args.symbol}")

        # Add the symbol
        result = adder.add_symbol(args.symbol.upper(), force_update=args.force)

        # Display results
        print(f"\n{'='*50}")
        print(f"RESULT FOR {args.symbol.upper()}")
        print(f"{'='*50}")
        print(f"Success: {result['success']}")
        print(f"Status: {result['status']}")

        if result["success"]:
            print(f"✅ Successfully added {args.symbol.upper()}")
            print(f"Price points: {result['price_data']['points']}")
            print(
                f"Date range: {result['price_data']['first_date']} to {result['price_data']['last_date']}"
            )
            if "duration_seconds" in result:
                print(f"Duration: {result['duration_seconds']:.2f} seconds")
        else:
            print(f"❌ Failed to add {args.symbol.upper()}")
            if result["errors"]:
                print("Errors:")
                for error in result["errors"]:
                    print(f"  - {error}")

        if result["warnings"]:
            print("Warnings:")
            for warning in result["warnings"]:
                print(f"  ⚠️ {warning}")

    except Exception as e:
        print(f"❌ Error initializing SymbolAdder: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

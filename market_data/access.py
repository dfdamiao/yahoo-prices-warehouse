# market_data_access.py
import os
import pandas as pd
import logging
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Union, Optional, Tuple, Any
from arcticdb import Arctic
from fuzzywuzzy import fuzz
import xlsxwriter
import re

# Import centralized configuration
from market_data.settings import DB_PATH

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("market_data_access")

# Suppress ArcticDB warnings
logging.getLogger("arcticdb").setLevel(logging.ERROR)


class MarketDataAccess:
    """
    Comprehensive access to market data stored in ArcticDB, with advanced
    search, filtering, and export capabilities.
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize access to market data in ArcticDB.

        Args:
            db_path: Path to the ArcticDB database (defaults to config.DB_PATH)
        """
        self.db_path = db_path or DB_PATH
        self.arctic = None
        self.symbol_metadata_lib = None
        self.price_history_lib = None
        self.price_metadata_lib = None
        self._connected = False

        # Connect to the database
        self._connect()

        # Cache for metadata
        self._symbols_metadata_cache = None
        self._ticker_classification_cache = None

    def _connect(self) -> None:
        """Connect to ArcticDB and initialize libraries."""
        try:
            logger.info(f"Connecting to ArcticDB at {self.db_path}")
            self.arctic = Arctic(self.db_path)

            # Get library references
            self.symbol_metadata_lib = self.arctic.get_library("symbol_metadata")
            self.price_history_lib = self.arctic.get_library("price_history.daily")
            self.price_metadata_lib = self.arctic.get_library("price_history.metadata")

            logger.info("Successfully connected to market data libraries")

            self._connected = True
            self._warm_symbol_cache()

        except Exception as e:
            logger.error(f"Failed to connect to ArcticDB: {str(e)}")
            self._connected = False
            raise

    def _warm_symbol_cache(self, enable_warming: bool = True) -> None:
        """
        Warm up ArcticDB symbol list cache for better performance.

        Args:
            enable_warming: Whether to perform cache warming (default True)
        """
        if not enable_warming:
            return

        try:
            logger.info("Warming up symbol list cache...")

            # Warm up each library's symbol cache
            price_symbols = len(self.price_history_lib.list_symbols())
            metadata_symbols = len(self.symbol_metadata_lib.list_symbols())
            price_meta_symbols = len(self.price_metadata_lib.list_symbols())

            logger.info(
                f"Symbol cache warmed: {price_symbols} price symbols, "
                f"{metadata_symbols} metadata symbols, {price_meta_symbols} price metadata symbols"
            )

        except Exception as e:
            logger.warning(
                f"Symbol cache warming failed: {str(e)} - continuing without cache"
            )

    def _parse_period(self, period: str) -> Tuple[datetime, datetime]:
        """
        Parse a period string into start and end dates.

        Args:
            period: Period string like '1d', '1mo', '1y', 'ytd', 'max'

        Returns:
            Tuple of (start_date, end_date)
        """
        end_date = datetime.now()

        if period == "max":
            # Use a very old date for max
            start_date = datetime(1900, 1, 1)
        elif period == "ytd":
            # Year to date
            start_date = datetime(end_date.year, 1, 1)
        else:
            # Parse period like '1d', '1mo', '1y', etc.
            match = re.match(r"(\d+)([dmy].*)", period)
            if not match:
                raise ValueError(f"Invalid period format: {period}")

            amount = int(match.group(1))
            unit = match.group(2)

            if unit.startswith("d"):
                start_date = end_date - timedelta(days=amount)
            elif unit.startswith("mo"):
                # Approximate months as 30 days
                start_date = end_date - timedelta(days=amount * 30)
            elif unit.startswith("y"):
                # Approximate years as 365 days
                start_date = end_date - timedelta(days=amount * 365)
            else:
                raise ValueError(f"Unknown period unit: {unit}")

        return start_date, end_date

    def _filter_date_range(
        self,
        data: pd.DataFrame,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pd.DataFrame:
        """
        Filter DataFrame to specified date range.

        Args:
            data: DataFrame with DatetimeIndex
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            Filtered DataFrame
        """
        if not isinstance(data.index, pd.DatetimeIndex):
            logger.warning("Data does not have DatetimeIndex, cannot filter by date")
            return data

        filtered_data = data

        if start_date:
            filtered_data = filtered_data[
                filtered_data.index >= pd.Timestamp(start_date)
            ]

        if end_date:
            filtered_data = filtered_data[filtered_data.index <= pd.Timestamp(end_date)]

        return filtered_data

    def get_symbols_metadata(self, refresh: bool = False) -> pd.DataFrame:
        """
        Get metadata for all symbols, with caching for performance.

        Args:
            refresh: Force refresh of cached metadata

        Returns:
            DataFrame containing symbol metadata
        """
        if self._symbols_metadata_cache is None or refresh:
            try:
                logger.info("Loading symbols metadata from database")
                self._symbols_metadata_cache = self.symbol_metadata_lib.read(
                    "symbols_metadata"
                ).data
                logger.info(
                    f"Loaded metadata for {len(self._symbols_metadata_cache)} symbols"
                )
            except Exception as e:
                logger.error(f"Error loading symbols metadata: {str(e)}")
                return pd.DataFrame()

        return self._symbols_metadata_cache

    def get_symbol_metadata(self, symbol: str) -> Optional[pd.Series]:
        """
        Get metadata for a specific symbol.

        Args:
            symbol: Symbol identifier

        Returns:
            Series with symbol metadata or None if not found
        """
        metadata = self.get_symbols_metadata()

        if metadata.empty:
            return None

        if symbol in metadata.index:
            return metadata.loc[symbol]

        return None

    # ------------------------------------------------------------------
    # Ticker classification (a metadata pipeline)
    # Source: ArcticDB symbol_metadata_lib["ticker_classification"]
    # Refreshed every ~6 months by a metadata-integration job
    # ------------------------------------------------------------------

    def get_ticker_classification(
        self, refresh: bool = False
    ) -> pd.DataFrame:
        """Return the full ticker classification table (cached).

        Columns include: asset_class, sector, invested_region, style,
        domicile, isin, quoteType, currency, exchange, fundFamily,
        longName, last_bar_date, suspected_discontinued, ...

        See ``the metadata schema docs`` for the full schema.
        """
        if self._ticker_classification_cache is None or refresh:
            try:
                logger.info("Loading ticker classification from ArcticDB")
                self._ticker_classification_cache = (
                    self.symbol_metadata_lib.read("ticker_classification").data  # type: ignore[union-attr]
                )
                logger.info(
                    f"Loaded classification for "
                    f"{len(self._ticker_classification_cache)} tickers"  # type: ignore[arg-type]
                )
            except Exception as e:
                logger.error(
                    f"Error loading ticker classification: {e}. "
                    f"Run a metadata-integration job "
                    f"to populate it."
                )
                return pd.DataFrame()
        return self._ticker_classification_cache  # type: ignore[return-value]

    def get_ticker_classification_for(
        self, symbol: str
    ) -> Optional[pd.Series]:
        """Return the classification row for a single ticker, or None."""
        df = self.get_ticker_classification()
        if df.empty or symbol not in df.index:
            return None
        return df.loc[symbol]

    def filter_tickers(
        self,
        asset_class: Optional[str] = None,
        sector: Optional[str] = None,
        invested_region: Optional[str] = None,
        style: Optional[str] = None,
        currency: Optional[str] = None,
        domicile: Optional[str] = None,
        quote_type: Optional[str] = None,
        exclude_discontinued: bool = True,
    ) -> pd.DataFrame:
        """Filter the classification table by any combination of dimensions.

        Example::

            mda.filter_tickers(asset_class="equity", invested_region="EM")
            mda.filter_tickers(sector="Technology", exclude_discontinued=True)
            mda.filter_tickers(domicile="IE")  # UCITS-only

        Returns a DataFrame indexed by ticker.
        """
        df = self.get_ticker_classification()
        if df.empty:
            return df
        mask = pd.Series(True, index=df.index)
        if asset_class is not None:
            mask &= df["asset_class"] == asset_class
        if sector is not None:
            mask &= df["sector"] == sector
        if invested_region is not None:
            mask &= df["invested_region"] == invested_region
        if style is not None:
            mask &= df["style"] == style
        if currency is not None:
            mask &= df["currency"] == currency
        if domicile is not None:
            mask &= df["domicile"] == domicile
        if quote_type is not None:
            mask &= df["quoteType"] == quote_type
        if exclude_discontinued and "suspected_discontinued" in df.columns:
            mask &= ~df["suspected_discontinued"].fillna(False)
        return df.loc[mask]  # type: ignore[return-value]

    def get_active_tickers(self) -> List[str]:
        """Return all tickers NOT flagged as suspected_discontinued."""
        df = self.get_ticker_classification()
        if df.empty:
            return []
        if "suspected_discontinued" not in df.columns:
            return [str(t) for t in df.index]
        active = df[~df["suspected_discontinued"].fillna(False)]
        return [str(t) for t in active.index]

    def get_price_history(
        self,
        symbol: str,
        period: str = None,
        start_date: Union[str, datetime] = None,
        end_date: Union[str, datetime] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Get price history for a symbol with optional date filtering.

        Args:
            symbol: Symbol identifier
            period: Optional period string like '1mo', '1y', 'max'
            start_date: Optional start date (string or datetime)
            end_date: Optional end date (string or datetime)

        Returns:
            DataFrame with price history or None if not found
        """
        try:
            # Check if symbol exists
            if symbol not in self.price_history_lib.list_symbols():
                logger.warning(f"Symbol {symbol} not found in price history")
                return None

            # Read price data
            price_data = self.price_history_lib.read(symbol).data

            if price_data.empty:
                logger.warning(f"Empty price data for symbol {symbol}")
                return None

            # Handle date filtering
            if period:
                # Period takes precedence over explicit dates
                period_start, period_end = self._parse_period(period)
                price_data = self._filter_date_range(
                    price_data, period_start, period_end
                )
            else:
                # Convert string dates to datetime if needed
                if start_date and isinstance(start_date, str):
                    start_date = pd.to_datetime(start_date)
                if end_date and isinstance(end_date, str):
                    end_date = pd.to_datetime(end_date)

                # Filter by date range if specified
                if start_date or end_date:
                    price_data = self._filter_date_range(
                        price_data, start_date, end_date
                    )

            return price_data

        except Exception as e:
            logger.error(f"Error retrieving price history for {symbol}: {str(e)}")
            return None

    def get_symbol_with_prices(
        self,
        symbol: str,
        period: str = None,
        start_date: Union[str, datetime] = None,
        end_date: Union[str, datetime] = None,
    ) -> Dict[str, Any]:
        """
        Get both metadata and price history for a symbol.

        Args:
            symbol: Symbol identifier
            period: Optional period string like '1mo', '1y', 'max'
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            Dictionary with metadata and price history
        """
        result = {
            "symbol": symbol,
            "metadata": None,
            "price_history": None,
            "price_metadata": None,
        }

        # Get symbol metadata
        metadata = self.get_symbol_metadata(symbol)
        if metadata is not None:
            result["metadata"] = metadata.to_dict()

        # Get price history
        price_history = self.get_price_history(symbol, period, start_date, end_date)
        if price_history is not None and not price_history.empty:
            result["price_history"] = price_history

            # Add price stats (handle NaT values)
            min_date = price_history.index.min()
            max_date = price_history.index.max()

            result["price_stats"] = {
                "start_date": min_date.strftime("%Y-%m-%d") if pd.notna(min_date) else None,
                "end_date": max_date.strftime("%Y-%m-%d") if pd.notna(max_date) else None,
                "days": (max_date - min_date).days if pd.notna(min_date) and pd.notna(max_date) else 0,
                "data_points": len(price_history),
            }

        # Get price metadata if available
        try:
            if symbol in self.price_metadata_lib.list_symbols():
                price_meta = self.price_metadata_lib.read(symbol).data
                if not price_meta.empty:
                    result["price_metadata"] = price_meta.iloc[0].to_dict()
        except Exception as e:
            logger.warning(f"Error retrieving price metadata for {symbol}: {str(e)}")

        return result

    def get_multiple_prices(
        self,
        symbols: Union[str, List[str]],
        period: str = None,
        start_date: Union[str, datetime] = None,
        end_date: Union[str, datetime] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        Get price histories for multiple symbols.

        Args:
            symbols: Single symbol (string) or list of symbol identifiers
            period: Optional period string like '1mo', '1y', 'max'
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            Dictionary mapping symbols to price histories
        """
        # Handle single symbol (string) input
        if isinstance(symbols, str):
            symbols = [symbols]

        result = {}

        for symbol in symbols:
            price_data = self.get_price_history(symbol, period, start_date, end_date)
            if price_data is not None:
                result[symbol] = price_data

        return result

    def get_multiple_symbols_with_prices(
        self,
        symbols: Union[str, List[str]],
        period: str = None,
        start_date: Union[str, datetime] = None,
        end_date: Union[str, datetime] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get both metadata and price histories for multiple symbols.

        Args:
            symbols: Single symbol (string) or list of symbol identifiers
            period: Optional period string like '1mo', '1y', 'max'
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            Dictionary mapping symbols to their data (metadata and prices)
        """
        # Handle single symbol (string) input
        if isinstance(symbols, str):
            symbols = [symbols]

        result = {}

        for symbol in symbols:
            symbol_data = self.get_symbol_with_prices(
                symbol, period, start_date, end_date
            )
            result[symbol] = symbol_data

        return result

    def search_symbols(self, pattern: str, quote_type: str = None) -> pd.DataFrame:
        """
        Search for symbols matching a pattern, with optional filtering by quote type.

        Args:
            pattern: Pattern to search for
            quote_type: Optional quote type filter (e.g., 'EQUITY', 'FUTURE')

        Returns:
            DataFrame with matching symbols
        """
        metadata = self.get_symbols_metadata()

        if metadata.empty:
            return pd.DataFrame()

        # Prepare the pattern for case-insensitive search
        pattern = pattern.lower()

        # Filter by symbol and names
        mask = metadata.index.str.lower().str.contains(pattern)

        # Also search in longName and shortName if they exist
        for field in ["longName", "shortName"]:
            if field in metadata.columns:
                field_mask = metadata[field].str.lower().str.contains(pattern, na=False)
                mask = mask | field_mask

        # Apply the filter
        result = metadata[mask]

        # Further filter by quote type if specified
        if quote_type:
            if "quoteType" in result.columns:
                result = result[result["quoteType"] == quote_type]
            else:
                logger.warning("Cannot filter by quoteType: column not found")

        logger.info(f"Found {len(result)} symbols matching pattern '{pattern}'")
        return result

    # Enhanced search functionality
    def advanced_search(
        self,
        pattern: str,
        fields: List[str] = None,
        fuzzy_search: bool = True,
        case_sensitive: bool = False,
        quote_type: Union[str, List[str]] = None,
        min_score: int = 70,
    ) -> pd.DataFrame:
        """
        Advanced search with preprocessing, fuzzy matching, and multiple filters.

        Args:
            pattern: Pattern to search for
            fields: Fields to search in (defaults to index, longName, shortName)
            fuzzy_search: Whether to use fuzzy matching
            case_sensitive: Whether search is case sensitive
            quote_type: Optional quote type filter (single or list)
            min_score: Minimum score for fuzzy matches (0-100)

        Returns:
            DataFrame with matching symbols, with match score column
        """
        metadata = self.get_symbols_metadata()

        if metadata.empty:
            return pd.DataFrame()

        # Default fields to search
        if fields is None:
            fields = ["symbol", "longName", "shortName"]

        # Create a copy of metadata with symbol as a column for easier searching
        search_df = metadata.copy()
        if "symbol" in fields and "symbol" not in search_df.columns:
            search_df["symbol"] = search_df.index

        # Prepare the pattern
        if not case_sensitive:
            pattern = pattern.lower()

        # Preprocess pattern to remove special characters for exact matching
        processed_pattern = re.sub(r"[^a-zA-Z0-9]", "", pattern)

        # Track match scores
        match_scores = pd.Series(0, index=search_df.index)

        # Exact matching with preprocessing
        for field in fields:
            if field in search_df.columns:
                # Process the field values
                if not case_sensitive:
                    field_values = search_df[field].astype(str).str.lower()
                else:
                    field_values = search_df[field].astype(str)

                # Search with original pattern
                exact_matches = field_values.str.contains(pattern, regex=True, na=False)
                match_scores[exact_matches] = 100  # Perfect match

                # If we have a processed pattern, search with that too
                if processed_pattern != pattern:
                    # Remove special chars from field values
                    processed_values = field_values.apply(
                        lambda x: re.sub(r"[^a-zA-Z0-9]", "", x)
                    )
                    processed_matches = processed_values.str.contains(
                        processed_pattern, regex=True, na=False
                    )
                    # Update scores only for new matches
                    new_matches = processed_matches & ~match_scores.index.isin(
                        match_scores[match_scores == 100].index
                    )
                    match_scores[new_matches] = 95  # Very good match

        # Fuzzy matching if enabled and we haven't found many exact matches
        if fuzzy_search and (len(match_scores[match_scores >= min_score]) < 50):
            for field in fields:
                if field in search_df.columns:
                    # Only compare values from rows not already well-matched
                    unmatched = match_scores[match_scores < min_score].index
                    if len(unmatched) == 0:
                        continue

                    field_values = search_df.loc[unmatched, field].astype(str)

                    # Calculate fuzzy match scores
                    for idx, value in field_values.items():
                        if not pd.isna(value) and value:
                            # Skip very long strings or empty values
                            if len(value) > 100:
                                continue

                            score = fuzz.partial_ratio(
                                pattern, value if case_sensitive else value.lower()
                            )

                            # Update score if better than current
                            if score > match_scores[idx]:
                                match_scores[idx] = score

        # Filter by minimum score
        matches = match_scores[match_scores >= min_score].index
        result = metadata.loc[matches].copy()

        # Add match score column
        result["match_score"] = match_scores[matches]

        # Sort by match score (highest first)
        result = result.sort_values("match_score", ascending=False)

        # Filter by quote type if specified
        if quote_type:
            if "quoteType" in result.columns:
                if isinstance(quote_type, list):
                    result = result[result["quoteType"].isin(quote_type)]
                else:
                    result = result[result["quoteType"] == quote_type]
            else:
                logger.warning("Cannot filter by quoteType: column not found")

        logger.info(
            f"Found {len(result)} symbols matching pattern '{pattern}' using advanced search"
        )
        return result

    def multi_pattern_search(
        self,
        patterns: List[str],
        fields: List[str] = None,
        fuzzy_search: bool = True,
        quote_type: Union[str, List[str]] = None,
        match_all: bool = False,
    ) -> pd.DataFrame:
        """
        Search with multiple patterns.

        Args:
            patterns: List of patterns to search for
            fields: Fields to search in
            fuzzy_search: Whether to use fuzzy matching
            quote_type: Optional quote type filter
            match_all: Whether all patterns must match (AND) or any can match (OR)

        Returns:
            DataFrame with matching symbols
        """
        if not patterns:
            return pd.DataFrame()

        # Get results for each pattern
        pattern_results = []
        for pattern in patterns:
            result = self.advanced_search(
                pattern, fields=fields, fuzzy_search=fuzzy_search, quote_type=quote_type
            )
            pattern_results.append(result)

        if not pattern_results:
            return pd.DataFrame()

        # Combine results based on match_all parameter
        if match_all:
            # All patterns must match (intersection)
            # Start with all symbols from first result
            common_symbols = set(pattern_results[0].index)

            # Intersect with each subsequent result
            for result in pattern_results[1:]:
                common_symbols = common_symbols.intersection(set(result.index))

            # Get the rows for these symbols
            if common_symbols:
                final_result = pattern_results[0].loc[list(common_symbols)]
            else:
                final_result = pd.DataFrame()
        else:
            # Any pattern can match (union)
            # Concatenate all results and drop duplicates
            final_result = pd.concat(pattern_results).drop_duplicates()

        logger.info(f"Multi-pattern search found {len(final_result)} symbols")
        return final_result

    # Export functionality
    def export_symbol_to_csv(
        self,
        symbol: str,
        output_path: str,
        period: str = None,
        start_date: str = None,
        end_date: str = None,
    ) -> bool:
        """
        Export a symbol's metadata and price history to CSV file.

        Args:
            symbol: Symbol identifier
            output_path: Path to save the CSV file
            period: Optional period string
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            True if successful, False otherwise
        """
        try:
            # Get symbol data
            symbol_data = self.get_symbol_with_prices(
                symbol, period, start_date, end_date
            )

            if symbol_data["price_history"] is None:
                logger.warning(f"No price history found for {symbol}")
                return False

            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

            # Create a DataFrame with metadata in the first rows, then prices
            metadata_rows = []

            # Add symbol info
            metadata_rows.append(["Symbol", symbol])

            # Add metadata if available
            if symbol_data["metadata"]:
                for key, value in symbol_data["metadata"].items():
                    metadata_rows.append([key, value])

            # Add price metadata if available
            if symbol_data["price_metadata"]:
                metadata_rows.append(["", ""])
                metadata_rows.append(["Price Metadata", ""])
                for key, value in symbol_data["price_metadata"].items():
                    metadata_rows.append([key, value])

            # Add price stats
            if "price_stats" in symbol_data:
                metadata_rows.append(["", ""])
                metadata_rows.append(["Price Statistics", ""])
                for key, value in symbol_data["price_stats"].items():
                    metadata_rows.append([key, value])

            # Add separator
            metadata_rows.append(["", ""])
            metadata_rows.append(["Price History", ""])
            metadata_rows.append(["", ""])

            # Get price history
            price_df = symbol_data["price_history"]

            # Reset index to include date as column
            price_df = price_df.reset_index()

            # Write to CSV
            with open(output_path, "w") as f:
                # Write metadata rows
                for row in metadata_rows:
                    f.write(f"{row[0]},{row[1]}\n")

                # Write price data
                price_df.to_csv(f, index=False)

            logger.info(f"Exported {symbol} data to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Error exporting {symbol} to CSV: {str(e)}")
            return False

    def export_symbols_to_excel(
        self,
        symbols: List[str],
        output_path: str,
        period: str = None,
        start_date: str = None,
        end_date: str = None,
    ) -> bool:
        """
        Export multiple symbols' data to Excel file with one tab per symbol.

        Args:
            symbols: List of symbol identifiers
            output_path: Path to save the Excel file
            period: Optional period string
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            True if successful, False otherwise
        """
        try:
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

            # Create Excel workbook
            workbook = xlsxwriter.Workbook(output_path)

            # Create formats
            header_format = workbook.add_format(
                {"bold": True, "bg_color": "#D3D3D3", "border": 1}
            )

            date_format = workbook.add_format({"num_format": "yyyy-mm-dd"})

            section_format = workbook.add_format(
                {"bold": True, "font_size": 12, "bg_color": "#E6E6FA"}
            )

            # Process each symbol
            successful = 0

            for symbol in symbols:
                try:
                    # Get symbol data
                    symbol_data = self.get_symbol_with_prices(
                        symbol, period, start_date, end_date
                    )

                    if symbol_data["price_history"] is None:
                        logger.warning(f"No price history found for {symbol}, skipping")
                        continue

                    # Create a worksheet for this symbol
                    # Use a safe worksheet name (Excel limits worksheet names to 31 chars and no special chars)
                    safe_name = re.sub(r"[\\/*\[\]:?]", "_", symbol)[:31]
                    worksheet = workbook.add_worksheet(safe_name)

                    # Write metadata section
                    row = 0

                    # Add symbol info
                    worksheet.write(row, 0, "Symbol", section_format)
                    worksheet.write(row, 1, symbol)
                    row += 2

                    # Add metadata if available
                    if symbol_data["metadata"]:
                        worksheet.write(row, 0, "Metadata", section_format)
                        row += 1

                        for key, value in symbol_data["metadata"].items():
                            worksheet.write(row, 0, key)
                            worksheet.write(row, 1, value)
                            row += 1

                        row += 1

                    # Add price metadata if available
                    if symbol_data["price_metadata"]:
                        worksheet.write(row, 0, "Price Metadata", section_format)
                        row += 1

                        for key, value in symbol_data["price_metadata"].items():
                            worksheet.write(row, 0, key)
                            worksheet.write(row, 1, value)
                            row += 1

                        row += 1

                    # Add price stats
                    if "price_stats" in symbol_data:
                        worksheet.write(row, 0, "Price Statistics", section_format)
                        row += 1

                        for key, value in symbol_data["price_stats"].items():
                            worksheet.write(row, 0, key)
                            worksheet.write(row, 1, value)
                            row += 1

                        row += 1

                    # Add price history
                    worksheet.write(row, 0, "Price History", section_format)
                    row += 2

                    # Get price history
                    price_df = symbol_data["price_history"].reset_index()

                    # Write header
                    for col_idx, col_name in enumerate(price_df.columns):
                        worksheet.write(row, col_idx, col_name, header_format)
                    row += 1

                    # Write data
                    for data_row in range(len(price_df)):
                        for col_idx, col_name in enumerate(price_df.columns):
                            value = price_df.iloc[data_row, col_idx]

                            # Format dates
                            if col_name == "Date" or col_name == "index":
                                worksheet.write_datetime(
                                    row, col_idx, value.to_pydatetime(), date_format
                                )
                            else:
                                worksheet.write(row, col_idx, value)
                        row += 1

                    # Auto-adjust column widths
                    worksheet.autofit()

                    successful += 1

                except Exception as e:
                    logger.error(f"Error exporting {symbol} to Excel: {str(e)}")
                    # Continue with next symbol

            # Close the workbook
            workbook.close()

            logger.info(
                f"Exported {successful} out of {len(symbols)} symbols to {output_path}"
            )
            return successful > 0

        except Exception as e:
            logger.error(f"Error exporting symbols to Excel: {str(e)}")
            return False

    # Symbol status functions
    def get_symbol_status_counts(self) -> Dict[str, int]:
        """
        Get counts of symbols by status.

        Returns:
            Dictionary with counts by status
        """
        metadata = self.get_symbols_metadata()

        if metadata.empty or "status" not in metadata.columns:
            return {"unknown": len(metadata)}

        # Count by status
        status_counts = metadata["status"].value_counts().to_dict()

        # Add total
        status_counts["total"] = len(metadata)

        return status_counts

    def get_active_symbols(self) -> pd.DataFrame:
        """
        Get all active symbols.

        Returns:
            DataFrame with active symbols
        """
        metadata = self.get_symbols_metadata()

        if metadata.empty:
            return pd.DataFrame()

        if "status" in metadata.columns:
            return metadata[metadata["status"] == "active"]
        else:
            logger.warning("Status column not found, returning all symbols")
            return metadata

    def get_failed_symbols(self) -> List[str]:
        """
        Get symbols that have failed price data retrieval.

        Returns:
            List of failed symbol identifiers
        """
        failed_symbols = []

        try:
            # Check symbols with price metadata
            all_price_meta_symbols = set(self.price_metadata_lib.list_symbols())

            for symbol in all_price_meta_symbols:
                try:
                    meta_df = self.price_metadata_lib.read(symbol).data

                    if meta_df.empty:
                        continue

                    if (
                        "data_status" in meta_df.columns
                        and meta_df.iloc[0]["data_status"] == "failed"
                    ):
                        failed_symbols.append(symbol)

                except Exception:
                    # Skip any symbols that fail to read
                    continue

            logger.info(f"Found {len(failed_symbols)} failed symbols")

        except Exception as e:
            logger.error(f"Error getting failed symbols: {str(e)}")

        return failed_symbols

    def get_stale_symbols(self, days_threshold: int = 30) -> List[str]:
        """
        Get symbols with stale price data.

        Args:
            days_threshold: Number of days after which data is considered stale

        Returns:
            List of stale symbol identifiers
        """
        stale_symbols = []
        current_date = datetime.now().date()
        stale_cutoff = current_date - timedelta(days=days_threshold)

        try:
            # Check symbols with price metadata
            all_price_meta_symbols = set(self.price_metadata_lib.list_symbols())

            for symbol in all_price_meta_symbols:
                try:
                    meta_df = self.price_metadata_lib.read(symbol).data

                    if meta_df.empty:
                        continue

                    if "last_date" in meta_df.columns:
                        try:
                            last_date = pd.to_datetime(
                                meta_df.iloc[0]["last_date"]
                            ).date()

                            if last_date < stale_cutoff:
                                stale_symbols.append(symbol)

                        except Exception:
                            # Skip if date parsing fails
                            continue

                except Exception:
                    # Skip any symbols that fail to read
                    continue

            logger.info(
                f"Found {len(stale_symbols)} stale symbols (older than {days_threshold} days)"
            )

        except Exception as e:
            logger.error(f"Error getting stale symbols: {str(e)}")

        return stale_symbols

    def get_symbols_data(
        self, symbols, period="1mo", start_date=None, end_date=None, batch_size=100
    ):
        """
        Retrieve price data and metadata for multiple symbols with a clean, restructured format.

        Args:
            symbols: Single symbol (string) or list of symbol identifiers
            period: Time period for price data ('1mo', '1y', 'max', etc.)
            start_date: Optional start date (used if period is None)
            end_date: Optional end date (used if period is None)
            batch_size: Number of symbols to process in each batch

        Returns:
            Dictionary with 'prices' and 'metadata' keys, each containing
            a dictionary of symbol-indexed data
        """
        # Handle single symbol (string) input
        if isinstance(symbols, str):
            symbols = [symbols]

        logger.info(
            f"Retrieving structured data for {len(symbols)} symbols with period={period}"
        )

        # Check if we need to batch process
        if len(symbols) > batch_size:
            logger.info(f"Processing {len(symbols)} symbols in batches of {batch_size}")

            # Initialize result containers
            all_prices = {}
            all_metadata = {}

            # Process in batches
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i : i + batch_size]
                logger.info(
                    f"Processing batch {i//batch_size + 1}: symbols {i+1}-{min(i+batch_size, len(symbols))}"
                )

                # Get batch data
                batch_result = self.get_symbols_data(
                    batch, period, start_date, end_date, batch_size=len(batch)
                )

                # Update results
                all_prices.update(batch_result["prices"])
                all_metadata.update(batch_result["metadata"])

            logger.info(
                f"Completed batch processing: {len(all_prices)} price series, {len(all_metadata)} metadata entries"
            )
            return {"prices": all_prices, "metadata": all_metadata}

        # Retrieve data for all symbols (for a single batch)
        try:
            raw_data = self.get_multiple_symbols_with_prices(
                symbols, period=period, start_date=start_date, end_date=end_date
            )
            logger.info(f"Successfully retrieved data for {len(raw_data)} symbols")
        except Exception as e:
            logger.error(f"Failed to retrieve symbol data: {str(e)}")
            raise

        # Restructure the data
        prices = {}
        metadata = {}
        missing_price_data = []

        for symbol, symbol_data in raw_data.items():
            # Add price data if available
            if (
                symbol_data.get("price_history") is not None
                and not symbol_data["price_history"].empty
            ):
                prices[symbol] = symbol_data["price_history"]
            else:
                missing_price_data.append(symbol)

            # Flatten and combine metadata
            symbol_metadata = {}

            # Add symbol basic info
            symbol_metadata["symbol"] = symbol

            # Add general metadata if available
            if symbol_data.get("metadata"):
                for key, value in symbol_data["metadata"].items():
                    symbol_metadata[key] = value

            # Add price metadata if available
            if symbol_data.get("price_metadata"):
                for key, value in symbol_data["price_metadata"].items():
                    symbol_metadata[key] = value

            # Add calculated price stats if available
            if symbol_data.get("price_stats"):
                for key, value in symbol_data["price_stats"].items():
                    # Avoid overwriting existing keys (price_metadata takes precedence)
                    if key not in symbol_metadata:
                        symbol_metadata[key] = value

            metadata[symbol] = symbol_metadata

        # Log any issues
        if missing_price_data:
            logger.warning(
                f"No price data found for {len(missing_price_data)} symbols: {missing_price_data[:5]}..."
            )

        # Return the restructured data
        logger.info(
            f"Successfully processed data: {len(prices)} price series, {len(metadata)} metadata entries"
        )
        return {"prices": prices, "metadata": metadata}

    def get_symbols_as_dataframe(
        self,
        symbols: Union[str, List[str]],
        column: str = "Close",
        period: str = None,
        start_date: Union[str, datetime] = None,
        end_date: Union[str, datetime] = None,
        fill_method: Optional[str] = "ffill",
    ) -> pd.DataFrame:
        """
        Get price data for multiple symbols as a single DataFrame with symbols as columns.

        Args:
            symbols: Single symbol (string) or list of symbol identifiers
            column: Price column to extract ('Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close')
            period: Optional period string like '1mo', '1y', 'max'
            start_date: Optional start date
            end_date: Optional end date
            fill_method: Method to fill missing values ('ffill', 'bfill', None)

        Returns:
            DataFrame with DatetimeIndex and symbols as columns
        """
        # Handle single symbol (string) input
        if isinstance(symbols, str):
            symbols = [symbols]

        logger.info(
            f"Creating DataFrame for {len(symbols)} symbols, column={column}, period={period}"
        )

        # Get price data for all symbols
        prices_dict = self.get_multiple_prices(symbols, period, start_date, end_date)

        if not prices_dict:
            logger.warning("No price data retrieved for any symbol")
            return pd.DataFrame()

        # Extract the specified column from each symbol's data
        series_dict = {}
        for symbol, price_df in prices_dict.items():
            if column in price_df.columns:
                series_dict[symbol] = price_df[column]
            else:
                logger.warning(f"Column '{column}' not found for {symbol}, skipping")

        if not series_dict:
            logger.warning(f"No data found for column '{column}'")
            return pd.DataFrame()

        # Combine into a single DataFrame
        combined_df = pd.DataFrame(series_dict)

        # Ensure DatetimeIndex
        if not isinstance(combined_df.index, pd.DatetimeIndex):
            combined_df.index = pd.to_datetime(combined_df.index)

        # Sort by date
        combined_df = combined_df.sort_index()

        # Fill missing values if requested
        if fill_method:
            if fill_method == "ffill":
                combined_df = combined_df.ffill()
            elif fill_method == "bfill":
                combined_df = combined_df.bfill()
            else:
                combined_df = combined_df.fillna(method=fill_method)

        logger.info(
            f"Created DataFrame: {len(combined_df)} rows, {len(combined_df.columns)} symbols"
        )
        return combined_df

    def export_to_parquet(
        self,
        symbols: Union[str, List[str]],
        output_path: str,
        period: str = None,
        start_date: Union[str, datetime] = None,
        end_date: Union[str, datetime] = None,
        include_metadata: bool = True,
        compression: str = "snappy",
    ) -> bool:
        """
        Export symbol data to Parquet format for efficient storage.

        Args:
            symbols: Single symbol or list of symbols
            output_path: Path to save the Parquet file
            period: Optional period string
            start_date: Optional start date
            end_date: Optional end date
            include_metadata: Whether to include metadata in a separate file
            compression: Compression algorithm ('snappy', 'gzip', 'brotli', None)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Handle single symbol
            if isinstance(symbols, str):
                symbols = [symbols]

            logger.info(
                f"Exporting {len(symbols)} symbols to Parquet: {output_path}"
            )

            # Create output directory
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

            # Get data for all symbols
            data = self.get_symbols_data(symbols, period, start_date, end_date)

            if not data["prices"]:
                logger.warning("No price data to export")
                return False

            # Export prices
            # Combine all symbols into one DataFrame with MultiIndex (Date, Symbol)
            price_frames = []
            for symbol, price_df in data["prices"].items():
                price_df = price_df.copy()
                price_df["Symbol"] = symbol
                price_frames.append(price_df)

            if price_frames:
                combined_prices = pd.concat(price_frames)
                combined_prices = combined_prices.reset_index()

                # Ensure Date column is properly formatted
                if "Date" in combined_prices.columns:
                    combined_prices["Date"] = pd.to_datetime(combined_prices["Date"])
                elif combined_prices.index.name == "Date":
                    combined_prices = combined_prices.reset_index()
                    combined_prices["Date"] = pd.to_datetime(combined_prices["Date"])

                # Save to Parquet
                combined_prices.to_parquet(
                    output_path,
                    engine="pyarrow",
                    compression=compression,
                    index=False,
                )
                logger.info(f"Saved price data to {output_path}")

            # Export metadata if requested
            if include_metadata and data["metadata"]:
                metadata_path = output_path.replace(".parquet", "_metadata.parquet")
                metadata_df = pd.DataFrame.from_dict(data["metadata"], orient="index")

                metadata_df.to_parquet(
                    metadata_path,
                    engine="pyarrow",
                    compression=compression,
                )
                logger.info(f"Saved metadata to {metadata_path}")

            logger.info(
                f"Successfully exported {len(symbols)} symbols to Parquet format"
            )
            return True

        except Exception as e:
            logger.error(f"Error exporting to Parquet: {str(e)}")
            return False

    def export_to_pickle(
        self,
        symbols: Union[str, List[str]],
        output_path: str,
        period: str = None,
        start_date: Union[str, datetime] = None,
        end_date: Union[str, datetime] = None,
        protocol: int = pickle.HIGHEST_PROTOCOL,
    ) -> bool:
        """
        Export symbol data to Pickle format for Python object serialization.

        Args:
            symbols: Single symbol or list of symbols
            output_path: Path to save the pickle file
            period: Optional period string
            start_date: Optional start date
            end_date: Optional end date
            protocol: Pickle protocol version (default: highest)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Handle single symbol
            if isinstance(symbols, str):
                symbols = [symbols]

            logger.info(
                f"Exporting {len(symbols)} symbols to Pickle: {output_path}"
            )

            # Create output directory
            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

            # Get data for all symbols
            data = self.get_symbols_data(symbols, period, start_date, end_date)

            if not data["prices"]:
                logger.warning("No price data to export")
                return False

            # Create export structure
            export_data = {
                "symbols": symbols,
                "prices": data["prices"],
                "metadata": data["metadata"],
                "export_date": datetime.now(),
                "period": period,
                "start_date": start_date,
                "end_date": end_date,
            }

            # Save to pickle
            with open(output_path, "wb") as f:
                pickle.dump(export_data, f, protocol=protocol)

            logger.info(
                f"Successfully exported {len(symbols)} symbols to Pickle format"
            )
            return True

        except Exception as e:
            logger.error(f"Error exporting to Pickle: {str(e)}")
            return False

    def export_symbols_to_csv_improved(
        self,
        symbols: Union[str, List[str]],
        output_dir: str,
        period: str = None,
        start_date: Union[str, datetime] = None,
        end_date: Union[str, datetime] = None,
        include_metadata: bool = True,
        single_file: bool = False,
    ) -> bool:
        """
        Export symbols to CSV with improved date formatting and structure.

        Args:
            symbols: Single symbol or list of symbols
            output_dir: Directory to save CSV files
            period: Optional period string
            start_date: Optional start date
            end_date: Optional end date
            include_metadata: Whether to include metadata in the CSV
            single_file: If True, combine all symbols into one CSV

        Returns:
            True if successful, False otherwise
        """
        try:
            # Handle single symbol
            if isinstance(symbols, str):
                symbols = [symbols]

            logger.info(
                f"Exporting {len(symbols)} symbols to CSV: {output_dir}"
            )

            # Create output directory
            os.makedirs(output_dir, exist_ok=True)

            # Get data for all symbols
            data = self.get_symbols_data(symbols, period, start_date, end_date)

            if not data["prices"]:
                logger.warning("No price data to export")
                return False

            if single_file:
                # Combine all symbols into one CSV
                price_frames = []
                for symbol, price_df in data["prices"].items():
                    price_df = price_df.copy()
                    price_df["Symbol"] = symbol
                    price_frames.append(price_df)

                combined_df = pd.concat(price_frames)
                combined_df = combined_df.reset_index()

                # Format dates as YYYY-MM-DD
                if "Date" in combined_df.columns:
                    combined_df["Date"] = pd.to_datetime(combined_df["Date"]).dt.strftime("%Y-%m-%d")

                output_path = os.path.join(output_dir, "combined_symbols.csv")
                combined_df.to_csv(output_path, index=False, date_format="%Y-%m-%d")
                logger.info(f"Saved combined data to {output_path}")

                # Save metadata if requested
                if include_metadata and data["metadata"]:
                    metadata_df = pd.DataFrame.from_dict(data["metadata"], orient="index")
                    metadata_path = os.path.join(output_dir, "combined_metadata.csv")
                    metadata_df.to_csv(metadata_path)
                    logger.info(f"Saved metadata to {metadata_path}")

            else:
                # Save each symbol to a separate CSV
                successful = 0
                for symbol in symbols:
                    if symbol not in data["prices"]:
                        logger.warning(f"No price data for {symbol}, skipping")
                        continue

                    price_df = data["prices"][symbol].copy()
                    price_df = price_df.reset_index()

                    # Format dates as YYYY-MM-DD
                    if "Date" in price_df.columns:
                        price_df["Date"] = pd.to_datetime(price_df["Date"]).dt.strftime("%Y-%m-%d")

                    # Safe filename
                    safe_symbol = re.sub(r'[<>:"/\\|?*]', '_', symbol)
                    output_path = os.path.join(output_dir, f"{safe_symbol}.csv")

                    if include_metadata and symbol in data["metadata"]:
                        # Write metadata first, then price data
                        with open(output_path, "w") as f:
                            f.write("# Metadata\n")
                            for key, value in data["metadata"][symbol].items():
                                f.write(f"# {key},{value}\n")
                            f.write("\n")

                            # Write price data
                            price_df.to_csv(f, index=False, date_format="%Y-%m-%d")
                    else:
                        price_df.to_csv(output_path, index=False, date_format="%Y-%m-%d")

                    successful += 1

                logger.info(f"Exported {successful}/{len(symbols)} symbols to CSV")

            return True

        except Exception as e:
            logger.error(f"Error exporting to CSV: {str(e)}")
            return False

    def disconnect(self) -> None:
        """Disconnect from ArcticDB and clean up resources."""
        if not self._connected:
            logger.debug("Already disconnected from ArcticDB")
            return

        try:
            logger.info(f"Disconnecting from ArcticDB at {self.db_path}")

            # Clear library references
            self.symbol_metadata_lib = None
            self.price_history_lib = None
            self.price_metadata_lib = None

            # Clear cache
            self._symbols_metadata_cache = None

            # Clear Arctic connection
            self.arctic = None

            self._connected = False
            logger.info("Successfully disconnected from ArcticDB")

        except Exception as e:
            logger.error(f"Error during ArcticDB disconnection: {str(e)}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures cleanup."""
        self.disconnect()

    def __del__(self):
        """Destructor - ensure cleanup on object deletion."""
        try:
            if hasattr(self, "_connected") and self._connected:
                self.disconnect()
        except Exception:
            # Ignore any errors during cleanup in destructor
            pass

    @property
    def is_connected(self) -> bool:
        """Check if currently connected to the database."""
        return self._connected and self.arctic is not None

    def reconnect(self) -> None:
        """Disconnect and reconnect to the database."""
        logger.info("Reconnecting to ArcticDB...")
        self.disconnect()
        self._connect()

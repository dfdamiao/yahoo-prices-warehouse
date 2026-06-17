#!/usr/bin/env python3
# refresh_symbol_caches.py
import logging
import time
import sys
from pathlib import Path
from datetime import datetime


from market_data.settings import DB_PATH
from market_data.manager import MarketDataManager

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("cache_refresh")


def should_skip_refresh(cache_file, max_age_hours=24):
    """
    Check if cache refresh should be skipped based on timestamp.

    Args:
        cache_file: Path to timestamp cache file
        max_age_hours: Maximum age in hours before refresh needed

    Returns:
        Tuple of (should_skip: bool, age_hours: float)
    """
    if not cache_file.exists():
        return False, 0

    try:
        last_refresh = datetime.fromtimestamp(cache_file.stat().st_mtime)
        age = datetime.now() - last_refresh
        age_hours = age.total_seconds() / 3600

        if age_hours < max_age_hours:
            return True, age_hours

    except Exception as e:
        logger.warning(f"Error checking cache timestamp: {e}")

    return False, 0


def refresh_all_symbol_caches(db_path=DB_PATH, force=False, max_age_hours=24):
    """
    Optimized symbol list cache refresh for ArcticDB 5.4 on macOS with LMDB.
    Addresses the warning: "list_symbols may take longer than expected"
    Includes timestamp checking to skip recent refreshes.

    Args:
        db_path: Path to the ArcticDB database
        force: Force refresh regardless of timestamp (default: False)
        max_age_hours: Skip refresh if last run was within this many hours (default: 24)

    Returns:
        Dictionary with refresh statistics
    """
    from market_data.settings import LOGS_DIR

    # Check timestamp cache
    cache_dir = Path(LOGS_DIR) / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    timestamp_file = cache_dir / "symbol_cache_refresh.timestamp"

    skip, age_hours = should_skip_refresh(timestamp_file, max_age_hours)

    if skip and not force:
        logger.info(f"Skipping refresh - last run was {age_hours:.1f} hours ago (< {max_age_hours}h threshold)")
        logger.info("Use force=True to override this check")
        return {
            "skipped": True,
            "last_refresh_hours_ago": age_hours,
            "symbol_metadata_count": 0,
            "price_history_count": 0,
            "price_metadata_count": 0,
            "duration_seconds": 0,
        }

    logger.info("Running ArcticDB 5.4 symbol list cache refresh...")

    try:
        start_time = time.time()

        # Step 1: Configure ArcticDB's caching behavior
        try:
            import arcticdb.config as adb_config

            # In 5.4, these settings have been refined
            # Disable version map caching to force fresh reads
            adb_config.set_config_int("VersionMap.ReloadInterval", 0)

            # Lower threshold for symbol list compaction
            adb_config.set_config_int(
                "SymbolList.MaxDelta", 50
            )  # More aggressive in 5.4

            # Increase object cache size for better performance
            adb_config.set_config_int("Storage.ObjectCacheSize", 2048)

            # Enable aggressive caching for read operations
            adb_config.set_config_int("Storage.S3ReadCaching", 1)

            logger.info("ArcticDB 5.4 cache parameters configured")
        except Exception as e:
            logger.warning(f"Could not configure ArcticDB parameters: {str(e)}")

        # Step 2: Get ArcticDB libraries from the data manager
        data_manager = MarketDataManager.get_instance(db_path=db_path)

        # Track results
        symbol_meta_count = 0
        price_history_count = 0
        price_meta_count = 0

        # Step 3: Process each library with optimized approach
        logger.info("Refreshing symbol_metadata library cache...")
        try:
            # Get the library
            library = data_manager.symbol_metadata_lib

            # First reload the symbol list using the documented method
            if hasattr(library, "reload_symbol_list"):
                logger.info("Using reload_symbol_list on symbol_metadata")
                library.reload_symbol_list()
                logger.info("Symbol list cache reload complete")

            # Now enumerate all symbols to ensure cache is fully loaded
            symbols = list(library.list_symbols())  # Force full enumeration
            logger.info(f"Symbol metadata library contains {len(symbols)} symbols")
            symbol_meta_count = len(symbols)
        except Exception as e:
            logger.error(f"Error refreshing symbol_metadata library: {str(e)}")

        logger.info("Refreshing price_history.daily library cache...")
        try:
            # Get the library
            library = data_manager.price_history_lib

            # First reload the symbol list
            if hasattr(library, "reload_symbol_list"):
                logger.info("Using reload_symbol_list on price_history.daily")
                library.reload_symbol_list()
                logger.info("Symbol list cache reload complete")

            # Now enumerate all symbols
            symbols = list(library.list_symbols())
            logger.info(f"Price history library contains {len(symbols)} symbols")
            price_history_count = len(symbols)
        except Exception as e:
            logger.error(f"Error refreshing price_history.daily library: {str(e)}")

        logger.info("Refreshing price_history.metadata library cache...")
        try:
            # Get the library
            library = data_manager.price_metadata_lib

            # First reload the symbol list
            if hasattr(library, "reload_symbol_list"):
                logger.info("Using reload_symbol_list on price_history.metadata")
                library.reload_symbol_list()
                logger.info("Symbol list cache reload complete")

            # Now enumerate all symbols
            symbols = list(library.list_symbols())
            logger.info(f"Price metadata library contains {len(symbols)} symbols")
            price_meta_count = len(symbols)
        except Exception as e:
            logger.error(f"Error refreshing price_history.metadata library: {str(e)}")

        # Step 4: For LMDB on macOS, ensure storage is properly flushed
        if "lmdb" in db_path.lower() and sys.platform == "darwin":
            logger.info("Detected LMDB on macOS, performing additional sync operations")
            try:
                # Try to get the Arctic instance through data_manager
                arctic = None
                for attr_name in ["_arctic", "arctic"]:
                    if hasattr(data_manager, attr_name):
                        arctic = getattr(data_manager, attr_name)
                        break

                if arctic:
                    # In 5.4, use more direct LMDB sync methods if available
                    for attr_name in ["_conn", "_store", "store"]:
                        if hasattr(arctic, attr_name):
                            store = getattr(arctic, attr_name)
                            if hasattr(store, "sync"):
                                store.sync()
                                logger.info("Performed LMDB sync on macOS")
                                break
                else:
                    logger.warning("Could not access Arctic instance for LMDB sync")
            except Exception as e:
                logger.warning(f"LMDB sync attempt: {str(e)}")

        # Report results
        duration = time.time() - start_time
        logger.info(f"Cache refresh completed in {duration:.2f} seconds")

        # Update timestamp file
        timestamp_file.touch()
        logger.info(f"Updated cache timestamp: {timestamp_file}")

        return {
            "skipped": False,
            "symbol_metadata_count": symbol_meta_count,
            "price_history_count": price_history_count,
            "price_metadata_count": price_meta_count,
            "duration_seconds": duration,
        }

    except Exception as e:
        logger.error(f"Error in symbol cache refresh: {str(e)}")
        return {
            "skipped": False,
            "symbol_metadata_count": 0,
            "price_history_count": 0,
            "price_metadata_count": 0,
            "duration_seconds": 0,
            "error": str(e),
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Refresh ArcticDB symbol list caches")
    parser.add_argument("--force", action="store_true", help="Force refresh ignoring timestamp")
    parser.add_argument("--max-age-hours", type=float, default=24, help="Skip if refreshed within N hours")
    args = parser.parse_args()

    results = refresh_all_symbol_caches(force=args.force, max_age_hours=args.max_age_hours)

    print("\nSymbol Cache Refresh Summary:")
    if results.get("skipped"):
        print(f"✓ Skipped - last refresh was {results['last_refresh_hours_ago']:.1f} hours ago")
        print("  (Use --force to override)")
    else:
        print(f"Symbol Metadata Library: {results['symbol_metadata_count']} symbols")
        print(f"Price History Library: {results['price_history_count']} symbols")
        print(f"Price Metadata Library: {results['price_metadata_count']} symbols")
        print(f"Total duration: {results['duration_seconds']:.2f} seconds")

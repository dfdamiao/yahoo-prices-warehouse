import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from market_data.manager import MarketDataManager


def get_symbols_by_status(status="complete", db_path=None, batch_size=1000, use_threading=True, max_workers=4):
    """
    Get a list of symbols that exist in price_history.daily AND have the specified status in metadata.
    Optimized for large datasets with batch processing and optional concurrent reads.

    Args:
        status: The status to filter by (default: "complete")
        db_path: Optional database path
        batch_size: Number of symbols to process in each batch
        use_threading: Enable concurrent reads (default: True)
        max_workers: Number of concurrent threads (default: 4)

    Returns:
        List of symbols with the specified status
    """
    logger = logging.getLogger()
    logger.info(f"Filtering for symbols with '{status}' status (optimized method)...")

    data_manager = MarketDataManager.get_instance(db_path)

    # Get lists of symbols from both libraries
    price_symbols = set(data_manager.price_history_lib.list_symbols())
    price_meta_symbols = set(data_manager.price_metadata_lib.list_symbols())

    logger.info(f"Total symbols in price_history.daily: {len(price_symbols)}")
    logger.info(f"Total symbols in price_history.metadata: {len(price_meta_symbols)}")

    # Find symbols that exist in both libraries (intersection)
    common_symbols = price_symbols.intersection(price_meta_symbols)
    logger.info(f"Symbols present in both libraries: {len(common_symbols)}")

    # Drop tickers flagged 'discontinued' in symbols_metadata (set by
    # a dead-ticker job). Falls back to the unfiltered
    # set if symbols_metadata is missing or has no 'status' column.
    try:
        md = data_manager.symbol_metadata_lib.read("symbols_metadata").data
        if md is not None and "status" in md.columns:
            active_set = set(md[md["status"] == "active"].index)
            n_before = len(common_symbols)
            common_symbols = common_symbols & active_set
            logger.info(
                f"Active-status filter: {n_before} -> {len(common_symbols)} "
                f"({n_before - len(common_symbols)} dropped as discontinued)"
            )
        else:
            logger.warning(
                "symbols_metadata missing 'status' column - "
                "skipping discontinuation filter"
            )
    except Exception as e:
        logger.warning(
            f"Could not apply active-status filter ({e}); "
            f"using full intersected list"
        )

    common_symbols = list(common_symbols)

    # Process in batches to be memory-efficient
    filtered_symbols = []
    total_batches = (len(common_symbols) + batch_size - 1) // batch_size

    logger.info(
        f"Processing in {total_batches} batches of up to {batch_size} symbols each..."
    )

    for batch_idx in range(total_batches):
        # Calculate batch boundaries
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(common_symbols))
        batch = common_symbols[start_idx:end_idx]

        logger.info(
            f"Processing batch {batch_idx+1}/{total_batches}: symbols {start_idx+1}-{end_idx}"
        )

        # Process batch with optional threading for concurrent reads
        batch_filtered = []

        def check_symbol_status(symbol):
            """Check if symbol has the target status"""
            try:
                metadata = data_manager.price_metadata_lib.read(symbol).data

                if not metadata.empty and "data_status" in metadata.columns:
                    symbol_status = metadata.iloc[0]["data_status"]
                    return symbol if symbol_status == status else None

            except Exception as e:
                logger.error(f"Error reading metadata for {symbol}: {str(e)}")
            return None

        if use_threading and len(batch) > 10:
            # Use ThreadPoolExecutor for concurrent reads
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(check_symbol_status, sym): sym for sym in batch}

                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        batch_filtered.append(result)
        else:
            # Sequential processing for small batches
            for symbol in batch:
                result = check_symbol_status(symbol)
                if result:
                    batch_filtered.append(result)

        # Add batch results to overall list
        filtered_symbols.extend(batch_filtered)
        logger.info(
            f"Found {len(batch_filtered)} '{status}' symbols in this batch, {len(filtered_symbols)} total so far"
        )

    logger.info(
        f"Complete: Found {len(filtered_symbols)} symbols with '{status}' status"
    )
    if price_symbols:
        logger.info(
            f"This is {len(filtered_symbols)/len(price_symbols)*100:.1f}% of all price data symbols"
        )

    return filtered_symbols

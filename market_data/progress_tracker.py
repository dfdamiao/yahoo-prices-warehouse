# progress_tracker.py
import os
import json
import random
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging


from market_data.settings import DB_PATH
from market_data._notify import send_telegram_notification

# Configure logging
logger = logging.getLogger("market_data.progress")


class ProgressTracker:
    """
    Manages progress tracking for batch symbol data updates.

    Tracks which symbols have been processed, which are pending,
    and maintains batch information for resumable processing.
    """

    def __init__(
        self,
        market_data_dir: str,
        progress_file_name: str = "price_update_progress.json",
        min_batch_size: int = 100,
        max_batch_size: int = 300,
        upcoming_batch_queue_size: int = 20,
    ):
        """
        Initialize the progress tracker.

        Args:
            market_data_dir: Directory for market data files
            progress_file_name: Name of the progress tracking file
            min_batch_size: Minimum symbols per batch
            max_batch_size: Maximum symbols per batch
            upcoming_batch_queue_size: Number of upcoming batches to keep in queue
        """
        self.market_data_dir = market_data_dir
        self.progress_file_path = os.path.join(market_data_dir, progress_file_name)
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        self.upcoming_batch_queue_size = upcoming_batch_queue_size

        # Initialize empty progress state
        self.progress_data = {
            "session_info": {},
            "symbol_status": {"completed": [], "failed": []},
            "batch_queue": {
                "completed_batch_count": 0,
                "next_batch_id": 1,
                "current_batch": None,
                "upcoming_batches": [],
                "batch_size_stats": {
                    "recent_sizes": [],
                    "min_size": min_batch_size,
                    "max_size": max_batch_size,
                    "adaptive_target": (min_batch_size + max_batch_size) // 2,
                },
            },
            "performance_metrics": {
                "average_batch_time": 0,
                "symbols_per_minute": 0,
                "rate_limit_hits": 0,
                "recent_batch_durations": [],
            },
        }

        # Internal tracking sets for efficient lookups
        self._completed_symbols = set()
        self._failed_symbols = set()
        self._all_symbols = set()

        # Check if progress file exists and load it
        if os.path.exists(self.progress_file_path):
            self.load_existing_progress()

    def initialize_new_session(
        self,
        symbols_to_process: List[str],
        update_period: str = "1mo",
        database_path: str = DB_PATH,
        generate_all_batches: bool = True,  # New parameter
    ):
        """
        Initialize a new update session with the given symbols.

        Args:
            symbols_to_process: List of symbols to update
            update_period: Period for data updates (e.g., "1mo")
            database_path: Path to the database being updated
            generate_all_batches: Whether to generate all batches upfront
        """
        # Store all symbols - important to convert to a list here
        self._all_symbols = set(symbols_to_process)

        # Initialize session info
        current_time = datetime.now().isoformat()
        self.progress_data["session_info"] = {
            "start_time": current_time,
            "last_update_time": current_time,
            "status": "initialized",
            "total_symbols_to_process": len(symbols_to_process),
            "completed_count": 0,
            "failed_count": 0,
            "pending_count": len(symbols_to_process),
            "completion_percentage": 0.0,
            "update_period": update_period,
            "version": "1.0",
            "database_path": database_path,
        }

        # Reset tracking
        self.progress_data["symbol_status"]["completed"] = []
        self.progress_data["symbol_status"]["failed"] = []
        self._completed_symbols = set()
        self._failed_symbols = set()

        # Reset batch queue
        self.progress_data["batch_queue"]["completed_batch_count"] = 0
        self.progress_data["batch_queue"]["next_batch_id"] = 1
        self.progress_data["batch_queue"]["current_batch"] = None
        self.progress_data["batch_queue"]["upcoming_batches"] = []

        # Reset performance metrics
        self.progress_data["performance_metrics"] = {
            "average_batch_time": 0,
            "symbols_per_minute": 0,
            "rate_limit_hits": 0,
            "recent_batch_durations": [],
        }

        # Add all symbols list to progress data for persistence
        # This is key to fixing the "missing symbols" problem
        self.progress_data["all_symbols"] = list(symbols_to_process)

        # Generate initial batch queue
        if generate_all_batches:
            # Calculate how many batches we need
            avg_batch_size = (self.min_batch_size + self.max_batch_size) // 2
            estimated_batch_count = len(symbols_to_process) // avg_batch_size + 1
            logger.info(f"Generating all {estimated_batch_count} batches upfront")
            self.generate_batches(estimated_batch_count)
        else:
            self.generate_batches(self.upcoming_batch_queue_size)

        # Save initial state
        self.save_progress()

        logger.info(
            f"Initialized new session with {len(symbols_to_process)} symbols to process"
        )
        return True

    def generate_batches(self, count: int = None) -> int:
        """
        Generate random batches from pending symbols.

        Args:
            count: Number of batches to generate, or None for queue_size

        Returns:
            Number of batches generated
        """
        # Default to filling the queue
        if count is None:
            current_queue_size = len(
                self.progress_data["batch_queue"]["upcoming_batches"]
            )
            count = max(0, self.upcoming_batch_queue_size - current_queue_size)

        if count <= 0:
            return 0

        pending_symbols = list(self.get_pending_symbols())

        # If no symbols are pending, can't generate batches
        if not pending_symbols:
            return 0

        # Track which symbols have been assigned to new batches
        assigned_symbols = set()
        batches_generated = 0

        # Get the next batch ID
        next_batch_id = self.progress_data["batch_queue"]["next_batch_id"]

        # If we're generating all batches at once, divide all pending symbols
        if count > len(pending_symbols) // self.min_batch_size:
            # We're trying to generate all batches
            logger.info(f"Dividing {len(pending_symbols)} symbols into batches")

            # Reset next_batch_id if we're generating all batches fresh
            if self.progress_data["batch_queue"]["completed_batch_count"] == 0:
                next_batch_id = 1
                self.progress_data["batch_queue"]["next_batch_id"] = 1
            else:
                next_batch_id = self.progress_data["batch_queue"]["next_batch_id"]

            # Shuffle symbols for randomness
            random.shuffle(pending_symbols)

            # Divide all symbols into batches of random size
            remaining_symbols = pending_symbols.copy()

            while remaining_symbols:
                # Determine batch size (random but within limits)
                # IMPORTANT FIX: Handle case where min_batch_size > remaining symbols
                if len(remaining_symbols) < self.min_batch_size:
                    # If fewer symbols remain than min_batch_size, use all remaining symbols
                    batch_size = len(remaining_symbols)
                else:
                    # Otherwise, use a random size between min and max (or remaining count)
                    batch_size = random.randint(
                        self.min_batch_size,
                        min(self.max_batch_size, len(remaining_symbols)),
                    )

                # Take the first batch_size symbols
                batch_symbols = remaining_symbols[:batch_size]
                remaining_symbols = remaining_symbols[batch_size:]

                # Create batch object
                batch = {
                    "batch_id": next_batch_id,
                    "size": len(batch_symbols),
                    "symbols": batch_symbols,
                    "status": "pending",
                }

                # Add to queue
                self.progress_data["batch_queue"]["upcoming_batches"].append(batch)
                next_batch_id += 1
                batches_generated += 1
                assigned_symbols.update(batch_symbols)

                # Log progress periodically
                if batches_generated % 100 == 0:
                    logger.info(f"Generated {batches_generated} batches so far")

        else:
            # Original code for generating a limited number of batches
            for _ in range(count):
                # Get symbols that haven't been assigned to a batch yet
                available_symbols = [
                    s for s in pending_symbols if s not in assigned_symbols
                ]

                # If no more available symbols, stop generating batches
                if not available_symbols:
                    break

                # Determine batch size - FIX THE SAME ISSUE HERE
                target_size = self.progress_data["batch_queue"]["batch_size_stats"][
                    "adaptive_target"
                ]

                # Handle case where min_batch_size > available symbols
                if len(available_symbols) < self.min_batch_size:
                    actual_size = len(available_symbols)
                else:
                    actual_size = min(
                        max(self.min_batch_size, target_size),
                        self.max_batch_size,
                        len(available_symbols),
                    )

                # Randomly select symbols
                batch_symbols = random.sample(available_symbols, actual_size)
                assigned_symbols.update(batch_symbols)

                # Create batch object
                batch = {
                    "batch_id": next_batch_id,
                    "size": actual_size,
                    "symbols": batch_symbols,
                    "status": "pending",
                }

                # Add to queue
                self.progress_data["batch_queue"]["upcoming_batches"].append(batch)
                next_batch_id += 1
                batches_generated += 1

        # Update next batch ID
        self.progress_data["batch_queue"]["next_batch_id"] = next_batch_id

        # Update batch size stats
        if batches_generated > 0:
            recent_sizes = self.progress_data["batch_queue"]["batch_size_stats"][
                "recent_sizes"
            ]
            sizes = [
                batch["size"]
                for batch in self.progress_data["batch_queue"]["upcoming_batches"][
                    -min(10, batches_generated) :
                ]
            ]
            recent_sizes.extend(sizes)
            if len(recent_sizes) > 10:  # Keep only the 10 most recent
                recent_sizes = recent_sizes[-10:]
            self.progress_data["batch_queue"]["batch_size_stats"][
                "recent_sizes"
            ] = recent_sizes

        logger.info(f"Generated {batches_generated} new batches")
        return batches_generated

    def get_next_batch(self) -> Optional[Dict[str, Any]]:
        """
        Get the next batch to process.

        Returns:
            Batch dictionary or None if no more batches
        """
        # **NEW: Check if session is already marked as completed**
        if self.progress_data["session_info"]["status"] == "completed":
            logger.info("Session already completed - no more batches to process")
            return None

        # If there's a current in-progress batch, return that
        if self.progress_data["batch_queue"]["current_batch"] is not None:
            return self.progress_data["batch_queue"]["current_batch"]

        # If no more upcoming batches, check if we actually need more
        if not self.progress_data["batch_queue"]["upcoming_batches"]:
            # Check if we've completed all symbols
            pending_symbols = self.get_pending_symbols()
            if not pending_symbols:
                # No more symbols to process - we're done!
                logger.info("No pending symbols and no more batches. Update complete.")
                self.progress_data["session_info"]["status"] = "completed"
                self.save_progress()
                return None

            # If we have pending symbols, try to create batches
            if len(pending_symbols) > 0:
                # If we have enough for a normal batch, generate normally
                if len(pending_symbols) >= self.min_batch_size:
                    if self.generate_batches() == 0:
                        logger.warning(
                            "No pending symbols could be converted to batches"
                        )
                        return None
                else:
                    # Create one final small batch with remaining symbols
                    logger.info(
                        f"Creating final batch with {len(pending_symbols)} symbols "
                        f"(below min_batch_size of {self.min_batch_size})"
                    )

                    next_batch_id = self.progress_data["batch_queue"]["next_batch_id"]

                    final_batch = {
                        "batch_id": next_batch_id,
                        "size": len(pending_symbols),
                        "symbols": list(pending_symbols),
                        "status": "pending",
                    }

                    self.progress_data["batch_queue"]["upcoming_batches"].append(
                        final_batch
                    )
                    self.progress_data["batch_queue"]["next_batch_id"] = (
                        next_batch_id + 1
                    )
                    self.save_progress()
            else:
                # Truly no pending symbols left
                logger.info("No pending symbols remaining. Update complete.")
                self.progress_data["session_info"]["status"] = "completed"
                self.save_progress()
                return None

        # Get the next batch from the queue
        if self.progress_data["batch_queue"]["upcoming_batches"]:
            next_batch = self.progress_data["batch_queue"]["upcoming_batches"].pop(0)
            self.progress_data["batch_queue"]["current_batch"] = next_batch
            return next_batch

        return None

    def mark_batch_started(self, batch_id: int) -> bool:
        """
        Mark a batch as started.

        Args:
            batch_id: ID of the batch to mark as started

        Returns:
            True if successful, False otherwise
        """
        current_batch = self.progress_data["batch_queue"]["current_batch"]

        if current_batch is None or current_batch["batch_id"] != batch_id:
            logger.error(
                f"Cannot mark batch {batch_id} as started: not the current batch"
            )
            return False

        current_batch["status"] = "in_progress"
        current_batch["start_time"] = datetime.now().isoformat()

        # Update last update time
        self.progress_data["session_info"][
            "last_update_time"
        ] = datetime.now().isoformat()
        self.progress_data["session_info"]["status"] = "in_progress"

        self.save_progress()

        logger.info(
            f"Started batch {batch_id} with {len(current_batch['symbols'])} symbols"
        )
        return True

    def update_performance_metrics(
        self, batch_duration: float, rate_limit_hit: bool = False
    ) -> None:
        """
        Update performance metrics after a batch completes.

        Args:
            batch_duration: Duration of the batch in seconds
            rate_limit_hit: Whether a rate limit was hit during the batch
        """
        metrics = self.progress_data["performance_metrics"]

        # Update recent batch durations
        recent_durations = metrics["recent_batch_durations"]
        recent_durations.append(batch_duration)
        if len(recent_durations) > 10:  # Keep only the 10 most recent
            recent_durations = recent_durations[-10:]
        metrics["recent_batch_durations"] = recent_durations

        # Update average batch time
        if recent_durations:
            metrics["average_batch_time"] = sum(recent_durations) / len(
                recent_durations
            )

        # Update symbols per minute
        current_batch = self.progress_data["batch_queue"]["current_batch"]
        if current_batch and batch_duration > 0:
            batch_size = current_batch.get(
                "size", len(current_batch.get("symbols", []))
            )
            symbols_per_minute = (batch_size / batch_duration) * 60

            # Smoothed update of overall rate
            if metrics["symbols_per_minute"] == 0:
                metrics["symbols_per_minute"] = symbols_per_minute
            else:
                # 70% old value, 30% new value for smoothing
                metrics["symbols_per_minute"] = (
                    0.7 * metrics["symbols_per_minute"]
                ) + (0.3 * symbols_per_minute)

        # Update rate limit counter
        if rate_limit_hit:
            metrics["rate_limit_hits"] += 1

        # Dynamic batch size adjustment based on performance
        if len(recent_durations) >= 3:
            # If recent batches are taking longer, reduce target size
            if batch_duration > 1.2 * metrics["average_batch_time"]:
                new_target = max(
                    self.min_batch_size,
                    int(
                        self.progress_data["batch_queue"]["batch_size_stats"][
                            "adaptive_target"
                        ]
                        * 0.9
                    ),
                )
                self.progress_data["batch_queue"]["batch_size_stats"][
                    "adaptive_target"
                ] = new_target
                logger.debug(
                    f"Reducing target batch size to {new_target} due to slower performance"
                )

            # If recent batches are faster, cautiously increase target size
            elif (
                batch_duration < 0.8 * metrics["average_batch_time"]
                and not rate_limit_hit
            ):
                new_target = min(
                    self.max_batch_size,
                    int(
                        self.progress_data["batch_queue"]["batch_size_stats"][
                            "adaptive_target"
                        ]
                        * 1.1
                    ),
                )
                self.progress_data["batch_queue"]["batch_size_stats"][
                    "adaptive_target"
                ] = new_target
                logger.debug(
                    f"Increasing target batch size to {new_target} due to good performance"
                )

    def save_progress(self) -> bool:
        """
        Save current progress to file using atomic write.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.progress_file_path), exist_ok=True)

            # Create a copy of progress data for saving
            save_data = self.progress_data.copy()

            # Optimize file size by removing symbol lists from batches if they're completed
            # This significantly reduces file size while keeping all necessary information
            for batch in save_data.get("batch_queue", {}).get("upcoming_batches", []):
                if batch.get("status") == "completed":
                    # For completed batches, we don't need the full symbol list
                    batch["symbols"] = []

            # Write to a temporary file first
            temp_file = f"{self.progress_file_path}.tmp"

            with open(temp_file, "w") as f:
                json.dump(save_data, f, indent=2)

            # Rename to the actual file (atomic operation)
            os.replace(temp_file, self.progress_file_path)

            return True

        except Exception as e:
            logger.error(f"Failed to save progress file: {str(e)}")
            return False

    def load_existing_progress(self) -> bool:
        """
        Load existing progress from file.

        Returns:
            bool: True if successfully loaded, False otherwise
        """
        try:
            with open(self.progress_file_path, "r") as f:
                loaded_data = json.load(f)

            # Validate basic structure
            required_keys = [
                "session_info",
                "symbol_status",
                "batch_queue",
                "performance_metrics",
            ]
            if not all(key in loaded_data for key in required_keys):
                logger.error("Progress file missing required keys, cannot load")
                return False

            self.progress_data = loaded_data

            # Rebuild internal tracking sets
            # First, ensure we have lists to work with
            if not isinstance(self.progress_data["symbol_status"]["completed"], list):
                self.progress_data["symbol_status"]["completed"] = []
            if not isinstance(self.progress_data["symbol_status"]["failed"], list):
                self.progress_data["symbol_status"]["failed"] = []

            # Clear and rebuild the _completed_symbols set
            self._completed_symbols = set(
                self.progress_data["symbol_status"]["completed"]
            )

            # Load failed symbols
            self._failed_symbols = set(
                symbol_info["symbol"]
                for symbol_info in self.progress_data["symbol_status"]["failed"]
            )

            # Load all symbols
            if "all_symbols" in self.progress_data:
                self._all_symbols = set(self.progress_data["all_symbols"])
                logger.info(
                    f"Loaded {len(self._all_symbols)} symbols from progress file"
                )
            else:
                # Extract all symbols from batches as before
                all_symbols = set()
                # Add symbols from batches, completed and failed symbols

                # Add completed and failed symbols
                all_symbols.update(self._completed_symbols)
                all_symbols.update(self._failed_symbols)

                # If we still don't have all symbols, try to make up the difference
                total_expected = self.progress_data["session_info"][
                    "total_symbols_to_process"
                ]
                if len(all_symbols) < total_expected:
                    # Warning about missing symbols
                    missing_count = total_expected - len(all_symbols)
                    logger.warning(
                        f"Could not find all expected symbols in progress file. "
                        f"Missing {missing_count} symbols."
                    )

                self._all_symbols = all_symbols

            # CRITICAL FIX: Use the count stored in session_info for completed symbols
            # instead of the length of the limited list
            if "completed_count" in self.progress_data["session_info"]:
                # If we have a stored count, use it
                completed_count = self.progress_data["session_info"]["completed_count"]
            else:
                # If no stored count (old format), calculate from batch data
                completed_count = self.progress_data["batch_queue"][
                    "completed_batch_count"
                ] * ((self.min_batch_size + self.max_batch_size) // 2)
                # Store the calculated count
                self.progress_data["session_info"]["completed_count"] = completed_count

            # Update the completed symbols count if it doesn't match our set
            # This happens because we only keep the most recent 1000 in the file
            if len(self._completed_symbols) < completed_count:
                logger.info(
                    f"Note: Only tracking {len(self._completed_symbols)} of {completed_count} "
                    f"completed symbols in memory (limited to recent 1000 in file)"
                )

            # Update failed count
            self.progress_data["session_info"]["failed_count"] = len(
                self._failed_symbols
            )

            # Update pending count
            pending_count = max(
                0, len(self._all_symbols) - completed_count - len(self._failed_symbols)
            )
            self.progress_data["session_info"]["pending_count"] = pending_count

            # Update percentage
            total = self.progress_data["session_info"]["total_symbols_to_process"]
            self.progress_data["session_info"]["completion_percentage"] = (
                (completed_count / total) * 100 if total > 0 else 0
            )

            logger.info(
                f"Loaded progress: {completed_count} completed, "
                f"{len(self._failed_symbols)} failed"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to load progress file: {str(e)}")
            return False

    # progress_tracker.py - REPLACE mark_batch_completed method

    def mark_batch_completed(self, batch_id: int, duration_seconds: float) -> bool:
        """Mark a batch as completed with FIXED completion logic."""
        current_batch = self.progress_data["batch_queue"]["current_batch"]

        if current_batch is None or current_batch["batch_id"] != batch_id:
            logger.error(
                f"Cannot mark batch {batch_id} as completed: not the current batch"
            )
            return False

        # Update batch info
        current_batch["status"] = "completed"
        current_batch["end_time"] = datetime.now().isoformat()
        current_batch["duration_seconds"] = duration_seconds

        # Count successes and failures
        batch_symbols = set(current_batch["symbols"])
        success_count = len(batch_symbols.intersection(self._completed_symbols))
        failure_count = len(batch_symbols.intersection(self._failed_symbols))

        current_batch["success_count"] = success_count
        current_batch["failure_count"] = failure_count

        # Update completed batch count
        self.progress_data["batch_queue"]["completed_batch_count"] += 1

        # Update performance metrics
        self.update_performance_metrics(duration_seconds)

        # Clear current batch
        self.progress_data["batch_queue"]["current_batch"] = None

        # Update last update time
        self.progress_data["session_info"][
            "last_update_time"
        ] = datetime.now().isoformat()

        # Recalculate pending count
        pending_count = max(
            0,
            len(self._all_symbols)
            - len(self._completed_symbols)
            - len(self._failed_symbols),
        )
        self.progress_data["session_info"]["pending_count"] = pending_count

        # Get actual pending symbols
        pending_symbols = self.get_pending_symbols()

        # FIXED COMPLETION LOGIC: Check actual state, not estimated batches
        has_upcoming_batches = (
            len(self.progress_data["batch_queue"]["upcoming_batches"]) > 0
        )

        # CRITICAL FIX: Only mark complete if there are ZERO pending symbols
        # If there are pending symbols < min_batch_size, we need to create a final small batch
        is_complete = (
            len(pending_symbols) == 0  # MUST be exactly zero
            and not has_upcoming_batches
            and self.progress_data["batch_queue"]["current_batch"] is None
        )

        # CRITICAL: If we have pending symbols but fewer than min_batch_size,
        # create a final small batch to ensure they get processed
        if (
            len(pending_symbols) > 0
            and len(pending_symbols) < self.min_batch_size
            and not has_upcoming_batches
            and self.progress_data["batch_queue"]["current_batch"] is None
        ):
            logger.info(
                f"Creating final small batch with {len(pending_symbols)} remaining symbols "
                f"(below min_batch_size of {self.min_batch_size})"
            )

            # Create one final batch with all remaining symbols
            next_batch_id = self.progress_data["batch_queue"]["next_batch_id"]

            final_batch = {
                "batch_id": next_batch_id,
                "size": len(pending_symbols),
                "symbols": list(pending_symbols),
                "status": "pending",
            }

            self.progress_data["batch_queue"]["upcoming_batches"].append(final_batch)
            self.progress_data["batch_queue"]["next_batch_id"] = next_batch_id + 1

            logger.info(
                f"Final batch {next_batch_id} created with {len(pending_symbols)} symbols"
            )
            self.save_progress()

        if is_complete:
            # Mark as completed
            self.progress_data["session_info"]["status"] = "completed"
            logger.info("All symbols processed. Update session complete!")
            logger.info(
                f"Final: {len(self._completed_symbols)} completed, {len(self._failed_symbols)} failed"
            )

            # Final notification handled by symbol_updater.run_update()
            self.save_progress()
            return True

        # NOT complete - log detailed status
        completed_batches = self.progress_data["batch_queue"]["completed_batch_count"]
        total_batches_in_queue = completed_batches + len(
            self.progress_data["batch_queue"]["upcoming_batches"]
        )
        if self.progress_data["batch_queue"]["current_batch"]:
            total_batches_in_queue += 1

        logger.info(
            f"Batch {batch_id} done: {success_count}✅/{failure_count}❌ | "
            f"Batches: {completed_batches}/{total_batches_in_queue} | "
            f"Pending symbols: {len(pending_symbols)} | "
            f"Queued batches: {len(self.progress_data['batch_queue']['upcoming_batches'])}"
        )

        # Generate more batches if queue is running low AND we have pending symbols
        if (
            len(self.progress_data["batch_queue"]["upcoming_batches"])
            < self.upcoming_batch_queue_size
        ):
            if len(pending_symbols) >= self.min_batch_size:
                logger.debug(
                    f"Generating more batches - {len(pending_symbols)} symbols still pending"
                )
                self.generate_batches()
            else:
                logger.debug(
                    f"Not generating batches - only {len(pending_symbols)} pending (< {self.min_batch_size})"
                )

        # Send periodic notifications
        if success_count > 0 or failure_count > 0:
            if completed_batches % 100 == 0:
                stats = self.get_summary_stats()
                send_telegram_notification(
                    f"✳️ Batch {batch_id} completed: {success_count}✅/{failure_count}❌\n"
                    f"Progress: {stats['completion_percentage']:.1f}%\n"
                    f"Batches: {completed_batches}/{total_batches_in_queue}\n"
                    f"Speed: {stats['symbols_per_minute']:.1f} symbols/min"
                )

        self.save_progress()
        return True

    def is_complete(self) -> bool:
        """
        Check if all symbols have been processed based on batches.

        Returns:
            True if all batches have been processed, False otherwise
        """
        # Check if we have any pending symbols
        pending_symbols = self.get_pending_symbols()

        # If we have no pending symbols, we should have no batches
        if len(pending_symbols) == 0:
            # If there are still batches, clear them
            if (
                self.progress_data["batch_queue"]["upcoming_batches"]
                or self.progress_data["batch_queue"]["current_batch"]
            ):
                logger.warning(
                    "No pending symbols but still have batches. Clearing queue."
                )
                self.progress_data["batch_queue"]["upcoming_batches"] = []
                self.progress_data["batch_queue"]["current_batch"] = None
                self.progress_data["session_info"]["status"] = "completed"
                self.save_progress()
            return True

        # Otherwise check if there are any pending batches
        if (
            self.progress_data["batch_queue"]["upcoming_batches"]
            or self.progress_data["batch_queue"]["current_batch"]
        ):
            return False

        # If no more pending symbols or batches, we're done
        return True

    def get_pending_symbols(self):
        """
        Get a list of symbols that still need processing.
        Ensures count is never negative.
        """
        # Get all symbols that haven't been processed yet
        pending = self._all_symbols - self._completed_symbols - self._failed_symbols

        # Safety check - if pending is mathematically negative, return empty set
        if len(pending) < 0:
            logger.warning(
                f"Detected invalid negative pending count ({len(pending)}). Resetting to empty set."
            )
            return set()

        return pending

    def mark_symbol_completed(
        self,
        symbol: str,
        data_points: int = None,
        first_date: str = None,
        last_date: str = None,
    ) -> bool:
        """
        Mark a symbol as successfully completed.
        """
        if symbol in self._completed_symbols:
            logger.debug(f"Symbol {symbol} already marked as completed")
            return True

        current_batch = self.progress_data["batch_queue"]["current_batch"]
        if current_batch is None or symbol not in current_batch["symbols"]:
            logger.error(f"Symbol {symbol} not in current batch")
            return False

        # Remove from failed symbols if it was there
        if symbol in self._failed_symbols:
            self._failed_symbols.remove(symbol)

            # Update the failed list in progress data too
            self.progress_data["symbol_status"]["failed"] = [
                f
                for f in self.progress_data["symbol_status"]["failed"]
                if f["symbol"] != symbol
            ]

        # Add to completed symbols set
        self._completed_symbols.add(symbol)

        # Add to completed list (with limited size to prevent file growth)
        completed_list = self.progress_data["symbol_status"]["completed"]

        # # Keep only the 1000 most recent completed symbols for the file
        # if len(completed_list) >= 1000:
        #     completed_list = completed_list[-999:]

        completed_list.append(symbol)
        self.progress_data["symbol_status"]["completed"] = completed_list

        # Update counts with safety check
        self.progress_data["session_info"]["completed_count"] = len(
            self._completed_symbols
        )

        # Calculate pending with safety check
        pending_count = max(
            0,
            len(self._all_symbols)
            - len(self._completed_symbols)
            - len(self._failed_symbols),
        )
        self.progress_data["session_info"]["pending_count"] = pending_count

        # Calculate completion percentage with safety check
        total = self.progress_data["session_info"]["total_symbols_to_process"]
        completed = self.progress_data["session_info"]["completed_count"]
        failed = self.progress_data["session_info"]["failed_count"]
        self.progress_data["session_info"]["completion_percentage"] = (
            ((completed + failed) / total) * 100 if total > 0 else 100
        )

        logger.debug(
            f"Marked symbol {symbol} as completed, total completed: {completed}"
        )
        return True

    def mark_symbol_failed(
        self, symbol: str, error: str, error_category: str = "unknown"
    ) -> bool:
        """
        Mark a symbol as failed with error details.
        """
        if symbol in self._failed_symbols:
            # Update error information for existing failed symbol
            for failed_info in self.progress_data["symbol_status"]["failed"]:
                if failed_info["symbol"] == symbol:
                    failed_info["error"] = error
                    failed_info["error_category"] = error_category
                    failed_info["attempts"] = failed_info.get("attempts", 0) + 1
                    failed_info["last_attempt_time"] = datetime.now().isoformat()
                    break
            logger.debug(f"Updated failed symbol {symbol} with new error info")
            return True

        current_batch = self.progress_data["batch_queue"]["current_batch"]
        if current_batch is None or symbol not in current_batch["symbols"]:
            logger.error(f"Symbol {symbol} not in current batch")
            return False

        # Add to failed symbols
        self._failed_symbols.add(symbol)

        # Add to failed list
        failed_info = {
            "symbol": symbol,
            "batch_id": current_batch["batch_id"],
            "error": error,
            "error_category": error_category,
            "attempts": 1,
            "last_attempt_time": datetime.now().isoformat(),
        }

        self.progress_data["symbol_status"]["failed"].append(failed_info)

        # Update counts with safety check
        self.progress_data["session_info"]["failed_count"] = len(self._failed_symbols)

        # Calculate pending with safety check
        pending_count = max(
            0,
            len(self._all_symbols)
            - len(self._completed_symbols)
            - len(self._failed_symbols),
        )
        self.progress_data["session_info"]["pending_count"] = pending_count

        # Calculate completion percentage with safety check
        total = self.progress_data["session_info"]["total_symbols_to_process"]
        completed = self.progress_data["session_info"]["completed_count"]
        failed = self.progress_data["session_info"]["failed_count"]
        self.progress_data["session_info"]["completion_percentage"] = (
            ((completed + failed) / total) * 100 if total > 0 else 100
        )

        logger.debug(f"Marked symbol {symbol} as failed: {error_category} - {error}")
        return True

    def get_summary_stats(self):
        """
        Get summary statistics about the current progress.
        Implements multiple safety checks for consistency.
        """
        # Get basic info with safety checks
        total_symbols = self.progress_data["session_info"]["total_symbols_to_process"]
        completed_symbols = len(self._completed_symbols)
        failed_symbols = len(self._failed_symbols)

        # Ensure we never have negative pending symbols
        pending_symbols = max(0, total_symbols - completed_symbols - failed_symbols)

        # If the math doesn't add up, log a warning and adjust
        expected_total = completed_symbols + failed_symbols + pending_symbols
        if expected_total != total_symbols:
            logger.warning(
                f"Symbol count discrepancy detected: Expected {total_symbols} but found {expected_total} "
                f"({completed_symbols} completed, {failed_symbols} failed, {pending_symbols} pending)"
            )

        # Calculate completion percentage safely
        completion_percentage = (
            ((completed_symbols + failed_symbols) / total_symbols) * 100
            if total_symbols > 0
            else 100
        )

        # Store the calculated values in the progress data for consistency
        self.progress_data["session_info"]["completed_count"] = completed_symbols
        self.progress_data["session_info"]["failed_count"] = failed_symbols
        self.progress_data["session_info"]["pending_count"] = pending_symbols
        self.progress_data["session_info"][
            "completion_percentage"
        ] = completion_percentage

        # Rest of your existing stats calculation...

        # Calculate batch-related metrics
        avg_batch_size = (self.min_batch_size + self.max_batch_size) // 2
        completed_batches = self.progress_data["batch_queue"]["completed_batch_count"]
        total_batches = (total_symbols + avg_batch_size - 1) // avg_batch_size

        # Calculate remaining time estimate
        symbols_per_minute = self.progress_data["performance_metrics"][
            "symbols_per_minute"
        ]
        estimated_minutes = (
            (pending_symbols / symbols_per_minute)
            if symbols_per_minute > 0 and pending_symbols > 0
            else 0
        )

        # Get error breakdown
        error_categories = {}
        for failed_info in self.progress_data["symbol_status"]["failed"]:
            category = failed_info.get("error_category", "unknown")
            error_categories[category] = error_categories.get(category, 0) + 1

        return {
            "total_symbols": total_symbols,
            "completed_symbols": completed_symbols,
            "failed_symbols": failed_symbols,
            "pending_symbols": pending_symbols,
            "completion_percentage": completion_percentage,
            "batches_completed": completed_batches,
            "current_batch_id": self.progress_data["batch_queue"]["next_batch_id"] - 1,
            "total_batches": total_batches,
            "estimated_minutes_remaining": estimated_minutes,
            "symbols_per_minute": self.progress_data["performance_metrics"][
                "symbols_per_minute"
            ],
            "rate_limit_hits": self.progress_data["performance_metrics"][
                "rate_limit_hits"
            ],
            "error_categories": error_categories,
            "status": self.progress_data["session_info"]["status"],
        }

    def verify_tracking_consistency(self):
        """
        Verify and fix any inconsistencies in progress tracking.
        Call this periodically to ensure tracking remains accurate.
        """
        # Get the actual counts
        completed_count = len(self._completed_symbols)
        failed_count = len(self._failed_symbols)
        total_expected = self.progress_data["session_info"]["total_symbols_to_process"]

        # Check for and remove any duplicates (symbols in both completed and failed)
        duplicates = self._completed_symbols.intersection(self._failed_symbols)
        if duplicates:
            logger.warning(
                f"Found {len(duplicates)} symbols in both completed and failed sets. Fixing..."
            )
            for symbol in duplicates:
                self._failed_symbols.discard(symbol)

            # Update failed list in progress data
            self.progress_data["symbol_status"]["failed"] = [
                f
                for f in self.progress_data["symbol_status"]["failed"]
                if f["symbol"] not in duplicates
            ]

        # Recalculate all counts
        self.progress_data["session_info"]["completed_count"] = completed_count
        self.progress_data["session_info"]["failed_count"] = failed_count
        pending_count = max(0, total_expected - completed_count - failed_count)
        self.progress_data["session_info"]["pending_count"] = pending_count

        # Update completion percentage
        self.progress_data["session_info"]["completion_percentage"] = (
            ((completed_count + failed_count) / total_expected) * 100
            if total_expected > 0
            else 100
        )

        # Log the verification results
        logger.info(
            f"Tracking consistency verified: {completed_count} completed, "
            f"{failed_count} failed, {pending_count} pending"
        )

        return True

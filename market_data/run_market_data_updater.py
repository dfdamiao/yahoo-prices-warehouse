#!/usr/bin/env python3
"""
Market Data Updater - Command Line Version
Converts market data from Jupyter notebook to standalone script
"""

import argparse
import atexit
import logging
import os
import sys
import time
import threading
from datetime import datetime

import psutil
from pathlib import Path


# Import centralized configuration
from market_data.settings import MARKET_DATA_DIR, LOGS_DIR

# Import your custom modules
from market_data._notify import send_telegram_notification
from market_data.symbol_utils import get_symbols_by_status
from market_data.symbol_updater import SymbolUpdater


class TerminalProgressDisplay:
    """Clean terminal progress display with colors"""

    def __init__(self):
        self.last_update = 0

    def display(self, stats: dict):
        """Display progress in terminal with colors and progress bar"""
        # Only update every 2 seconds to avoid screen flicker
        if time.time() - self.last_update < 2:
            return

        self.last_update = time.time()

        completed = stats["completed_symbols"]
        failed = stats["failed_symbols"]
        total = stats["total_symbols"]
        pending = total - completed - failed
        progress_pct = (completed + failed) / total * 100 if total > 0 else 0

        # Clear screen and move cursor to top
        os.system("clear" if os.name == "posix" else "cls")

        # Header
        print("=" * 60)
        print("🚀 MARKET DATA UPDATE PROGRESS".center(60))
        print("=" * 60)
        print()

        # Progress bar
        bar_width = 50
        filled = int(bar_width * progress_pct / 100)
        bar = "█" * filled + "░" * (bar_width - filled)
        print(f"Progress: [{bar}] {progress_pct:.1f}%")
        print()

        # Statistics grid
        print(f"{'Completed:':<12} {completed:>10,} ✅")
        print(f"{'Failed:':<12} {failed:>10,} ❌")
        print(f"{'Pending:':<12} {pending:>10,} ⏳")
        total = completed + failed
        success_rate = (completed / total * 100) if total > 0 else 0.0
        print(f"{'Success:':<12} {success_rate:>10.1f}%")
        print(f"{'Total:':<12} {total:>10,}")
        print()

        # Performance metrics
        speed = stats.get("symbols_per_minute", 0)
        print(f"Speed: {speed:.1f} symbols/min")

        if pending > 0 and speed > 0:
            eta_minutes = pending / speed
            eta_hours = int(eta_minutes // 60)
            eta_mins = int(eta_minutes % 60)
            print(f"ETA: ~{eta_hours}h {eta_mins}m")

        print()
        print("Press Ctrl+C to interrupt (progress will be saved)")
        print("-" * 60)


class BatteryMonitor:
    """Simple battery monitoring"""

    def __init__(self):
        self.last_check = 0
        self.last_notification = 0

    def check(self, force: bool = False) -> tuple:
        """Check battery status"""
        now = time.time()

        # Check every 5 minutes or if forced
        if not force and now - self.last_check < 300:
            return None, None

        self.last_check = now

        try:
            battery = psutil.sensors_battery()
            if battery:
                percent = battery.percent
                plugged = battery.power_plugged

                # Send notifications for low battery (max once per hour)
                if now - self.last_notification > 3600:
                    if percent < 20 and not plugged:
                        msg = f"⚠️ CRITICAL BATTERY: {percent}% - Plug in immediately!"
                        print(f"\n{msg}")
                        send_telegram_notification(msg)
                        self.last_notification = now
                    elif percent < 40 and not plugged:
                        msg = f"🔋 LOW BATTERY: {percent}% - Consider plugging in"
                        print(f"\n{msg}")
                        send_telegram_notification(msg)
                        self.last_notification = now

                return percent, plugged

        except Exception as e:
            logging.debug(f"Battery check failed: {e}")

        return None, None


class MarketDataUpdaterCLI:
    """Command line interface for market data updater"""

    def __init__(self, market_data_dir: str, **kwargs):
        self.market_data_dir = market_data_dir
        self.progress_display = TerminalProgressDisplay()
        self.battery_monitor = BatteryMonitor()

        # Initialize updater
        self.updater = SymbolUpdater(market_data_dir=market_data_dir, **kwargs)

        logging.info(f"Initialized updater for {market_data_dir}")

    def run_with_monitoring(self) -> dict:
        """Run update with live monitoring"""
        result = {"stats": None, "error": None}

        def update_thread():
            try:
                result["stats"] = self.updater.run_update()
            except Exception as e:
                result["error"] = str(e)
                logging.error(f"Update thread error: {e}")

        # Start update in background thread
        thread = threading.Thread(target=update_thread, daemon=True)
        thread.start()

        # Monitor progress
        try:
            while thread.is_alive():
                # Update display
                stats = self.updater.get_update_status()
                self.progress_display.display(stats)

                # Check battery
                self.battery_monitor.check()

                # Wait before next update
                time.sleep(2)

        except KeyboardInterrupt:
            logging.info("Received interrupt signal")
            print("\n⏸️ Interrupt received - stopping gracefully...")
            print("Progress has been saved and can be resumed later.")

            # Give the thread a moment to finish current batch
            thread.join(timeout=10)

            # Get final stats
            try:
                final_stats = self.updater.get_update_status()
                print("\nFinal Status:")
                print(f"  Completed: {final_stats['completed_symbols']:,}")
                print(f"  Failed: {final_stats['failed_symbols']:,}")
                print(f"  Progress: {final_stats['completion_percentage']:.1f}%")

                send_telegram_notification(
                    f"⏸️ Update interrupted\n"
                    f"Progress: {final_stats['completion_percentage']:.1f}%\n"
                    f"Completed: {final_stats['completed_symbols']:,}"
                )
            except Exception as e:
                logging.error(f"Error getting final stats: {e}")

            return {"interrupted": True}

        # Check for errors
        if result["error"]:
            raise Exception(result["error"])

        return result["stats"]

    def resume_existing(self) -> dict:
        """Resume an existing update"""
        print("🔄 Attempting to resume previous update...")

        if not self.updater.resume_update():
            raise Exception("Failed to resume existing update")

        print("✅ Successfully loaded previous progress")

        # Show current status
        stats = self.updater.get_update_status()
        print(f"Current progress: {stats['completion_percentage']:.1f}%")
        print(f"Completed: {stats['completed_symbols']:,}")
        print(f"Failed: {stats['failed_symbols']:,}")
        print(
            f"Pending: {stats['total_symbols'] - stats['completed_symbols'] - stats['failed_symbols']:,}"
        )

        send_telegram_notification("🔄 Resuming market data update")

        return self.run_with_monitoring()

    def start_new_update(self, status: str = "complete") -> dict:
        """Start a new update"""
        print(f"🆕 Starting new update for symbols with status: {status}")

        # Get symbols to update
        symbols = get_symbols_by_status(status=status)

        if not symbols:
            raise Exception(f"No symbols found with status '{status}'")

        print(f"📊 Found {len(symbols):,} symbols to update")

        # Prepare update
        if not self.updater.prepare_update(symbols=symbols, generate_all_batches=True):
            raise Exception("Failed to prepare update")

        print("✅ Update prepared successfully")

        send_telegram_notification(
            f"🚀 Starting new market data update\n"
            f"Symbols: {len(symbols):,}\n"
            f"Status filter: {status}"
        )

        return self.run_with_monitoring()


def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO

    # Create logs directory (uses config.py for portable path)
    log_dir = LOGS_DIR
    os.makedirs(log_dir, exist_ok=True)

    # Setup logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"market_updater_{timestamp}.log")

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )

    print(f"📝 Logging to: {log_file}")
    return log_file


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Market Data Updater - Download and update financial data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python market_data_updater.py --resume          # Resume previous update
    python market_data_updater.py --new             # Start new update
    python market_data_updater.py --new --status active  # Update only active symbols
    python market_data_updater.py --new --period 3mo     # Use 3-month update period
        """,
    )

    # Action arguments (mutually exclusive)
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument(
        "--resume", action="store_true", help="Resume previous interrupted update"
    )
    action_group.add_argument("--new", action="store_true", help="Start new update")

    # Configuration arguments
    parser.add_argument(
        "--market-data-dir",
        default=MARKET_DATA_DIR,
        help=f"Market data directory (default: {MARKET_DATA_DIR})",
    )
    parser.add_argument(
        "--status",
        default="complete",
        help="Symbol status filter for new updates (default: complete)",
    )
    parser.add_argument("--period", default="1mo", help="Update period (default: 1mo)")
    parser.add_argument(
        "--min-batch", type=int, default=150, help="Minimum batch size (default: 100)"
    )
    parser.add_argument(
        "--max-batch", type=int, default=150, help="Maximum batch size (default: 150)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Setup logging
    log_file = setup_logging(args.verbose)

    # Write PID file so cli/ helpers can detect an active updater run.
    pid_file = Path(args.market_data_dir) / ".updater.pid"
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text(str(os.getpid()))
    atexit.register(lambda: pid_file.unlink(missing_ok=True))

    try:
        print("🚀 Market Data Updater Starting...")
        print(f"📁 Market Data Directory: {args.market_data_dir}")

        # OPTIMIZE SYSTEM FIRST
        print("🔧 Optimizing system settings...")
        from market_data.symbol_updater import optimize_system_for_updates

        if optimize_system_for_updates():
            print("✅ System optimization completed")
        else:
            print("⚠️ System optimization had issues - continuing anyway")

        # Initialize CLI with adjusted parameters for your system
        cli = MarketDataUpdaterCLI(
            market_data_dir=args.market_data_dir,
            min_batch_size=args.min_batch,  # Keep your 100-150
            max_batch_size=args.max_batch,  # Keep your 100-150
            update_period=args.period,
            enable_resource_monitoring=False,  # Enable resource monitoring
        )

        # Execute requested action
        if args.resume:
            final_stats = cli.resume_existing()
        else:  # args.new
            final_stats = cli.start_new_update(status=args.status)

        # Handle results
        if final_stats.get("interrupted"):
            print("\n⏸️ Update was interrupted but progress saved")
            sys.exit(1)

        # Success!
        print("\n" + "=" * 60)
        print("✅ UPDATE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"Completed: {final_stats['completed_symbols']:,}")
        print(f"Failed: {final_stats['failed_symbols']:,}")
        print(f"Success Rate: {final_stats['completion_percentage']:.1f}%")
        print(f"Speed: {final_stats['symbols_per_minute']:.1f} symbols/min")

        # Final notification already sent by symbol_updater.run_update()

    except KeyboardInterrupt:
        print("\n⏸️ Interrupted by user")
        send_telegram_notification("⏸️ Market data update interrupted by user")
        sys.exit(1)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        logging.error(f"Main execution error: {e}", exc_info=True)
        send_telegram_notification(f"❌ Market data update failed: {e}")
        sys.exit(1)

    finally:
        print(f"\n📝 Full logs available at: {log_file}")


if __name__ == "__main__":
    main()

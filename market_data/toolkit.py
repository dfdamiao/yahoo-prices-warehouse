#!/usr/bin/env python3
"""Database Toolkit - maintenance for the ArcticDB/LMDB market-data warehouse.

Analyze, diagnose, test, check, repair, and optimize the local store this
warehouse builds. Runs against the same ArcticDB libraries the updater writes.

USAGE:
    # Analysis
    python -m market_data.toolkit analyze  --mode storage|metadata|symbols|distribution [--db-path <uri>]

    # Diagnostics
    python -m market_data.toolkit diagnose --mode status|damage [--db-path <uri>]

    # Tests
    python -m market_data.toolkit test     --mode connection|validation [--db-path <uri>]

    # Health check
    python -m market_data.toolkit check    --mode lmdb [--db-path <uri>]

    # Repair
    python -m market_data.toolkit repair   --mode locks [--fix] [--force] [--db-path <uri>]
    python -m market_data.toolkit repair   --mode lmdb  [--output-dir <path>] [--db-path <uri>]

    # Optimize
    python -m market_data.toolkit optimize --mode prune   [--dry-run] [--db-path <uri>]
    python -m market_data.toolkit optimize --mode compact [--output-dir <path>] [--db-path <uri>]

If --db-path is omitted, the path from your config.json (settings.DB_PATH) is used.

NOTE: `repair --mode lmdb` and `optimize --mode compact` shell out to the LMDB
command-line utilities `mdb_copy` / `mdb_stat` (install the system package
`lmdb-utils` on Debian/Ubuntu or `lmdb` via Homebrew on macOS). Every other
command is pure Python + ArcticDB.
"""

import os
import sys
import argparse
import logging
import subprocess
import shutil
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List

# Add parent directory to path for config imports
sys.path.insert(0, str(Path(__file__).parent.parent))


# Warehouse settings supply the default DB path + data dir.
from market_data import settings

# Optional imports for database operations
try:
    import arcticdb as adb
    from market_data.manager import MarketDataManager
    HAS_ARCTICDB = True
except ImportError:
    HAS_ARCTICDB = False
    adb = None
    MarketDataManager = None

# Optional import for process checking
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DatabaseAnalyzer:
    """Unified database analysis and diagnostics"""

    def __init__(self, db_path: str = None, market_data_dir: str = None):
        """
        Initialize analyzer with database path.

        Args:
            db_path: Full LMDB path (e.g., lmdb:///path/to/db)
            market_data_dir: Directory path (will be converted to lmdb:// path)
        """
        if market_data_dir:
            self.market_data_dir = market_data_dir
            self.db_path = f"lmdb:///{market_data_dir}"
        elif db_path:
            self.db_path = db_path
            self.market_data_dir = db_path.replace("lmdb:///", "").replace("lmdb://", "")
        else:
            # Use config defaults
            self.db_path = settings.DB_PATH
            self.market_data_dir = settings.MARKET_DATA_DIR

        self.physical_path = self.db_path.replace("lmdb:///", "").replace("lmdb://", "")
        logger.info(f"Database: {self.db_path}")

    def _check_arcticdb(self):
        """Check if arcticdb is available"""
        if not HAS_ARCTICDB:
            logger.error("ArcticDB is not installed. Install with: pip install arcticdb")
            raise ImportError("arcticdb module is required for this operation")

    def analyze_storage(self) -> Dict:
        """Analyze physical storage and library sizes"""
        self._check_arcticdb()
        logger.info("\n=== STORAGE ANALYSIS ===")

        path = Path(self.physical_path)
        if not path.exists():
            logger.error(f"Database path does not exist: {path}")
            return {"error": "Database not found"}

        # Calculate directory size
        total_size = 0
        file_count = 0
        file_breakdown = {}

        for root, dirs, files in os.walk(path):
            for file in files:
                file_path = Path(root) / file
                file_size = file_path.stat().st_size
                total_size += file_size
                file_count += 1
                ext = file_path.suffix or "no_extension"
                file_breakdown[ext] = file_breakdown.get(ext, 0) + file_size

        total_gb = total_size / (1024**3)
        logger.info(f"Total Size: {total_gb:.2f} GB ({file_count:,} files)")

        # Analyze libraries
        try:
            mdm = MarketDataManager.get_instance(db_path=self.db_path)

            libraries = {
                "symbol_metadata": mdm.symbol_metadata_lib,
                "price_history.daily": mdm.price_history_lib,
                "price_history.metadata": mdm.price_metadata_lib,
            }

            for lib_name, lib in libraries.items():
                symbols = list(lib.list_symbols())
                logger.info(f"  {lib_name}: {len(symbols):,} symbols")

            mdm.close()

        except Exception as e:
            logger.error(f"Error analyzing libraries: {e}")

        return {
            "total_size_gb": total_gb,
            "file_count": file_count,
            "file_breakdown": file_breakdown,
        }

    def analyze_metadata(self, export_html: bool = False) -> Dict:
        """Analyze metadata across all symbols"""
        self._check_arcticdb()
        logger.info("\n=== METADATA ANALYSIS ===")

        try:
            ac = adb.Arctic(self.db_path)

            price_symbols = set(ac["price_history.daily"].list_symbols())
            metadata_symbols = set(ac["price_history.metadata"].list_symbols())

            logger.info(f"Price history symbols: {len(price_symbols):,}")
            logger.info(f"Metadata symbols: {len(metadata_symbols):,}")

            # Analyze distribution
            complete = len(price_symbols & metadata_symbols)
            price_only = len(price_symbols - metadata_symbols)
            metadata_only = len(metadata_symbols - price_symbols)

            logger.info(f"  Complete (both): {complete:,}")
            logger.info(f"  Price only: {price_only:,}")
            logger.info(f"  Metadata only: {metadata_only:,}")

            results = {
                "price_symbols": len(price_symbols),
                "metadata_symbols": len(metadata_symbols),
                "complete": complete,
                "price_only": price_only,
                "metadata_only": metadata_only,
            }

            if export_html:
                self._export_html_report(results)

            return results

        except Exception as e:
            logger.error(f"Error analyzing metadata: {e}")
            return {"error": str(e)}

    def analyze_symbols(self) -> Dict:
        """Analyze symbol consistency across libraries"""
        self._check_arcticdb()
        logger.info("\n=== SYMBOL ANALYSIS ===")

        try:
            ac = adb.Arctic(self.db_path)

            symbol_meta_lib = ac["symbol_metadata"]
            price_lib = ac["price_history.daily"]
            metadata_lib = ac["price_history.metadata"]

            # Get all symbols
            symbol_meta = set(symbol_meta_lib.list_symbols())
            price_symbols = set(price_lib.list_symbols())
            metadata_symbols = set(metadata_lib.list_symbols())

            # Find inconsistencies
            all_symbols = symbol_meta | price_symbols | metadata_symbols

            issues = {
                "no_price_data": list(metadata_symbols - price_symbols),
                "no_metadata": list(price_symbols - metadata_symbols),
                "no_symbol_meta": list(price_symbols - symbol_meta),
            }

            logger.info(f"Total unique symbols: {len(all_symbols):,}")
            logger.info(f"Symbols with no price data: {len(issues['no_price_data']):,}")
            logger.info(f"Symbols with no metadata: {len(issues['no_metadata']):,}")
            logger.info(f"Symbols with no symbol_meta: {len(issues['no_symbol_meta']):,}")

            return {
                "total_symbols": len(all_symbols),
                "issues": issues,
            }

        except Exception as e:
            logger.error(f"Error analyzing symbols: {e}")
            return {"error": str(e)}

    def analyze_distribution(self, min_points: int = 50) -> Dict:
        """Analyze data point distribution"""
        self._check_arcticdb()
        logger.info(f"\n=== DISTRIBUTION ANALYSIS (min {min_points} points) ===")

        try:
            mdm = MarketDataManager.get_instance(db_path=self.db_path)

            price_symbols = list(mdm.price_history_lib.list_symbols())

            distribution = {
                "0_points": 0,
                "1-50_points": 0,
                "51-250_points": 0,
                "251-500_points": 0,
                "500+_points": 0,
            }

            insufficient = []

            for symbol in price_symbols:
                try:
                    data = mdm.price_history_lib.read(symbol).data
                    points = len(data) if data is not None else 0

                    if points == 0:
                        distribution["0_points"] += 1
                    elif points <= 50:
                        distribution["1-50_points"] += 1
                    elif points <= 250:
                        distribution["51-250_points"] += 1
                    elif points <= 500:
                        distribution["251-500_points"] += 1
                    else:
                        distribution["500+_points"] += 1

                    if points < min_points:
                        insufficient.append({"symbol": symbol, "points": points})

                except Exception as e:
                    logger.debug(f"Error reading {symbol}: {e}")

            for category, count in distribution.items():
                pct = (count / len(price_symbols) * 100) if price_symbols else 0
                logger.info(f"  {category}: {count:,} ({pct:.1f}%)")

            logger.info(f"\nInsufficient data (<{min_points} points): {len(insufficient):,}")

            mdm.close()

            return {
                "distribution": distribution,
                "insufficient_count": len(insufficient),
                "insufficient_symbols": insufficient[:10],  # First 10
            }

        except Exception as e:
            logger.error(f"Error analyzing distribution: {e}")
            return {"error": str(e)}

    def diagnose_status(self) -> Dict:
        """Diagnose symbol status issues"""
        self._check_arcticdb()
        logger.info("\n=== STATUS DIAGNOSTICS ===")

        try:
            ac = adb.Arctic(self.db_path)

            metadata_lib = ac["price_history.metadata"]

            metadata_symbols = set(metadata_lib.list_symbols())

            # Check for status issues
            statuses = {"complete": 0, "stale": 0, "failed": 0, "unknown": 0}
            stale_symbols = []

            for symbol in metadata_symbols:
                try:
                    meta = metadata_lib.read(symbol).data
                    if not meta.empty and "data_status" in meta.columns:
                        status = meta.iloc[0]["data_status"]
                        statuses[status] = statuses.get(status, 0) + 1

                        if status == "stale":
                            stale_symbols.append(symbol)
                    else:
                        statuses["unknown"] += 1
                except Exception:
                    statuses["unknown"] += 1

            logger.info("Symbol statuses:")
            for status, count in statuses.items():
                logger.info(f"  {status}: {count:,}")

            logger.info(f"\nStale symbols: {len(stale_symbols):,}")
            if stale_symbols[:5]:
                logger.info(f"  Examples: {', '.join(stale_symbols[:5])}")

            return {
                "statuses": statuses,
                "stale_count": len(stale_symbols),
                "stale_symbols": stale_symbols[:10],
            }

        except Exception as e:
            logger.error(f"Error diagnosing status: {e}")
            return {"error": str(e)}

    def diagnose_damage(self) -> Dict:
        """Assess database damage/corruption"""
        self._check_arcticdb()
        logger.info("\n=== DAMAGE ASSESSMENT ===")

        try:
            ac = adb.Arctic(self.db_path)

            libraries = ac.list_libraries()
            results = {"healthy": [], "issues": []}

            for lib_name in libraries:
                try:
                    lib = ac[lib_name]
                    symbols = lib.list_symbols()

                    # Try reading a sample
                    if symbols:
                        sample = symbols[0]
                        lib.read(sample)

                    results["healthy"].append(lib_name)
                    logger.info(f"  {lib_name}: ✓ Healthy ({len(symbols)} symbols)")

                except Exception as e:
                    results["issues"].append({"library": lib_name, "error": str(e)})
                    logger.error(f"  {lib_name}: ✗ Issue - {e}")

            logger.info(f"\nHealthy libraries: {len(results['healthy'])}/{len(libraries)}")
            logger.info(f"Libraries with issues: {len(results['issues'])}")

            return results

        except Exception as e:
            logger.error(f"Error assessing damage: {e}")
            return {"error": str(e)}

    def test_connection(self) -> Dict:
        """Test database connectivity"""
        self._check_arcticdb()
        logger.info("\n=== CONNECTION TEST ===")

        try:
            ac = adb.Arctic(self.db_path)
            logger.info(f"✓ Connected to: {self.db_path}")

            libraries = ac.list_libraries()
            logger.info(f"✓ Found libraries: {libraries}")

            results = {"connected": True, "libraries": {}}

            for lib_name in libraries:
                try:
                    lib = ac[lib_name]
                    symbols = lib.list_symbols()
                    results["libraries"][lib_name] = len(symbols)
                    logger.info(f"  {lib_name}: {len(symbols):,} symbols")
                except Exception as e:
                    results["libraries"][lib_name] = f"Error: {e}"
                    logger.error(f"  {lib_name}: Error - {e}")

            return results

        except Exception as e:
            logger.error(f"✗ Connection failed: {e}")
            return {"connected": False, "error": str(e)}

    def test_validation(self) -> Dict:
        """Validate database relationships"""
        self._check_arcticdb()
        logger.info("\n=== VALIDATION TEST ===")

        try:
            ac = adb.Arctic(self.db_path)

            symbol_lib = ac["symbol_metadata"]
            price_lib = ac["price_history.daily"]

            # Get symbols_metadata if exists
            try:
                symbols_metadata = symbol_lib.read("symbols_metadata").data
                meta_count = len(symbols_metadata)
            except Exception:
                symbols_metadata = None
                meta_count = 0

            price_symbols = set(price_lib.list_symbols())

            logger.info(f"Symbols in metadata: {meta_count:,}")
            logger.info(f"Symbols with price data: {len(price_symbols):,}")

            # Check consistency
            issues = []
            if symbols_metadata is not None:
                meta_symbols = set(symbols_metadata["symbol"].values)
                orphaned = price_symbols - meta_symbols
                missing = meta_symbols - price_symbols

                if orphaned:
                    issues.append(f"{len(orphaned)} symbols have price data but no metadata entry")
                if missing:
                    issues.append(f"{len(missing)} symbols in metadata but no price data")

            if issues:
                logger.warning("Validation issues found:")
                for issue in issues:
                    logger.warning(f"  - {issue}")
            else:
                logger.info("✓ No validation issues found")

            return {
                "valid": len(issues) == 0,
                "issues": issues,
            }

        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return {"valid": False, "error": str(e)}

    def _find_all_lmdb_databases(self, db_path: str) -> List[Dict]:
        """Find all LMDB database directories."""
        lmdb_databases = []

        for root, dirs, files in os.walk(db_path):
            # Check if this directory contains LMDB files
            has_data_mdb = "data.mdb" in files
            has_lock_mdb = "lock.mdb" in files

            if has_data_mdb:
                lmdb_databases.append(
                    {
                        "path": root,
                        "has_data": has_data_mdb,
                        "has_lock": has_lock_mdb,
                        "data_size": os.path.getsize(os.path.join(root, "data.mdb"))
                        if has_data_mdb
                        else 0,
                        "lock_size": os.path.getsize(os.path.join(root, "lock.mdb"))
                        if has_lock_mdb
                        else 0,
                    }
                )

        return lmdb_databases

    def _test_lmdb_database(self, lmdb_info: Dict, mdb_stat_path: str) -> Dict:
        """Test if an LMDB database is readable."""
        lmdb_path = lmdb_info["path"]

        try:
            # Use mdb_stat to check database health
            result = subprocess.run(
                [mdb_stat_path, lmdb_path],
                capture_output=True,
                text=True,
                timeout=30,
            )

            if result.returncode == 0:
                # Parse some basic stats
                stats = {}
                for line in result.stdout.split("\n"):
                    if ":" in line:
                        key, value = line.split(":", 1)
                        stats[key.strip()] = value.strip()

                return {"status": "healthy", "stats": stats}
            else:
                return {"status": "corrupted", "error": result.stderr.strip()}

        except subprocess.TimeoutExpired:
            return {"status": "timeout", "error": "mdb_stat timed out"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def check_lmdb(self) -> Dict:
        """Check LMDB database health"""
        logger.info("\n=== LMDB HEALTH CHECK ===")

        # Find mdb_stat tool
        mdb_stat_path = shutil.which("mdb_stat")
        if not mdb_stat_path:
            # Try common conda location
            conda_path = "/opt/miniconda3/envs/quant/bin/mdb_stat"
            if os.path.exists(conda_path):
                mdb_stat_path = conda_path
            else:
                logger.error("❌ mdb_stat not found. Install LMDB tools.")
                return {"error": "mdb_stat not found"}

        logger.info(f"🔍 Checking LMDB database health in: {self.physical_path}")
        logger.info(f"Using mdb_stat: {mdb_stat_path}")

        # Find all LMDB databases
        lmdb_databases = self._find_all_lmdb_databases(self.physical_path)

        logger.info(f"Found {len(lmdb_databases)} LMDB databases")

        healthy_count = 0
        corrupted_count = 0
        results = []

        for i, lmdb_info in enumerate(lmdb_databases):
            logger.info(
                f"\n📁 [{i+1}/{len(lmdb_databases)}] Testing: {lmdb_info['path']}"
            )
            logger.info(f"   Data size: {lmdb_info['data_size']:,} bytes")
            logger.info(f"   Lock file: {'✅' if lmdb_info['has_lock'] else '❌'}")

            # Test the database
            test_result = self._test_lmdb_database(lmdb_info, mdb_stat_path)

            result = {
                "path": lmdb_info["path"],
                "relative_path": os.path.relpath(lmdb_info["path"], self.physical_path),
                "data_size": lmdb_info["data_size"],
                "has_lock": lmdb_info["has_lock"],
                "test_result": test_result,
            }

            if test_result["status"] == "healthy":
                logger.info("   Status: ✅ HEALTHY")
                healthy_count += 1
            else:
                logger.info(f"   Status: ❌ {test_result['status'].upper()}")
                if "error" in test_result:
                    logger.info(f"   Error: {test_result['error'][:100]}")
                corrupted_count += 1

            results.append(result)

        # Summary
        logger.info("\n📊 LMDB HEALTH SUMMARY:")
        logger.info(f"   ✅ Healthy databases: {healthy_count}")
        logger.info(f"   ❌ Corrupted databases: {corrupted_count}")
        logger.info(f"   📁 Total databases: {len(lmdb_databases)}")

        corruption_rate = (
            (corrupted_count / len(lmdb_databases)) * 100 if lmdb_databases else 0
        )
        logger.info(f"   💥 Corruption rate: {corruption_rate:.1f}%")

        # Show corrupted databases
        if corrupted_count > 0:
            logger.info("\n❌ CORRUPTED DATABASES:")
            for result in results:
                if result["test_result"]["status"] != "healthy":
                    logger.info(
                        f"   {result['relative_path']} - {result['test_result']['status']}"
                    )

        # Recommendation
        if corruption_rate == 0:
            recommendation = "✅ All LMDB databases are healthy! The issue might be at the ArcticDB layer."
        elif corruption_rate < 10:
            recommendation = f"⚠️ {corruption_rate:.1f}% corruption - REPAIR might work, but BACKUP RESTORE is safer"
        elif corruption_rate < 50:
            recommendation = f"❌ {corruption_rate:.1f}% corruption - BACKUP RESTORE strongly recommended"
        else:
            recommendation = (
                f"🚨 {corruption_rate:.1f}% corruption - BACKUP RESTORE required"
            )

        logger.info(f"\n🎯 RECOMMENDATION: {recommendation}")

        return {
            "healthy_count": healthy_count,
            "corrupted_count": corrupted_count,
            "total_count": len(lmdb_databases),
            "corruption_rate": corruption_rate,
            "recommendation": recommendation,
            "results": results,
        }

    def _find_arctic_processes(self) -> List[Dict]:
        """Find running processes that might be using ArcticDB."""
        if not HAS_PSUTIL:
            logger.warning("psutil not available - cannot check for running processes")
            return []

        arctic_processes = []

        for proc in psutil.process_iter(["pid", "name", "cmdline", "create_time"]):
            try:
                cmdline = " ".join(proc.info["cmdline"]) if proc.info["cmdline"] else ""

                # Look for processes that might be using ArcticDB
                if any(
                    keyword in cmdline.lower()
                    for keyword in [
                        "arcticdb",
                        "arctic",
                        "market_data",
                        "optimize_arcticdb",
                        "lmdb",
                        "trading",
                        "backtest",
                    ]
                ):
                    arctic_processes.append(
                        {
                            "pid": proc.info["pid"],
                            "name": proc.info["name"],
                            "cmdline": cmdline,
                            "create_time": datetime.fromtimestamp(
                                proc.info["create_time"]
                            ),
                            "age_hours": (time.time() - proc.info["create_time"])
                            / 3600,
                        }
                    )
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

        return arctic_processes

    def _find_stale_locks(self, stale_threshold_hours: int = 24) -> List[Dict]:
        """Find stale lock files in ArcticDB storage."""
        stale_locks = []
        base_path = Path(self.physical_path)

        for lock_file in base_path.rglob("lock.mdb"):
            try:
                stat = lock_file.stat()
                age_seconds = time.time() - stat.st_mtime
                age_hours = age_seconds / 3600
                age_days = age_hours / 24

                if age_hours > stale_threshold_hours:
                    rel_path = lock_file.relative_to(base_path)
                    library_path = str(rel_path.parent)

                    stale_locks.append(
                        {
                            "path": str(lock_file),
                            "library": library_path,
                            "age_hours": age_hours,
                            "age_days": age_days,
                            "size_bytes": stat.st_size,
                            "modified": datetime.fromtimestamp(stat.st_mtime),
                        }
                    )
            except Exception as e:
                logger.warning(f"Error checking {lock_file}: {e}")

        return stale_locks

    def _check_lock_files_in_use(self, lock_files: List[str]) -> Dict[str, bool]:
        """Check if lock files are in use by running processes."""
        if not HAS_PSUTIL:
            logger.warning("psutil not available - assuming all locks may be in use")
            return {lock: True for lock in lock_files}

        in_use = {}

        for lock_file in lock_files:
            try:
                for proc in psutil.process_iter(["pid", "open_files"]):
                    try:
                        if proc.info["open_files"]:
                            for file in proc.info["open_files"]:
                                if file.path == lock_file:
                                    in_use[lock_file] = True
                                    break
                        if lock_file in in_use:
                            break
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue

                if lock_file not in in_use:
                    in_use[lock_file] = False

            except Exception:
                in_use[lock_file] = True

        return in_use

    def repair_locks(
        self, stale_hours: int = 24, fix: bool = False, force: bool = False
    ) -> Dict:
        """Check and optionally repair stale ArcticDB locks."""
        self._check_arcticdb()
        logger.info("\n=== LOCK REPAIR ===")
        logger.info(f"Database: {self.physical_path}")
        logger.info(f"Stale threshold: {stale_hours} hours")

        # Step 1: Check for running processes
        logger.info("\n=== Step 1: Checking for running ArcticDB processes ===")
        arctic_processes = self._find_arctic_processes()

        if arctic_processes:
            logger.warning(
                f"Found {len(arctic_processes)} potentially relevant processes:"
            )
            for proc in arctic_processes:
                logger.warning(
                    f"  PID {proc['pid']}: {proc['name']} (age: {proc['age_hours']:.1f}h)"
                )
                logger.warning(f"    Command: {proc['cmdline'][:100]}...")
        else:
            logger.info("No ArcticDB-related processes found")

        # Step 2: Find stale locks
        logger.info(f"\n=== Step 2: Looking for stale locks (>{stale_hours}h old) ===")
        stale_locks = self._find_stale_locks(stale_hours)

        if stale_locks:
            logger.warning(f"Found {len(stale_locks)} stale lock files:")
            for lock in sorted(stale_locks, key=lambda x: x["age_days"], reverse=True):
                logger.warning(f"  {lock['library']}: {lock['age_days']:.1f} days old")
                logger.warning(f"    Path: {lock['path']}")
                logger.warning(f"    Modified: {lock['modified']}")
        else:
            logger.info("No stale locks found")

        # Step 3: Test connectivity
        logger.info("\n=== Step 3: Testing ArcticDB connectivity ===")
        try:
            ac = adb.Arctic(self.db_path)
            libraries = ac.list_libraries()
            logger.info(f"✅ Connected - found libraries: {libraries}")
            can_connect = True
        except Exception as e:
            logger.warning(f"❌ Cannot connect to ArcticDB: {e}")
            can_connect = False

        if can_connect:
            logger.info("✅ ArcticDB connection successful - locks may not be truly stale")
            if stale_locks and not force:
                logger.info("Use --force to remove locks despite successful connection")
        else:
            logger.warning("❌ Cannot connect to ArcticDB - locks are likely stale")

        # Step 4: Fix stale locks if requested
        results = {"removed": 0, "failed": 0, "skipped": 0, "errors": []}

        if stale_locks and fix:
            logger.info("\n=== Step 4: Removing stale locks ===")

            # Check which locks are in use
            lock_paths = [lock["path"] for lock in stale_locks]
            in_use_status = self._check_lock_files_in_use(lock_paths)

            for lock in stale_locks:
                lock_path = lock["path"]

                try:
                    # Skip if lock is in use and not forcing
                    if in_use_status.get(lock_path, True) and not force:
                        logger.warning(
                            f"Skipping {lock_path} - appears to be in use (use --force to override)"
                        )
                        results["skipped"] += 1
                        continue

                    os.remove(lock_path)
                    logger.info(
                        f"Removed stale lock: {lock_path} (age: {lock['age_days']:.1f} days)"
                    )
                    results["removed"] += 1

                except Exception as e:
                    error_msg = f"Failed to remove {lock_path}: {e}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
                    results["failed"] += 1

            logger.info("\nResults:")
            logger.info(f"  Removed: {results['removed']}")
            logger.info(f"  Failed: {results['failed']}")
            logger.info(f"  Skipped: {results['skipped']}")

            if results["removed"] > 0:
                logger.info("\n=== Step 5: Testing connectivity after cleanup ===")
                try:
                    ac = adb.Arctic(self.db_path)
                    ac.list_libraries()
                    logger.info("✅ ArcticDB connection successful after cleanup!")
                except Exception as e:
                    logger.warning(f"❌ Still cannot connect: {e}")

        elif stale_locks:
            logger.info(
                f"\nFound {len(stale_locks)} stale locks. Use --fix to remove them."
            )

        # Summary
        logger.info("\n=== Summary ===")
        logger.info(f"Database path: {self.physical_path}")
        logger.info(f"Running processes: {len(arctic_processes)}")
        logger.info(f"Stale locks: {len(stale_locks)}")
        logger.info(f"Can connect: {'Yes' if can_connect else 'No'}")

        return {
            "processes": len(arctic_processes),
            "stale_locks": len(stale_locks),
            "can_connect": can_connect,
            "repair_results": results,
        }

    def repair_lmdb(self, output_dir: str = None) -> Dict:
        """Repair LMDB databases using mdb_copy."""
        logger.info("\n=== LMDB REPAIR ===")

        # Find mdb_copy tool
        mdb_copy_path = shutil.which("mdb_copy")
        if not mdb_copy_path:
            conda_path = "/opt/miniconda3/envs/quant/bin/mdb_copy"
            if os.path.exists(conda_path):
                mdb_copy_path = conda_path
            else:
                logger.error("❌ mdb_copy not found. Install LMDB tools.")
                return {"error": "mdb_copy not found"}

        logger.info(f"Using mdb_copy: {mdb_copy_path}")

        # Find all LMDB databases
        lmdb_databases = self._find_all_lmdb_databases(self.physical_path)
        logger.info(f"Found {len(lmdb_databases)} LMDB databases to repair")

        repaired_count = 0
        failed_count = 0
        results = []

        for i, lmdb_info in enumerate(lmdb_databases):
            lmdb_dir = lmdb_info["path"]
            logger.info(f"\n🔧 [{i+1}/{len(lmdb_databases)}] Repairing: {lmdb_dir}")

            # Create repair directory
            if output_dir:
                repair_base = Path(output_dir)
                repair_base.mkdir(parents=True, exist_ok=True)
                repair_dir = (
                    repair_base
                    / f"{Path(lmdb_dir).relative_to(self.physical_path)}_repaired"
                )
            else:
                repair_dir = Path(f"{lmdb_dir}_repaired")

            if repair_dir.exists():
                shutil.rmtree(repair_dir)
            repair_dir.mkdir(parents=True, exist_ok=True)

            try:
                # Use mdb_copy -c to repair
                result = subprocess.run(
                    [mdb_copy_path, "-c", lmdb_dir, str(repair_dir)],
                    capture_output=True,
                    text=True,
                    timeout=600,
                )

                if result.returncode == 0:
                    logger.info(f"✅ Successfully repaired: {lmdb_dir}")
                    logger.info(f"   Output: {repair_dir}")
                    repaired_count += 1

                    results.append(
                        {
                            "original": lmdb_dir,
                            "repaired": str(repair_dir),
                            "status": "success",
                        }
                    )
                else:
                    logger.error(f"❌ Failed to repair: {lmdb_dir}")
                    logger.error(f"   Error: {result.stderr[:200]}")
                    failed_count += 1

                    results.append(
                        {
                            "original": lmdb_dir,
                            "status": "failed",
                            "error": result.stderr[:200],
                        }
                    )

            except subprocess.TimeoutExpired:
                logger.error(f"❌ Repair timed out: {lmdb_dir}")
                failed_count += 1
                results.append(
                    {"original": lmdb_dir, "status": "timeout", "error": "Timed out"}
                )

            except Exception as e:
                logger.error(f"❌ Error repairing {lmdb_dir}: {e}")
                failed_count += 1
                results.append({"original": lmdb_dir, "status": "error", "error": str(e)})

        # Summary
        logger.info("\n📊 LMDB REPAIR SUMMARY:")
        logger.info(f"   ✅ Repaired: {repaired_count}")
        logger.info(f"   ❌ Failed: {failed_count}")
        logger.info(f"   📁 Total: {len(lmdb_databases)}")

        if repaired_count > 0:
            logger.info(
                "\n⚠️ IMPORTANT: Repaired databases saved to *_repaired directories"
            )
            logger.info("   Review repairs before replacing original databases")
            logger.info(
                "   To replace: backup original, then move repaired to original location"
            )

        return {
            "repaired_count": repaired_count,
            "failed_count": failed_count,
            "total_count": len(lmdb_databases),
            "results": results,
        }

    def optimize_prune(
        self, libraries: List[str] = None, batch_size: int = 1000, dry_run: bool = False
    ) -> Dict:
        """Prune previous versions of symbols to keep only latest version."""
        self._check_arcticdb()
        logger.info("\n=== VERSION PRUNING ===")

        if libraries is None:
            libraries = [
                "price_history.daily",
                "price_history.metadata",
                "symbol_metadata",
            ]

        results = {
            "start_time": datetime.now(),
            "libraries_processed": 0,
            "symbols_pruned": 0,
            "libraries_stats": {},
            "errors": [],
            "dry_run": dry_run,
        }

        # Initialize data manager
        data_manager = MarketDataManager.get_instance(db_path=self.db_path)

        try:
            for lib_name in libraries:
                try:
                    library = None
                    if lib_name == "price_history.daily":
                        library = data_manager.price_history_lib
                    elif lib_name == "price_history.metadata":
                        library = data_manager.price_metadata_lib
                    elif lib_name == "symbol_metadata":
                        library = data_manager.symbol_metadata_lib
                    else:
                        logger.warning(f"Unknown library: {lib_name}, skipping")
                        continue

                    if not library:
                        logger.warning(
                            f"Could not access library: {lib_name}, skipping"
                        )
                        continue

                    # Get symbol list
                    logger.info(f"Listing symbols in {lib_name}...")
                    symbols = library.list_symbols()
                    total_symbols = len(symbols)
                    lib_pruned = 0

                    logger.info(f"Processing {total_symbols} symbols in {lib_name}")

                    # Process in batches
                    for i in range(0, total_symbols, batch_size):
                        batch = symbols[i : i + batch_size]
                        batch_pruned = 0

                        for symbol in batch:
                            try:
                                versions = library.list_versions(symbol)
                                if versions and len(versions) > 1:
                                    if dry_run:
                                        logger.info(
                                            f"Would prune {len(versions)-1} versions for {symbol}"
                                        )
                                    else:
                                        library.prune_previous_versions(symbol)
                                    batch_pruned += 1
                                    lib_pruned += 1
                            except Exception as e:
                                error_msg = f"Error pruning {symbol} in {lib_name}: {e}"
                                logger.error(error_msg)
                                results["errors"].append(error_msg)

                        # Progress
                        processed = min(i + batch_size, total_symbols)
                        logger.info(
                            f"Processed {processed}/{total_symbols} symbols in {lib_name}, "
                            f"pruned {batch_pruned}"
                        )

                    results["libraries_stats"][lib_name] = {
                        "total_symbols": total_symbols,
                        "symbols_pruned": lib_pruned,
                    }
                    results["symbols_pruned"] += lib_pruned
                    results["libraries_processed"] += 1

                    logger.info(
                        f"Completed {lib_name}: pruned {lib_pruned}/{total_symbols} symbols"
                    )

                except Exception as e:
                    error_msg = f"Error processing library {lib_name}: {e}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)

            results["end_time"] = datetime.now()
            results["duration"] = (
                results["end_time"] - results["start_time"]
            ).total_seconds()

            if not dry_run:
                logger.info(
                    f"\nVersion pruning completed in {results['duration']:.2f}s: "
                    f"pruned {results['symbols_pruned']} symbols across {results['libraries_processed']} libraries"
                )
            else:
                logger.info(
                    f"\nDry run completed in {results['duration']:.2f}s: "
                    f"would prune {results['symbols_pruned']} symbols"
                )

        finally:
            if data_manager:
                data_manager.close()
                logger.debug("Database connection closed")

        return results

    def optimize_compact(
        self, output_dir: str = None, libraries: List[str] = None
    ) -> Dict:
        """Compact database using mdb_copy (safer alternative to full export/reimport)."""
        logger.info("\n=== DATABASE COMPACTION (mdb_copy) ===")

        if libraries is None:
            libraries = [
                "price_history.daily",
                "price_history.metadata",
                "symbol_metadata",
            ]

        # Find mdb_copy
        mdb_copy_path = shutil.which("mdb_copy")
        if not mdb_copy_path:
            conda_path = "/opt/miniconda3/envs/quant/bin/mdb_copy"
            if os.path.exists(conda_path):
                mdb_copy_path = conda_path
            else:
                logger.error("❌ mdb_copy not found. Install LMDB tools.")
                return {"error": "mdb_copy not found"}

        logger.info(f"Using mdb_copy: {mdb_copy_path}")

        # Create output directory
        if not output_dir:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            parent = Path(self.physical_path).parent
            output_dir = parent / f"Market_Data_Compacted_{timestamp}"

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {output_dir}")

        results = {
            "start_time": datetime.now(),
            "libraries_compacted": 0,
            "total_original_gb": 0,
            "total_compacted_gb": 0,
            "space_saved_gb": 0,
            "libraries_stats": {},
            "errors": [],
        }

        # Create directory structure
        for lib in libraries:
            try:
                if lib == "price_history.daily":
                    (output_path / "price_history" / "daily").mkdir(
                        parents=True, exist_ok=True
                    )
                elif lib == "price_history.metadata":
                    (output_path / "price_history" / "metadata").mkdir(
                        parents=True, exist_ok=True
                    )
                elif lib == "symbol_metadata":
                    (output_path / "symbol_metadata").mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logger.error(f"Error creating directories: {e}")
                return {"error": f"Directory creation failed: {e}"}

        # Copy ArcticDB configuration
        arctic_cfg_source = Path(self.physical_path) / "_arctic_cfg"
        arctic_cfg_target = output_path / "_arctic_cfg"

        if arctic_cfg_source.exists():
            try:
                shutil.copytree(arctic_cfg_source, arctic_cfg_target)
                logger.info("✓ ArcticDB configuration copied")
            except Exception as e:
                logger.warning(f"Could not copy config: {e}")

        # Compact each library
        for lib in libraries:
            try:
                # Map library name to path
                if lib == "price_history.daily":
                    source = Path(self.physical_path) / "price_history" / "daily"
                    target = output_path / "price_history" / "daily"
                elif lib == "price_history.metadata":
                    source = Path(self.physical_path) / "price_history" / "metadata"
                    target = output_path / "price_history" / "metadata"
                elif lib == "symbol_metadata":
                    source = Path(self.physical_path) / "symbol_metadata"
                    target = output_path / "symbol_metadata"
                else:
                    logger.warning(f"Unknown library: {lib}, skipping")
                    continue

                logger.info(f"\n📦 Processing library: {lib}")

                if not source.exists():
                    logger.warning(f"Source not found: {source}, skipping")
                    continue

                data_file = source / "data.mdb"
                if not data_file.exists():
                    logger.warning(f"Data file not found: {data_file}, skipping")
                    continue

                # Get original size
                original_size = data_file.stat().st_size
                original_gb = original_size / (1024**3)
                logger.info(f"Original size: {original_gb:.2f} GB")

                # Compact using mdb_copy
                logger.info("Compacting...")
                result = subprocess.run(
                    [mdb_copy_path, "-c", str(source), str(target)],
                    capture_output=True,
                    text=True,
                    timeout=600,
                )

                if result.returncode != 0:
                    error_msg = f"Compaction failed for {lib}: {result.stderr}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
                    continue

                # Check compacted size
                compacted_data = target / "data.mdb"
                if not compacted_data.exists():
                    error_msg = f"Compaction succeeded but output not found: {compacted_data}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
                    continue

                compacted_size = compacted_data.stat().st_size
                compacted_gb = compacted_size / (1024**3)
                saving_gb = (original_size - compacted_size) / (1024**3)
                saving_pct = (
                    (original_size - compacted_size) / original_size * 100
                    if original_size > 0
                    else 0
                )

                logger.info(f"Compacted size: {compacted_gb:.2f} GB")
                logger.info(f"Space saved: {saving_gb:.2f} GB ({saving_pct:.1f}%)")

                results["libraries_stats"][lib] = {
                    "original_gb": original_gb,
                    "compacted_gb": compacted_gb,
                    "saved_gb": saving_gb,
                    "saved_pct": saving_pct,
                }

                results["total_original_gb"] += original_gb
                results["total_compacted_gb"] += compacted_gb
                results["space_saved_gb"] += saving_gb
                results["libraries_compacted"] += 1

            except Exception as e:
                error_msg = f"Error compacting {lib}: {e}"
                logger.error(error_msg)
                results["errors"].append(error_msg)

        # Finalize
        results["end_time"] = datetime.now()
        results["duration"] = (
            results["end_time"] - results["start_time"]
        ).total_seconds()
        results["total_saved_pct"] = (
            (results["space_saved_gb"] / results["total_original_gb"] * 100)
            if results["total_original_gb"] > 0
            else 0
        )

        logger.info("\n📊 COMPACTION SUMMARY:")
        logger.info(f"Duration: {results['duration']:.2f}s")
        logger.info(
            f"Total space saved: {results['space_saved_gb']:.2f} GB ({results['total_saved_pct']:.1f}%)"
        )
        logger.info("\n⚠️ To use compacted database:")
        logger.info(f"1. Backup original: mv {self.physical_path} {self.physical_path}_backup")
        logger.info(f"2. Replace with compacted: mv {output_dir} {self.physical_path}")

        results["output_dir"] = str(output_dir)
        return results

    def _export_html_report(self, results: Dict):
        """Export results as HTML report"""
        output_dir = Path(self.market_data_dir) / "analysis_reports"
        output_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        html_file = output_dir / f"analysis_report_{timestamp}.html"

        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Database Analysis Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1 {{ color: #2c3e50; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #3498db; color: white; }}
    </style>
</head>
<body>
    <h1>Database Analysis Report</h1>
    <p>Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    <p>Database: {self.db_path}</p>

    <h2>Summary</h2>
    <table>
        <tr><th>Metric</th><th>Value</th></tr>
        <tr><td>Price Symbols</td><td>{results.get('price_symbols', 'N/A'):,}</td></tr>
        <tr><td>Metadata Symbols</td><td>{results.get('metadata_symbols', 'N/A'):,}</td></tr>
        <tr><td>Complete</td><td>{results.get('complete', 'N/A'):,}</td></tr>
        <tr><td>Price Only</td><td>{results.get('price_only', 'N/A'):,}</td></tr>
        <tr><td>Metadata Only</td><td>{results.get('metadata_only', 'N/A'):,}</td></tr>
    </table>
</body>
</html>
"""

        with open(html_file, 'w') as f:
            f.write(html)

        logger.info(f"✓ HTML report exported: {html_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Database Toolkit - Unified analysis and diagnostics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze database")
    analyze_parser.add_argument(
        "--mode",
        required=True,
        choices=["storage", "metadata", "symbols", "distribution"],
        help="Analysis mode"
    )
    analyze_parser.add_argument(
        "--db-path",
        default=settings.DB_PATH,
        help=f"Database path (default: {settings.DB_PATH})"
    )
    analyze_parser.add_argument(
        "--market-data-dir",
        help="Market data directory (alternative to --db-path)"
    )
    analyze_parser.add_argument(
        "--min-points",
        type=int,
        default=50,
        help="Minimum data points for distribution analysis (default: 50)"
    )
    analyze_parser.add_argument(
        "--export-html",
        action="store_true",
        help="Export HTML report (metadata mode)"
    )

    # Diagnose command
    diagnose_parser = subparsers.add_parser("diagnose", help="Diagnose issues")
    diagnose_parser.add_argument(
        "--mode",
        required=True,
        choices=["status", "damage"],
        help="Diagnostic mode"
    )
    diagnose_parser.add_argument(
        "--db-path",
        default=settings.DB_PATH,
        help=f"Database path (default: {settings.DB_PATH})"
    )
    diagnose_parser.add_argument(
        "--market-data-dir",
        help="Market data directory (alternative to --db-path)"
    )

    # Test command
    test_parser = subparsers.add_parser("test", help="Test database")
    test_parser.add_argument(
        "--mode",
        required=True,
        choices=["connection", "validation"],
        help="Test mode"
    )
    test_parser.add_argument(
        "--db-path",
        default=settings.DB_PATH,
        help=f"Database path (default: {settings.DB_PATH})"
    )
    test_parser.add_argument(
        "--market-data-dir",
        help="Market data directory (alternative to --db-path)"
    )

    # Check command
    check_parser = subparsers.add_parser("check", help="Check database health")
    check_parser.add_argument(
        "--mode",
        required=True,
        choices=["lmdb"],
        help="Check mode (currently only lmdb supported)"
    )
    check_parser.add_argument(
        "--db-path",
        default=settings.DB_PATH,
        help=f"Database path (default: {settings.DB_PATH})"
    )
    check_parser.add_argument(
        "--market-data-dir",
        help="Market data directory (alternative to --db-path)"
    )

    # Repair command
    repair_parser = subparsers.add_parser("repair", help="Repair database issues")
    repair_parser.add_argument(
        "--mode",
        required=True,
        choices=["locks", "lmdb"],
        help="Repair mode (locks=stale locks, lmdb=corrupted files)"
    )
    repair_parser.add_argument(
        "--db-path",
        default=settings.DB_PATH,
        help=f"Database path (default: {settings.DB_PATH})"
    )
    repair_parser.add_argument(
        "--market-data-dir",
        help="Market data directory (alternative to --db-path)"
    )
    repair_parser.add_argument(
        "--stale-hours",
        type=int,
        default=24,
        help="Consider locks stale after N hours (locks mode, default: 24)"
    )
    repair_parser.add_argument(
        "--fix",
        action="store_true",
        help="Actually fix issues (otherwise just report, locks mode)"
    )
    repair_parser.add_argument(
        "--force",
        action="store_true",
        help="Force repair even if locks appear in use (locks mode)"
    )
    repair_parser.add_argument(
        "--output-dir",
        help="Output directory for repaired databases (lmdb mode)"
    )

    # Optimize command
    optimize_parser = subparsers.add_parser("optimize", help="Optimize database")
    optimize_parser.add_argument(
        "--mode",
        required=True,
        choices=["prune", "compact"],
        help="Optimize mode (prune=remove old versions, compact=mdb_copy compaction)"
    )
    optimize_parser.add_argument(
        "--db-path",
        default=settings.DB_PATH,
        help=f"Database path (default: {settings.DB_PATH})"
    )
    optimize_parser.add_argument(
        "--market-data-dir",
        help="Market data directory (alternative to --db-path)"
    )
    optimize_parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for prune mode (default: 1000)"
    )
    optimize_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate operations without making changes (prune mode)"
    )
    optimize_parser.add_argument(
        "--output-dir",
        help="Output directory for compacted database (compact mode)"
    )
    optimize_parser.add_argument(
        "--libraries",
        help="Comma-separated list of libraries (e.g., price_history.daily,symbol_metadata)"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Initialize analyzer
    analyzer = DatabaseAnalyzer(
        db_path=args.db_path if hasattr(args, 'db_path') else None,
        market_data_dir=args.market_data_dir if hasattr(args, 'market_data_dir') else None
    )

    # Execute command
    try:
        if args.command == "analyze":
            if args.mode == "storage":
                analyzer.analyze_storage()
            elif args.mode == "metadata":
                analyzer.analyze_metadata(export_html=args.export_html)
            elif args.mode == "symbols":
                analyzer.analyze_symbols()
            elif args.mode == "distribution":
                analyzer.analyze_distribution(min_points=args.min_points)

        elif args.command == "diagnose":
            if args.mode == "status":
                analyzer.diagnose_status()
            elif args.mode == "damage":
                analyzer.diagnose_damage()

        elif args.command == "test":
            if args.mode == "connection":
                analyzer.test_connection()
            elif args.mode == "validation":
                analyzer.test_validation()

        elif args.command == "check":
            if args.mode == "lmdb":
                analyzer.check_lmdb()

        elif args.command == "repair":
            if args.mode == "locks":
                analyzer.repair_locks(
                    stale_hours=args.stale_hours,
                    fix=args.fix,
                    force=args.force
                )
            elif args.mode == "lmdb":
                analyzer.repair_lmdb(output_dir=args.output_dir)

        elif args.command == "optimize":
            # Parse libraries if provided
            libraries = None
            if hasattr(args, 'libraries') and args.libraries:
                libraries = [lib.strip() for lib in args.libraries.split(',')]

            if args.mode == "prune":
                analyzer.optimize_prune(
                    libraries=libraries,
                    batch_size=args.batch_size,
                    dry_run=args.dry_run
                )
            elif args.mode == "compact":
                analyzer.optimize_compact(
                    output_dir=args.output_dir,
                    libraries=libraries
                )

        return 0

    except Exception as e:
        logger.error(f"Command failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

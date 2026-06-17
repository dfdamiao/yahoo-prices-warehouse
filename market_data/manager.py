# market_data_manager.py
import atexit
import logging
from threading import Lock
from typing import Dict, Optional
from arcticdb import Arctic

# Import centralized configuration
from market_data.settings import DB_PATH

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("market_data")


class MarketDataManager:
    """
    Singleton manager for ArcticDB market data connections.

    Ensures only one connection exists per database path to avoid
    LMDB connection errors.
    """

    # Class variables for singleton pattern by path
    _instances: Dict[str, "MarketDataManager"] = {}
    _lock = Lock()
    _default_path = DB_PATH  # Now uses config.py for portable paths

    @classmethod
    def get_instance(cls, db_path: Optional[str] = None) -> "MarketDataManager":
        """Get a MarketDataManager instance for the specified database path."""
        if db_path is None:
            db_path = cls._default_path

        with cls._lock:
            if db_path not in cls._instances or cls._instances[db_path]._arctic is None:
                cls._instances[db_path] = MarketDataManager(db_path)
            return cls._instances[db_path]

    def __init__(self, db_path: str):
        """Initialize the MarketDataManager (use get_instance() instead)."""
        self.db_path = db_path
        self._arctic = None
        self._symbol_metadata_lib = None
        self._price_history_lib = None
        self._price_metadata_lib = None
        self._initialized = False

        # Connect to the database
        self.connect()

        # Register cleanup on Python exit
        atexit.register(self.close)

    def connect(self) -> None:
        """Connect to the ArcticDB database and initialize libraries."""
        if self._arctic is not None:
            logger.debug(f"Already connected to {self.db_path}")
            return

        try:
            logger.info(f"Connecting to ArcticDB at {self.db_path}")
            self._arctic = Arctic(self.db_path)

            # Initialize libraries
            self._symbol_metadata_lib = self._arctic.get_library(
                "symbol_metadata", create_if_missing=True
            )
            self._price_history_lib = self._arctic.get_library(
                "price_history.daily", create_if_missing=True
            )
            self._price_metadata_lib = self._arctic.get_library(
                "price_history.metadata", create_if_missing=True
            )

            self._initialized = True
            logger.info(f"Successfully connected to ArcticDB at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to ArcticDB at {self.db_path}: {str(e)}")
            self._initialized = False
            raise

    def close(self) -> None:
        """Close the ArcticDB connection and clean up resources."""
        if self._arctic is not None:
            logger.info(f"Closing connection to ArcticDB at {self.db_path}")

            # Remove this instance from the instances dictionary
            with self.__class__._lock:
                if self.db_path in self.__class__._instances:
                    del self.__class__._instances[self.db_path]

            # Clear references to libraries
            self._symbol_metadata_lib = None
            self._price_history_lib = None
            self._price_metadata_lib = None

            # Set arctic to None to indicate closed connection
            self._arctic = None
            self._initialized = False

    @property
    def is_connected(self) -> bool:
        """Check if the manager is connected to the database."""
        return self._arctic is not None and self._initialized

    # Basic library access properties
    @property
    def symbol_metadata_lib(self):
        """Get the symbol metadata library."""
        if not self.is_connected:
            raise RuntimeError("Not connected to the database")
        return self._symbol_metadata_lib

    @property
    def price_history_lib(self):
        """Get the price history library."""
        if not self.is_connected:
            raise RuntimeError("Not connected to the database")
        return self._price_history_lib

    @property
    def price_metadata_lib(self):
        """Get the price metadata library."""
        if not self.is_connected:
            raise RuntimeError("Not connected to the database")
        return self._price_metadata_lib

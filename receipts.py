import concurrent.futures
import json
import logging
import random
import signal
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from queue import Queue
from typing import Dict, List

import grid3.network
import requests
from grid3.minting.period import Period

# Configure logging for the daemon
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

STANDARD_PERIOD_DURATION = 24 * 60 * 60 * (365 * 3 + 366 * 2) // 60
ONE_HOUR = 60 * 60

# With 50 workers, we can scrape ~7500 nodes in ~10 minutes
SCRAPER_WORKERS = 50

# Daemon mode constants
DAEMON_CHECK_INTERVAL = 10  # seconds between checks in the inner loop
DAEMON_CHECKS_PER_CYCLE = (
    60  # number of checks before checking receipts (10 minutes total)
)
DAEMON_REFRESH_INTERVAL = 6  # refresh node list every hour (6 * 10 minutes)
DAEMON_ERROR_RETRY_DELAY = 60  # seconds to wait after an error


class ReceiptHandler:
    """This is a wrapper for the Alpha Minting API, with caching. Fetched
    receipts are stored to disk and also symlinked into a folder for the node
    which the receipt corresponds to, when fetching at the node level.

    Thread safety is provided at the node level for get_node_receipts and get_receipt only.
    """

    def __init__(
        self,
        db_path: str = "receipts.db",
        connection_pool_size: int = 5,
        base_url: str = "https://alpha.minting.tfchain.grid.tf/api/v1",
        query_rate: int = ONE_HOUR,
    ):
        self.db_path = db_path
        self.connection_pool_size = connection_pool_size
        self.base_url = base_url
        self.query_rate = query_rate

        # Initialize connection pool
        self.pool = Queue()
        for _ in range(self.connection_pool_size):
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")  # 5 second timeout
            self.pool.put(conn)

        # Initialize database schema if it doesn't exist
        self.init_db()

        # Check if database is empty and print warning
        if self.is_database_empty():
            print(
                f"WARNING: Database '{db_path}' is empty. Please run the receipts daemon to populate receipts data."
            )

    @contextmanager
    def get_connection(self):
        conn = self.pool.get()
        try:
            yield conn
        finally:
            self.pool.put(conn)

    def init_db(self):
        """ "
        We store the period_end for each receipt because this is the data that's
        available in the original receipt which uniquely identifies the period.
        The period start gets scaled for nodes that were created within the
        period in question, so it's not reliable.

        For node timestamps, the period end is the relevant information, since we'll use it to determine whether it's possible that new receipts exist. We also store the last time we fetched against the API, so we can limit the rate of requests (mostly for UX, since checking for new receipts is relatively slow but we want to keep checking until the new ones are published).
        """
        with self.get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS receipts (
                    hash TEXT PRIMARY KEY,
                    node_id INTEGER,
                    receipt_type TEXT,
                    receipt_data TEXT,
                    period_end INTEGER
                )
            """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS node_last_period_end (
                    node_id INTEGER PRIMARY KEY,
                    timestamp INTEGER
                )
            """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS node_last_query (
                    node_id INTEGER PRIMARY KEY,
                    timestamp INTEGER
                )
            """
            )
            conn.commit()

    def is_database_empty(self) -> bool:
        """Check if the receipts table is empty"""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM receipts")
            return cursor.fetchone()[0] == 0

    def fetch_receipt(self, receipt_hash: str) -> Dict | None:
        """Fetch receipt from the API with a given hash."""
        url = f"{self.base_url}/{receipt_hash}"
        try:
            response = requests.get(url)
            if response.ok:
                return self.process_receipt(response.json())
            else:
                return None
        except requests.RequestException as e:
            print(f"Failed to fetch receipt {receipt_hash}: {str(e)}")
            return None

    def fetch_node_receipts(self, node_id: int) -> List[Dict]:
        """Fetch receipts from the API for a given node ID."""
        url = f"{self.base_url}/node/{node_id}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return [self.process_receipt(r) for r in response.json()]
        except requests.RequestException as e:
            print(f"Failed to fetch receipts for node {node_id}: {str(e)}")
            return []

    def process_receipt(self, data: Dict) -> dict:
        # Flatten receipts so the type is an attribute, and also include the
        # hash. This expects the data shape returned by the /node endpoint
        receipt_hash, receipt = data["hash"], data["receipt"]
        if "Minting" in receipt:
            receipt = receipt["Minting"]
            receipt["type"] = "Minting"
        elif "Fixup" in receipt:
            receipt = receipt["Fixup"]
            receipt["type"] = "Fixup"
        receipt["hash"] = receipt_hash
        return receipt

    def save_receipt(self, receipt: Dict) -> None:
        """Save a receipt to the database."""
        receipt_hash = receipt.get("hash")
        if not receipt_hash:
            raise ValueError("Receipt missing hash field")

        with self.get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO receipts (hash, node_id, receipt_type, receipt_data, period_end) VALUES (?, ?, ?, ?, ?)",
                (
                    receipt_hash,
                    receipt["node_id"],
                    receipt["type"],
                    json.dumps(receipt),
                    receipt["period"]["end"],
                ),
            )
            conn.commit()

    def get_stored_receipt(self, receipt_hash: str) -> Dict | None:
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT receipt_data FROM receipts WHERE hash = ?", (receipt_hash,)
            )
            row = cursor.fetchone()
            return json.loads(row[0]) if row else None

    def get_stored_node_receipts(self, node_id: int) -> List[Dict]:
        """Get all stored receipts for a given node ID."""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT receipt_data FROM receipts WHERE node_id = ?", (node_id,)
            )
            return [json.loads(row[0]) for row in cursor.fetchall()]

    def get_stored_node_period_receipts(
        self, node_id: int, period: Period
    ) -> List[Dict]:
        """Get all stored receipts for a given node ID during a period."""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT receipt_data FROM receipts WHERE node_id = ? AND period_end = ?",
                (node_id, period.end),
            )
            return [json.loads(row[0]) for row in cursor.fetchall()]

    def get_receipt(self, receipt_hash: str) -> Dict | None:
        # Read-only version: only return stored receipt
        return self.get_stored_receipt(receipt_hash)

    def get_node_receipts(self, node_id: int) -> List[Dict]:
        # Read-only version: only return stored receipts
        return self.get_stored_node_receipts(node_id)

    def get_node_period_receipts(self, node_id: int, period: Period) -> List[Dict]:
        # Read-only version: only return stored receipts
        return self.get_stored_node_period_receipts(node_id, period)


# Daemon-only helper functions (not part of ReceiptHandler class)
def fetch_and_process_node(handler: ReceiptHandler, node_id: int) -> List[Dict]:
    """Process all receipts for a given node (daemon only)."""
    receipts = handler.fetch_node_receipts(node_id)

    for receipt in receipts:
        handler.save_receipt(receipt)

    last_period_end = max([receipt["period"]["end"] for receipt in receipts], default=0)
    if last_period_end != 0:
        # Use direct SQL for daemon operations
        with handler.get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO node_last_period_end (node_id, timestamp) VALUES (?, ?)",
                (node_id, last_period_end),
            )
            conn.commit()

    # Save query timestamp
    with handler.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO node_last_query(node_id, timestamp) VALUES (?, ?)",
            (node_id, time.time()),
        )
        conn.commit()

    return receipts


def has_all_node_receipts(handler: ReceiptHandler, node_id: int) -> bool:
    """Check if node has all receipts (daemon only)."""
    with handler.get_connection() as conn:
        cursor = conn.execute(
            "SELECT timestamp FROM node_last_period_end WHERE node_id = ?",
            (node_id,),
        )
        row = cursor.fetchone()
        timestamp = row[0] if row else None
        return bool(timestamp and time.time() < timestamp + STANDARD_PERIOD_DURATION)


def query_time_elapsed(handler: ReceiptHandler, node_id: int) -> bool:
    """Check if query rate has elapsed (daemon only)."""
    with handler.get_connection() as conn:
        cursor = conn.execute(
            "SELECT timestamp FROM node_last_query WHERE node_id = ?",
            (node_id,),
        )
        row = cursor.fetchone()
        last_timestamp = row[0] if row else None
        if last_timestamp is None:
            return True
        else:
            return last_timestamp + handler.query_rate < time.time()


@dataclass
class NodeMintingPeriod:
    """This is an abstraction over receipts for a given node in a given period.
    When a fixup minting event occurs, two new receipts are generated for each
    node included in the fixup. One is a second "Minting" receipt with all the
    new values, and two is a "Fixup" receipt that describes the relationship
    between the two regular receipts. Therefore for any node in any period,
    there can be either 1 or 1 + 2N receipts, where N is the number of
    applicable fixups for that period (I'm not aware of any periods with more
    than one fixup so far, thus we'll asssume that N always equals 1).

    It's also compatible with periods that don't have receipts available yet
    because receipt publishing has not been completed yet for that period. In
    that case, node_id and period must be specified."""

    node_id: int
    period: Period
    minted_receipt: Dict | None = None
    correct_receipt: Dict | None = None
    fixup_receipt: Dict | None = None
    has_receipt: bool = False
    empty: bool = False

    def __post_init__(self):
        """Validate and set derived attributes after initialization"""
        self.has_receipt = bool(
            self.minted_receipt or self.correct_receipt or self.fixup_receipt
        )
        self._set_empty_status()

    @classmethod
    def from_receipts(
        cls,
        original_receipt: Dict | None,
        corrected_receipt: Dict | None = None,
        fixup_receipt: Dict | None = None,
    ) -> "NodeMintingPeriod":
        """Create a NodeMintingPeriod from receipt data"""
        if original_receipt:
            node_id = original_receipt["node_id"]
            period = Period(original_receipt["period"]["start"])
        elif fixup_receipt:
            node_id = fixup_receipt["node_id"]
            period = Period(fixup_receipt["period"]["start"])
        else:
            raise ValueError("Either original or fixup receipt must be provided.")

        return cls(
            node_id=node_id,
            period=period,
            minted_receipt=original_receipt,
            correct_receipt=corrected_receipt,
            fixup_receipt=fixup_receipt,
        )

    @classmethod
    def for_unpublished_period(
        cls, node_id: int, period: Period
    ) -> "NodeMintingPeriod":
        """Create a NodeMintingPeriod for an unpublished period"""
        return cls(node_id=node_id, period=period)

    def _set_empty_status(self) -> None:
        """Determine if the receipt represents empty uptime"""
        if self.fixup_receipt:
            if self.minted_receipt and self.correct_receipt:
                self.empty = (
                    self.minted_receipt["measured_uptime"] == 0
                    and self.correct_receipt["measured_uptime"] == 0
                )
            elif self.correct_receipt:
                self.empty = self.correct_receipt["measured_uptime"] == 0
            else:
                self.empty = False
        elif self.minted_receipt:
            self.empty = self.minted_receipt["measured_uptime"] == 0
        else:
            # For unpublished periods, assume not empty
            self.empty = False


def make_node_minting_periods(
    node_id: int, receipts_input: List[Dict]
) -> List[NodeMintingPeriod]:
    """We pass in the node id explicity, because this function might be called
    for nodes that are too new to have any receipts. In that case the receipts
    input list is empty and only one NodeMintingPeriod should be returned
    """
    period_receipts = []
    by_period = {}
    last_end = 0
    for receipt in receipts_input:
        period_end = receipt["period"]["end"]
        last_end = max(last_end, period_end)
        receipts = by_period.setdefault(period_end, {})
        if receipt["type"] == "Minting":
            receipts[receipt["hash"]] = receipt
        elif receipt["type"] == "Fixup":
            receipts["fixup"] = receipt

    for period_end, receipts in by_period.items():
        if "fixup" in receipts:
            fixup = receipts["fixup"]
            # Some hashes present in fixup receipts aren't returned by the API
            try:
                minted_receipt = receipts[fixup["minted_receipt"]]
            except KeyError:
                minted_receipt = None
            try:
                correct_receipt = receipts[fixup["correct_receipt"]]
            except KeyError:
                correct_receipt = None
            period_receipts.append(
                NodeMintingPeriod.from_receipts(
                    minted_receipt,
                    correct_receipt,
                    fixup,
                )
            )
        else:
            period_receipts.append(
                NodeMintingPeriod.from_receipts(receipts.popitem()[1])
            )

    # There are two scenarios, since minting and the publishing of receipts takes at least a few days after each period ends. Either the receipts for the last completed period are published or they are not. If they are not, then there are two periods, the last one and the current one, for which no receipts are available. Otherwise, it's only the current period. From there, we must also account for the fact that the node might not have existed last period. So if last period's receipts haven't been published, and this node has no receipts, we need to query the node creation time

    this_period = Period()
    previous_period = Period(offset=this_period.offset - 1)
    period_receipts.append(
        NodeMintingPeriod.for_unpublished_period(node_id, this_period)
    )
    # The last period we found a receipt for is earlier than the previous one
    if last_end < previous_period.end:
        if len(period_receipts) > 1:
            # Node has receipt history, so it must have existed before the
            # previous period
            period_receipts.append(
                NodeMintingPeriod.for_unpublished_period(node_id, previous_period)
            )
        else:
            # No receipt history, so we need to check if this node existed at
            # any time during the previous period. TODO: the node creation time
            # should be a parameter of this function that the caller can query
            # once and cache
            node = requests.get(f"https://gridproxy.grid.tf/nodes/{node_id}").json()
            if node["created"] < previous_period.end:
                period_receipts.append(
                    NodeMintingPeriod.for_unpublished_period(node_id, previous_period)
                )

    return period_receipts


def scrape_node(handler: ReceiptHandler, node_id: int):
    try:
        fetch_and_process_node(handler, node_id)
        return True
    except Exception as e:
        print(f"Error processing node {node_id}: {e}")
        return False


def scrape_nodes(handler: ReceiptHandler, node_ids: List[int]):
    with concurrent.futures.ThreadPoolExecutor(max_workers=SCRAPER_WORKERS) as executor:
        return list(
            executor.map(lambda node_id: scrape_node(handler, node_id), node_ids)
        )


def get_all_node_ids() -> List[int]:
    mainnet = grid3.network.GridNetwork()
    nodes = mainnet.graphql.nodes(["nodeID"])
    return [int(node["nodeID"]) for node in nodes]


def check_for_new_receipts(handler: ReceiptHandler, node_ids: List[int]) -> bool:
    """Check if any nodes have new receipts available.
    First checks a random node, and if new receipts are found there,
    checks all nodes. Returns True if new receipts were found and processed."""

    # First check a random node
    if node_ids:
        random_node_id = random.choice(node_ids)
        logging.info(f"Checking random node {random_node_id} for new receipts...")

        if not has_all_node_receipts(handler, random_node_id) and query_time_elapsed(
            handler, random_node_id
        ):
            receipts = fetch_and_process_node(handler, random_node_id)
            if receipts:
                logging.info(
                    f"Found new receipts on random node {random_node_id}, checking all nodes..."
                )
                # Found new receipts on random node, now check all nodes
                new_receipts_found = False
                for node_id in node_ids:
                    if not has_all_node_receipts(
                        handler, node_id
                    ) and query_time_elapsed(handler, node_id):
                        receipts = fetch_and_process_node(handler, node_id)
                        if receipts:
                            new_receipts_found = True
                return new_receipts_found

    return False


def main():
    # Create handler - this will create and initialize the database if it doesn't exist
    handler = ReceiptHandler()
    node_ids = get_all_node_ids()

    # Flag to control the daemon loop
    running = True

    def signal_handler(signum, frame):
        nonlocal running
        logging.info(f"Received signal {signum}, shutting down...")
        running = False

    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Only do initial full scrape if database is empty
    if handler.is_database_empty():
        logging.info(f"Found {len(node_ids)} nodes to process")
        results = scrape_nodes(handler, node_ids)
        logging.info(
            f"Initial scrape completed. {sum(results)} nodes processed successfully"
        )
    else:
        logging.info("Database already populated, skipping initial full scrape")
        # Still need to get node IDs for the daemon loop
        node_ids = get_all_node_ids()

    # Continuous monitoring loop - DAEMON MODE
    logging.info("Starting daemon mode...")

    # Initialize refresh counter
    refresh_counter = 0

    while running:
        try:
            # Refresh node IDs periodically
            if refresh_counter >= DAEMON_REFRESH_INTERVAL or refresh_counter == 0:
                logging.info("Refreshing node ID list...")
                node_ids = get_all_node_ids()
                logging.info(f"Found {len(node_ids)} nodes")
                refresh_counter = 0

            # Wait before checking for updates
            for _ in range(DAEMON_CHECKS_PER_CYCLE):
                if not running:
                    break
                time.sleep(DAEMON_CHECK_INTERVAL)

            if not running:
                break

            # Check for new receipts using current node list
            logging.info("Checking for new receipts...")
            new_receipts_found = check_for_new_receipts(handler, node_ids)

            if new_receipts_found:
                logging.info("New receipts were found and processed")
            else:
                logging.info("No new receipts found at this time")

            # Increment refresh counter
            refresh_counter += 1

        except Exception as e:
            logging.error(f"Error in daemon loop: {e}")
            time.sleep(DAEMON_ERROR_RETRY_DELAY)

    logging.info("Receipt daemon stopped")


if __name__ == "__main__":
    main()

import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from queue import Queue
from typing import Dict, List

import requests
from grid3.minting.period import Period

STANDARD_PERIOD_DURATION = 24 * 60 * 60 * (365 * 3 + 366 * 2) // 60
CONNECTION_POOL_SIZE = 5


class ReceiptHandler:
    """This is a wrapper for the Alpha Minting API, with caching. Fetched
    receipts are stored to disk and also symlinked into a folder for the node
    which the receipt corresponds to, when fetching at the node level.

    Thread safety is provided at the node level for get_node_receipts and get_receipt only.
    """

    def __init__(self, base_url: str = "https://alpha.minting.tfchain.grid.tf/api/v1"):
        self.base_url = base_url
        self.db_path = "receipts.db"
        self.pool = Queue()
        for _ in range(CONNECTION_POOL_SIZE):
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            self.pool.put(conn)

        # Initialize database
        self.init_db()

    @contextmanager
    def get_connection(self):
        conn = self.pool.get()
        try:
            yield conn
        finally:
            self.pool.put(conn)

    def init_db(self):
        # We store the period_end because this is the data that's availble in
        # the original receipt which uniquely identifies the period. The period
        # start gets scaled for nodes that were created within the period in
        # question, so it's not reliable.
        with self.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS receipts (
                    hash TEXT PRIMARY KEY,
                    node_id INTEGER,
                    receipt_type TEXT,
                    receipt_data TEXT,
                    period_end INTEGER
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS node_timestamps (
                    node_id INTEGER PRIMARY KEY,
                    latest_timestamp INTEGER
                )
            """)
            conn.commit()

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
            raise Exception(f"Failed to fetch receipt {receipt_hash}: {str(e)}")

    def fetch_receipts(self, node_id: int) -> List[Dict]:
        """Fetch receipts from the API for a given node ID."""
        url = f"{self.base_url}/node/{node_id}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return [self.process_receipt(r) for r in response.json()]
        except requests.RequestException as e:
            raise Exception(f"Failed to fetch receipts for node {node_id}: {str(e)}")

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

    def get_latest_timestamp(self, receipts: List[Dict]) -> int | None:
        """Extract the latest timestamp from a list of receipts."""
        latest_timestamp = None

        for receipt in receipts:
            if isinstance(receipt, dict):
                period = receipt["period"]
                end_time = period["end"]
                if end_time and (
                    latest_timestamp is None or end_time > latest_timestamp
                ):
                    latest_timestamp = end_time

        return latest_timestamp

    def save_latest_timestamp(self, node_id: int, timestamp: int):
        """Save the latest timestamp to the database."""
        with self.get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO node_timestamps (node_id, latest_timestamp) VALUES (?, ?)",
                (node_id, timestamp),
            )
            conn.commit()

    def get_stored_timestamp(self, node_id: int) -> int | None:
        """Get the stored timestamp for a node if it exists."""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT latest_timestamp FROM node_timestamps WHERE node_id = ?",
                (node_id,),
            )
            row = cursor.fetchone()
            return row[0] if row else None

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

    def fetch_and_process_node(self, node_id: int) -> List[Dict]:
        """Process all receipts for a given node."""
        receipts = self.fetch_receipts(node_id)

        for receipt in receipts:
            self.save_receipt(receipt)

        latest_timestamp = self.get_latest_timestamp(receipts)
        if latest_timestamp:
            self.save_latest_timestamp(node_id, latest_timestamp)

        print(f"Successfully processed {len(receipts)} receipts for node {node_id}")
        if latest_timestamp:
            print(f"Latest timestamp: {latest_timestamp}")

        return receipts

    def get_receipt(self, receipt_hash: str) -> Dict | None:
        """Sometimes we just need a single receipt. We don't bother linking to the node for now."""
        receipt = self.get_stored_receipt(receipt_hash)
        if not receipt:
            receipt = self.fetch_receipt(receipt_hash)
            if receipt:
                self.save_receipt(receipt)

        return receipt

    def has_node_receipts(self, node_id: int) -> bool:
        """If there's a timestamp on disk from a previous fetch, check if at
        least one minting period worth of time has elapsed since then. This
        indicates whether we already have all available receipts for this node.
        """
        timestamp = self.get_stored_timestamp(node_id)
        return bool(timestamp and time.time() < timestamp + STANDARD_PERIOD_DURATION)

    def get_node_receipts(self, node_id: int) -> List[Dict]:
        if not self.has_node_receipts(node_id):
            return self.fetch_and_process_node(node_id)
        else:
            return self.get_stored_node_receipts(node_id)

    def get_node_period_receipts(self, node_id: int, period: Period) -> List[Dict]:
        if not self.has_node_receipts(node_id):
            return self.fetch_and_process_node(node_id)
        else:
            return self.get_stored_node_receipts(node_id)


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


def make_node_minting_periods(receipts_input: List[Dict]) -> List[NodeMintingPeriod]:
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

    # There are two scenarios, since minting and the publishing of receipts takes at least a few days after each period ends. Either the receipts for the last completed period are published or they are not. If they are not, then there are two periods, the last one and the current one, for which no receipts are available. Otherwise, it's only the current period.

    this_period = Period()
    previous_period = Period(offset=this_period.offset - 1)
    node_id = period_receipts[0].node_id
    period_receipts.append(
        NodeMintingPeriod.for_unpublished_period(node_id, this_period)
    )
    if last_end < previous_period.end:
        period_receipts.append(
            NodeMintingPeriod.for_unpublished_period(node_id, previous_period)
        )

    return period_receipts


def main():
    # Example usage
    handler = ReceiptHandler()

    # Process receipts for node 42
    handler.fetch_and_process_node(42)

    # Example of getting stored timestamp
    timestamp = handler.get_stored_timestamp(42)
    if timestamp:
        print(f"Stored timestamp for node 42: {timestamp}")

    # Example of getting stored receipts
    receipts = handler.get_stored_node_receipts(42)
    print(f"Found {len(receipts)} stored receipts for node 42")


if __name__ == "__main__":
    main()

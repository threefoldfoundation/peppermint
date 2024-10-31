import json
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Dict, List

import requests
from grid3.minting.period import Period

STANDARD_PERIOD_DURATION = 24 * 60 * 60 * (365 * 3 + 366 * 2) // 60


class ReceiptHandler:
    """This is a wrapper for the Alpha Minting API, with caching. Fetched
    receipts are stored to disk and also symlinked into a folder for the node
    which the receipt corresponds to, when fetching at the node level.

    Thread safety is provided at the node level for get_node_receipts and get_receipt only.
    """

    def __init__(self, base_url: str = "https://alpha.minting.tfchain.grid.tf/api/v1"):
        self.base_url = base_url
        self.receipts_dir = Path("receipts")
        self.nodes_dir = Path("nodes")

        # Create base directories if they don't exist
        self.receipts_dir.mkdir(exist_ok=True)
        self.nodes_dir.mkdir(exist_ok=True)

        # To make the caching portion thread safe, we'll use per node locking
        # with the locks stored in this dict
        self.locks: Dict[int, Lock] = {}
        self.one_lock_to_rule_them_all = Lock()

    @contextmanager
    def lock(self, number: int):
        with self.one_lock_to_rule_them_all:
            lock = self.locks.setdefault(number, Lock())
        try:
            lock.acquire()
            yield
        finally:
            lock.release()
            with self.one_lock_to_rule_them_all:
                # If another thread didn't acquire the lock, deallocate it
                if not lock.locked():
                    self.locks.pop(number)

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

    def save_receipt(self, receipt: Dict) -> Path:
        """Save a receipt to a file named with its hash."""
        receipt_hash = receipt.get("hash")
        if not receipt_hash:
            raise ValueError("Receipt missing hash field")

        file_path = self.receipts_dir / f"{receipt_hash}.json"

        # Only write the file if it doesn't already exist
        if not file_path.exists():
            with open(file_path, "w") as f:
                json.dump(receipt, f, indent=2)

        return file_path

    def create_node_symlinks(self, node_id: int, receipt_paths: List[Path]):
        """Create symlinks to receipt files in the node's directory."""
        node_dir = self.nodes_dir / str(node_id)
        node_dir.mkdir(exist_ok=True)

        # Create relative symlinks from node directory to receipt files
        for receipt_path in receipt_paths:
            symlink_path = node_dir / receipt_path.name
            if not symlink_path.exists():
                # Calculate relative path from node directory to receipt file
                relative_path = os.path.relpath(receipt_path, node_dir)
                symlink_path.symlink_to(relative_path)

    def get_latest_timestamp(self, receipts: List[Dict]) -> int | None:
        """Extract the latest timestamp from a list of receipts."""
        latest_timestamp = None

        for receipt in receipts:
            # Handle both Minting and Fixup receipt types
            if isinstance(receipt, dict):
                period = receipt["period"]
                end_time = period["end"]
                if end_time and (
                    latest_timestamp is None or end_time > latest_timestamp
                ):
                    latest_timestamp = end_time

        return latest_timestamp

    def save_latest_timestamp(self, node_id: int, timestamp: int):
        """Save the latest timestamp to a file in the node's directory."""
        node_dir = self.nodes_dir / str(node_id)
        timestamp_file = node_dir / "latest_timestamp"

        with open(timestamp_file, "w") as f:
            f.write(str(timestamp))

    def get_stored_timestamp(self, node_id: int) -> int | None:
        """Get the stored timestamp for a node if it exists."""
        timestamp_file = self.nodes_dir / str(node_id) / "latest_timestamp"

        if timestamp_file.exists():
            try:
                with open(timestamp_file, "r") as f:
                    return int(f.read().strip())
            except (ValueError, IOError):
                return None
        return None

    def get_stored_receipt(self, receipt_hash: str) -> Dict | None:
        file_path = self.receipts_dir / f"{receipt_hash}.json"
        if file_path.is_file():
            try:
                with open(file_path, "r") as f:
                    return json.load(f)
            except (IOError, json.JSONDecodeError) as e:
                print(f"Error reading receipt {file_path}: {str(e)}")
                return None
        else:
            return None

    def get_stored_receipts(self, node_id: int) -> List[Dict]:
        """Get all stored receipts for a given node ID."""
        node_dir = self.nodes_dir / str(node_id)
        receipts = []

        if not node_dir.exists():
            return receipts

        for symlink in node_dir.iterdir():
            if symlink.is_symlink() and symlink.suffix == ".json":
                try:
                    with open(symlink, "r") as f:
                        receipt = json.load(f)
                        receipts.append(receipt)
                except (IOError, json.JSONDecodeError) as e:
                    print(f"Error reading receipt {symlink}: {str(e)}")
                    continue

        return receipts

    def fetch_and_process_node(self, node_id: int) -> List[Dict]:
        """Process all receipts for a given node."""
        # Fetch receipts
        receipts = self.fetch_receipts(node_id)

        # Save each receipt and collect their paths
        receipt_paths = []
        for receipt in receipts:
            receipt_path = self.save_receipt(receipt)
            receipt_paths.append(receipt_path)

        # Create symlinks in node directory
        self.create_node_symlinks(node_id, receipt_paths)

        # Get and save the latest timestamp
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
                node_id = int(receipt["node_id"])
                with self.lock(node_id):
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
        with self.lock(node_id):
            if not self.has_node_receipts(node_id):
                return self.fetch_and_process_node(node_id)
            else:
                return self.get_stored_receipts(node_id)


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
    receipts = handler.get_stored_receipts(42)
    print(f"Found {len(receipts)} stored receipts for node 42")


if __name__ == "__main__":
    main()

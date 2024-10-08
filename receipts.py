import os
import json
import requests
import time
from pathlib import Path
from typing import List, Dict

STANDARD_PERIOD_DURATION = 24 * 60 * 60 * (365 * 3 + 366 * 2) // 60


class ReceiptHandler:
    """This is a wrapper for the Alpha Minting API, with caching. Fetched
    receipts are stored to disk and also symlinked into a folder for the node
    which the receipt corresponds to, when fetching at the node level.
    """

    def __init__(self, base_url: str = "https://alpha.minting.tfchain.grid.tf/api/v1"):
        self.base_url = base_url
        self.receipts_dir = Path("receipts")
        self.nodes_dir = Path("nodes")

        # Create base directories if they don't exist
        self.receipts_dir.mkdir(exist_ok=True)
        self.nodes_dir.mkdir(exist_ok=True)

    def get_receipt(self, receipt_hash: str) -> Dict:
        """Sometimes we just need a single receipt. We don't bother linking to the node for now."""
        if receipt := self.get_stored_receipt(receipt_hash):
            return receipt
        else:
            receipt = self.fetch_receipt(receipt_hash)
            self.save_receipt(receipt)
            return receipt

    def fetch_receipt(self, receipt_hash: str) -> Dict:
        """Fetch receipt from the API with a given hash."""
        url = f"{self.base_url}/{receipt_hash}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            return self.process_receipt(response.json())
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
                period = receipt.get("period")
                end_time = period.get("end")
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

    def get_stored_receipt(self, receipt_hash: str) -> Dict:
        file_path = self.receipts_dir / f"{receipt_hash}.json"
        if file_path.is_file():
            try:
                with open(file_path, "r") as f:
                    return json.load(f)
            except (IOError, json.JSONDecodeError) as e:
                print(f"Error reading receipt {symlink}: {str(e)}")
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

    def fetch_and_process_node(self, node_id: int):
        """Process all receipts for a given node."""
        try:
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

        except Exception as e:
            print(f"Error processing node {node_id}: {str(e)}")

    def get_node_receipts(self, node_id: int) -> List[Dict]:
        """Checks for a timestamp on disk, indicating that receipts were
        previously fetched and cached. If found, and a new period hasn't
        elapsed, returns the cached receipts, because it's not possible for a
        new receipt to exist yet. Otherwise, fetch receipts from the API.
        """
        timestamp = self.get_stored_timestamp(node_id)
        print(timestamp)
        if not timestamp or time.time() > timestamp + STANDARD_PERIOD_DURATION:
            return self.fetch_and_process_node(node_id)
        else:
            return self.get_stored_receipts(node_id)


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

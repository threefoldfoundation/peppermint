import sqlite3
import json
import time
from typing import List, Dict, Tuple
from grid3.minting.period import Period, STANDARD_PERIOD_DURATION
from grid3.network import GridNetwork
from receipts import NodeMintingPeriod, make_node_minting_periods

def get_all_node_ids(db_path: str = "receipts.db") -> List[int]:
    """Get all unique node IDs from receipts table"""
    conn = sqlite3.connect(db_path)
    cursor = conn.execute("SELECT DISTINCT node_id FROM receipts")
    return [row[0] for row in cursor.fetchall()]

def get_node_receipts(db_path: str, node_id: int) -> List[Dict]:
    """Get all stored receipts for a node"""
    conn = sqlite3.connect(db_path)
    cursor = conn.execute(
        "SELECT receipt_data FROM receipts WHERE node_id = ?", (node_id,)
    )
    return [json.loads(row[0]) for row in cursor.fetchall()]

def calculate_node_uptime(node_period: NodeMintingPeriod) -> float:
    """Calculate uptime percentage for a single node period"""
    if node_period.correct_receipt:
        receipt = node_period.correct_receipt
    elif node_period.minted_receipt:
        receipt = node_period.minted_receipt

    # Calculate uptime percentage by dividing measured uptime by period duration
    period_duration = receipt["period"]["end"] - receipt["period"]["start"]
    return receipt["measured_uptime"] / period_duration

def calculate_average_uptime(node_id: int, db_path: str = "receipts.db") -> float:
    """Calculate average uptime for a node across all periods"""
    receipts = get_node_receipts(db_path, node_id)
    if not receipts:
        return 0.0

    periods = make_node_minting_periods(node_id, receipts)
    total_uptime = 0.0
    valid_periods = 0

    for period in periods:
        if not period.minted_receipt and not period.correct_receipt:
            continue

        uptime = calculate_node_uptime(period)

        # For $REASON, some uptimes are up to 2% or so above the associated
        # period duration. For $OTHER_REASON, some uptimes are upto about 50%
        # off, during the node's first period in the example I checked. We
        # normalize these to 100% and also throw out anything that's way out of
        # range, since for $YET_ANOTHER_REASON, some uptime figures are hugely
        # inflated.
        if 1 < uptime <= 2:
            total_uptime += 1
            valid_periods += 1
        elif uptime <= 1:
            total_uptime += uptime
            valid_periods += 1

        if node_id == 1:
            print(uptime, total_uptime, valid_periods)

    return total_uptime / valid_periods if valid_periods > 0 else 0.0

def rank_nodes(db_path: str = "receipts.db") -> List[Tuple[int, float]]:
    """Rank all nodes by their average uptime"""
    # node_ids = get_all_node_ids(db_path)
    mainnet = GridNetwork()
    nodes = mainnet.graphql.nodes(["nodeID"], updatedAt_gt=int(time.time() - 24 * 60 * 60))
    node_ids = [n["nodeID"] for n in nodes]
    ranked_nodes = []

    count = 0
    for node_id in node_ids:
        avg_uptime = calculate_average_uptime(node_id, db_path)
        if avg_uptime > 0:  # Only include nodes with some uptime
            ranked_nodes.append((node_id, avg_uptime))

        count += 1
        if count % 100 == 0:
            print(f"Processed {count} nodes")

    # Sort by average uptime descending
    ranked_nodes.sort(key=lambda x: x[1], reverse=True)
    return ranked_nodes

def print_rankings(ranked_nodes: List[Tuple[int, float]], top_n: int = 50):
    """Print the top N ranked nodes"""
    print(f"Top {top_n} Nodes by Average Uptime:")
    print("Rank\tNode ID\t\tAverage Uptime")
    print("----------------------------------")
    for rank, (node_id, uptime) in enumerate(ranked_nodes[:top_n], 1):
        print(f"{rank}\t{node_id}\t{uptime:.2%}")

if __name__ == "__main__":
    rankings = rank_nodes()
    print_rankings(rankings)

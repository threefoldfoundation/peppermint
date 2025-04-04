import sqlite3
import json
import time
import argparse
from pathlib import Path
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

def calculate_uptime_stats(node_id: int, db_path: str = "receipts.db") -> Tuple[float, float]:
    """Calculate uptime statistics for a node across all periods.
    Returns tuple of (average_uptime_percentage, total_uptime_seconds)"""
    receipts = get_node_receipts(db_path, node_id)
    if not receipts:
        return (0.0, 0.0)

    periods = make_node_minting_periods(node_id, receipts)
    total_uptime_percentage = 0.0
    total_uptime_seconds = 0.0
    valid_periods = 0

    for period in periods:
        if not period.minted_receipt and not period.correct_receipt:
            continue

        receipt = period.correct_receipt or period.minted_receipt
        uptime_seconds = receipt["measured_uptime"]
        period_duration = receipt["period"]["end"] - receipt["period"]["start"]
        uptime_percentage = uptime_seconds / period_duration

        # For $REASON, some uptimes are up to 2% or so above the associated
        # period duration. For $OTHER_REASON, some uptimes are upto about 50%
        # off, during the node's first period in the example I checked. We
        # normalize these to 100% and also throw out anything that's way out of
        # range, since for $YET_ANOTHER_REASON, some uptime figures are hugely
        # inflated.
        if 1 < uptime_percentage <= 2:
            total_uptime_percentage += 1
            total_uptime_seconds += period_duration
            valid_periods += 1
        elif uptime_percentage <= 1:
            total_uptime_percentage += uptime_percentage
            total_uptime_seconds += uptime_seconds
            valid_periods += 1

    avg_uptime = total_uptime_percentage / valid_periods if valid_periods > 0 else 0.0
    return (avg_uptime, total_uptime_seconds)

def rank_nodes(db_path: str = "receipts.db", node_ids: List[int] = None) -> List[Tuple[int, int, float, float]]:
    """Rank nodes by their average uptime and total uptime

    Args:
        db_path: Path to receipts database
        node_ids: Optional list of node IDs to rank. If None, ranks all active nodes.
    """
    if node_ids is None:
        mainnet = GridNetwork()
        nodes = mainnet.graphql.nodes(["nodeID", "farmID"], updatedAt_gt=int(time.time() - 24 * 60 * 60))
        node_ids = [n["nodeID"] for n in nodes]
        node_farms = {n["nodeID"]: n["farmID"] for n in nodes}
    else:
        mainnet = GridNetwork()
        nodes = mainnet.graphql.nodes(["nodeID", "farmID"], nodeID_in=node_ids)
        node_farms = {n["nodeID"]: n["farmID"] for n in nodes}

    ranked_nodes = []
    max_total_uptime = 0

    # First pass to find maximum total uptime
    for node_id in node_ids:
        avg_uptime, total_uptime = calculate_uptime_stats(node_id, db_path)
        if avg_uptime > 0 and total_uptime > max_total_uptime:
            max_total_uptime = total_uptime

    # Second pass to create ranked list with relative uptime
    for node_id in node_ids:
        avg_uptime, total_uptime = calculate_uptime_stats(node_id, db_path)
        if avg_uptime > 0:  # Only include nodes with some uptime
            relative_uptime = total_uptime / max_total_uptime if max_total_uptime > 0 else 0
            ranked_nodes.append((node_id, node_farms.get(node_id, 0), avg_uptime, total_uptime, relative_uptime))

    # Sort by average uptime descending
    ranked_nodes.sort(key=lambda x: x[1], reverse=True)
    return ranked_nodes

def generate_html(ranked_nodes: List[Tuple[int, int, float, float, float]], output_path: str = "rankings.html", top_n: int = None):
    """Generate an HTML file with sortable table of rankings

    Args:
        ranked_nodes: List of (node_id, avg_uptime, total_uptime) tuples
        output_path: Path to save HTML file
        top_n: Optional limit on number of nodes to show (None shows all)
    """
    display_count = len(ranked_nodes) if top_n is None else min(top_n, len(ranked_nodes))
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Node Uptime Rankings</title>
    <style>
        body {{
            font-family: sans-serif;
            margin: 2em;
            line-height: 1.6;
        }}
        .sort-controls {{
            display: flex;
            justify-content: flex-end;
            align-items: center;
            gap: 1em;
            margin-bottom: 1em;
        }}
        .slider-container {{
            display: flex;
            align-items: center;
            gap: 0.5em;
        }}
        input[type="range"] {{
            -webkit-appearance: none;
            width: 200px;
            height: 4px;
            background: #ddd;
            border-radius: 2px;
        }}
        input[type="range"]::-webkit-slider-thumb {{
            -webkit-appearance: none;
            width: 16px;
            height: 16px;
            background: #0066cc;
            border-radius: 50%;
            cursor: pointer;
        }}
        .slider-labels {{
            display: flex;
            justify-content: space-between;
            width: 200px;
        }}
        button {{
            padding: 0.5em 1em;
            background-color: #0066cc;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
        }}
        button:hover {{
            background-color: #0055aa;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 1em 0;
        }}
        th, td {{
            padding: 8px 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            cursor: pointer;
            background-color: #f8f8f8;
            font-weight: bold;
            position: relative;
        }}
        th:hover {{
            background-color: #eee;
        }}
        th.sorted-asc::after {{
            content: " ↑";
            color: #666;
        }}
        th.sorted-desc::after {{
            content: " ↓";
            color: #666;
        }}
        th.active {{
            background-color: #e0e0e0;
        }}
        tr:hover td {{
            background-color: #f5f5f5;
        }}
        .uptime {{
            width: 150px;
        }}
        a {{
            color: #0066cc;
            text-decoration: none;
        }}
        a:hover {{
            text-decoration: underline;
        }}
    </style>
</head>
<body>
    <h1>{'Top ' + str(display_count) + ' ' if top_n is not None else ''}Nodes by Average Uptime</h1>
    <div class="sort-controls">
        <div class="slider-container">
            <span>Average Uptime</span>
            <input type="range" min="0" max="100" value="0" class="sort-slider">
            <span>Total Uptime</span>
        </div>
        <button onclick="applySort()">Apply Sort</button>
    </div>
    <table id="rankingTable">
        <thead>
            <tr>
                <th onclick="sortTable(0)">Node ID</th>
                <th onclick="sortTable(1)">Farm ID</th>
                <th onclick="sortTable(2)">Average Uptime</th>
                <th onclick="sortTable(3)">Total Uptime</th>
            </tr>
        </thead>
        <tbody>
"""

    for node_id, farm_id, uptime, total_uptime, relative_uptime in ranked_nodes[:top_n] if top_n is not None else ranked_nodes:
        html += f"""            <tr>
                <td><a href="/node/{node_id}">{node_id}</a></td>
                <td><a href="/farm/{farm_id}">{farm_id}</a></td>
                <td class="uptime">{uptime:.2%}</td>
                <td class="uptime">{relative_uptime:.1%} of max</td>
            </tr>
"""

    html += f"""        </tbody>
    </table>
    <script>
        let currentSort = {{
            column: 1, // Default sort by average uptime
            direction: 'desc' // Default descending
        }};

        function sortTable(column) {{
            const table = document.getElementById("rankingTable");
            const rows = Array.from(table.rows).slice(1); // Skip header
            const headers = table.rows[0].cells;

            // Determine sort direction
            let isAsc;
            if (column === currentSort.column) {{
                isAsc = currentSort.direction === 'asc';
            }} else {{
                isAsc = false; // Default to descending for new column
            }}

            // Clear previous sort indicators
            for (let i = 0; i < headers.length; i++) {{
                headers[i].classList.remove('sorted-asc', 'sorted-desc', 'active');
            }}

            // Sort rows
            rows.sort((a, b) => {{
                let x = a.cells[column].textContent;
                let y = b.cells[column].textContent;

                if (column === 0) {{
                    // Sort numbers for node ID
                    return isAsc ? Number(x) - Number(y) : Number(y) - Number(x);
                }} else if (column === 1) {{
                    // Sort numbers for farm ID
                    return isAsc ? Number(x) - Number(y) : Number(y) - Number(x);
                }} else if (column === 2) {{
                    // Sort percentages for average uptime
                    x = parseFloat(x.replace('%', ''));
                    y = parseFloat(y.replace('%', ''));
                    return isAsc ? x - y : y - x;
                }} else {{
                    // Sort percentages for relative uptime
                    x = parseFloat(x.replace('% of max', ''));
                    y = parseFloat(y.replace('% of max', ''));
                    return isAsc ? x - y : y - x;
                }}
            }});

            // Rebuild table with sorted rows
            table.tBodies[0].append(...rows);

            // Update sort indicators
            currentSort = {{
                column: column,
                direction: isAsc ? 'desc' : 'asc'
            }};
            headers[column].classList.add('active', isAsc ? 'sorted-asc' : 'sorted-desc');
        }}

        function applySort() {{
            const slider = document.querySelector('.sort-slider');
            if (slider.value < 50) {{
                sortTable(2); // Sort by average uptime
            }} else {{
                sortTable(3); // Sort by total uptime
            }}
        }}

        // Initialize with default sort
        document.addEventListener('DOMContentLoaded', () => {{
            sortTable(currentSort.column);
        }});
    </script>
</body>
</html>"""

    Path(output_path).write_text(html)
    print(f"Generated HTML rankings at {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Rank nodes by uptime')
    parser.add_argument('node_ids', nargs='*', type=int,
                       help='Optional node IDs to rank (default: rank all active nodes)')
    parser.add_argument('--html', action='store_true',
                       help='Generate HTML output instead of console output')
    parser.add_argument('--output', default='rankings.html',
                       help='HTML output file path (default: rankings.html)')
    parser.add_argument('--top', type=int, default=None,
                       help='Optional limit on number of top nodes to show (default: show all)')
    args = parser.parse_args()

    if args.node_ids:
        rankings = rank_nodes(node_ids=args.node_ids)
    else:
        rankings = rank_nodes()

    if args.html:
        generate_html(rankings, output_path=args.output, top_n=args.top)
    else:
        print(f"Top {args.top} Nodes by Average Uptime:")
        print("Rank\tNode ID\t\tFarm ID\t\tAverage Uptime")
        print("----------------------------------------------")
        for rank, (node_id, farm_id, uptime, total_uptime, relative_uptime) in enumerate(rankings[:args.top], 1):
            print(f"{rank}\t{node_id}\t{farm_id}\t{uptime:.2%}\t{relative_uptime:.1%} of max")

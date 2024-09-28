from datetime import datetime, timedelta
import calendar
import urllib.parse
import requests
from fasthtml.common import *
import grid3.network
from collections import defaultdict

mainnet = grid3.network.GridNetwork()
app, rt = fast_app(live=True)

# Keep a cache of known receipts, keyed by hash.
receipts = {}

@rt("/")
def get():
    return render_main()

@rt("/{select}/{id_input}")
@rt("/{select}/{id_input}/{filter_option}")
def get(req, select: str, id_input: int, filter_option: str = None):
    if select == "node":
        results = [render_receipts(id_input)]
    elif select == "farm":
        nodes = mainnet.graphql.nodes(["nodeID"], farmID_eq=id_input)
        node_ids = sorted([node["nodeID"] for node in nodes])
        results = []
        if filter_option == "nodes":
            for node in node_ids:
                results.append(H2(f"Node {node}"))
                results.append(render_receipts(node))
        elif filter_option == "periods":
            grouped_receipts = group_receipts_by_period(node_ids)
            sorted_periods = sorted(grouped_receipts.keys(), key=lambda x: int(x.split('_')[1]), reverse=True)
            
            for i, period_key in enumerate(sorted_periods):
                period_receipts = grouped_receipts[period_key]
                period_end = datetime.fromtimestamp(int(period_key.split('_')[1]))
                
                # Determine prevalent month
                month_counts = defaultdict(int)
                for receipt_info in period_receipts:
                    receipt = receipt_info["receipt"]
                    start_date = datetime.fromtimestamp(receipt["period"]["start"])
                    end_date = datetime.fromtimestamp(receipt["period"]["end"])
                    days = (end_date - start_date).days + 1
                    for day in range(days):
                        current_date = start_date + timedelta(days=day)
                        month_counts[current_date.month] += 1
                prevalent_month = max(month_counts, key=month_counts.get)
                
                period_month = calendar.month_name[prevalent_month]
                period_year = period_end.year
                results.append(H2(f"Period {len(sorted_periods) - i} ({period_month} {period_year})"))
                rows = [receipt_header()]
                
                # Create a dictionary to store the latest receipt for each node
                latest_node_receipts = {}
                for receipt_info in period_receipts:
                    node_id = receipt_info["node_id"]
                    if node_id not in latest_node_receipts or receipt_info["receipt"]["period"]["end"] > latest_node_receipts[node_id]["receipt"]["period"]["end"]:
                        latest_node_receipts[node_id] = receipt_info

                # Add a row for each node, only if it has valid data
                for node_id in node_ids:
                    if node_id in latest_node_receipts:
                        receipt_info = latest_node_receipts[node_id]
                        row = render_receipt(receipt_info["hash"], receipt_info["receipt"], node_id)
                        if row:  # Only add the row if it's not None
                            rows.append(row)
                
                if len(rows) > 1:  # Only add the table if there are rows besides the header
                    results.append(Table(*rows))
                else:
                    results.append(P("No valid data for this period"))
        else:
            # Default behavior when no filter is selected
            for node in node_ids:
                results.append(H2(f"Node {node}"))
                results.append(render_receipts(node))

    has_result = False
    for result in results:
        if result:
            has_result = True

    if not has_result:
        results = "No receipts found."

    if "hx-request" in req.headers:
        return results
    else:
        return render_main(select, id_input, results, filter_option)

@rt("/node/{node_id}/{rhash}")
def get(node_id: int, rhash: str):
    details = render_details(rhash)

    if type(details) is not str:
        if receipts[rhash]["node_id"] != node_id:
            details = "Hash doesn't match node id"

    return render_main(id_input=node_id, result=details)

@rt("/details")
def get(rhash: str):
    return render_details(rhash)

def process_receipt(receipt):
    # Flatten receipts so the type is an attribute
    if "Minting" in receipt:
        r = receipt["Minting"]
        r["type"] = "Minting"
    elif "Fixup" in receipt:
        r = receipt["Fixup"]
        r["type"] = "Fixup"
    return r

def render_details(rhash):
    global receipts
    if rhash not in receipts:
        response = requests.get(
            f"https://alpha.minting.tfchain.grid.tf/api/v1/receipt/{rhash}"
        )
        if not response.ok:
            return "Hash not found"
        else:
            receipts[rhash] = process_receipt(response.json())

    receipt = receipts[rhash]
    return [
        H2(f"Node {receipt['node_id']} Details"),
        Table(
            receipt_header(),
            render_receipt(rhash, receipt, receipt['node_id'], False),
            *render_receipt_row2(receipt),
        ),
    ]

def render_main(select="node", id_input=None, result="", filter_option=None):
    farm_selected = select == "farm"
    return Titled(
        "Fetch Minting Receipts",
        Form(
            hx_get=f"/{select}/{id_input}",
            hx_push_url=f"/{select}/{id_input}",
            hx_target="#result",
            hx_trigger="submit",
            onsubmit="document.getElementById('result').innerHTML = 'Loading...'",
            oninput="""
                    const sel = this.elements.select.value;
                    const id = this.elements.id_input.value;
                    const filter = this.elements.filter_select ? this.elements.filter_select.value : '';
                    const path = '/' + sel + '/' + id + (filter ? '/' + filter : '');
                    this.setAttribute('hx-get', path);
                    this.setAttribute('hx-push-url', path);
                    htmx.process(this);
                    // Setting `value` and `selected` help make the form data persistent when using the browser back button.
                    this.elements.id_input.setAttribute('value', id)
                    for (child of this.elements.select.children){
                        if (child.value == sel) {
                            child.setAttribute('selected', 'selected')
                        } else {
                            child.removeAttribute('selected')
                        }
                    }
                    if (this.elements.filter_select) {
                        for (child of this.elements.filter_select.children){
                            if (child.value == filter) {
                                child.setAttribute('selected', 'selected')
                            } else {
                                child.removeAttribute('selected')
                            }
                        }
                    }
                    this.elements.id_input.getAttribute('value')
                    """,
        )(
            Div(
                Div(
                    Input(
                        type="number",
                        id="id_input",
                        placeholder=42,
                        value=id_input,
                        required="true",
                    ),
                    style="display: inline-block",
                ),
                Div(
                    Select(
                        Option("Node ID", value="node", selected=select == "node"),
                        Option("Farm ID", value="farm", selected=select == "farm"),
                        id="select",
                        onchange="toggleFilterOptions(this.value)"
                    ),
                    style="display: inline-block",
                ),
                Div(
                    Select(
                        Option("Filter by Node IDs", value="nodes", selected=filter_option == "nodes"),
                        Option("Filter by Periods", value="periods", selected=filter_option == "periods"),
                        id="filter_select",
                        style="display: none" if not farm_selected else "inline-block"
                    ),
                    style="display: inline-block",
                ),
                Div(
                    Button("Go", type="submit"),
                    style="display: inline-block",
                ),
            ),
        ),
        Br(),
        Div(*result, id="result"),
        Style(
            """
            table tr:hover td {
            background: #efefef;
            cursor: pointer;
            }
            """
        ),
        Script("""
            function toggleFilterOptions(value) {
                var filterSelect = document.getElementById('filter_select');
                if (value === 'farm') {
                    filterSelect.style.display = 'inline-block';
                } else {
                    filterSelect.style.display = 'none';
                }
            }
        """)
    )

def render_receipts(node_id):
    global receipts
    if node_id:
        try:
            node_id = int(node_id)
        except ValueError:
            return "Please enter a valid node id"
    else:
        return "Please enter a valid node id"

    try:
        response = requests.get(
            f"https://alpha.minting.tfchain.grid.tf/api/v1/node/{node_id}"
        ).json()
    except requests.exceptions.JSONDecodeError:
        return None

    rows = [receipt_header()]
    for receipt in response:
        rhash, receipt_data = receipt["hash"], receipt["receipt"]
        receipt_data = process_receipt(receipt_data)
        if rhash not in receipts:
            receipts[rhash] = receipt_data
        if receipt_data["type"] == "Minting":
            row = render_receipt(rhash, receipt_data, node_id)
            if row:  # Only add the row if it's not None
                rows.append(row)
    return Table(*rows) if len(rows) > 1 else P("No valid data for this node")

def group_receipts_by_period(node_ids):
    global receipts
    periods = {}

    for node_id in node_ids:
        try:
            response = requests.get(
                f"https://alpha.minting.tfchain.grid.tf/api/v1/node/{node_id}"
            ).json()
        except requests.exceptions.JSONDecodeError:
            continue

        for receipt in response:
            rhash, receipt_data = receipt["hash"], receipt["receipt"]
            receipt_data = process_receipt(receipt_data)
            if receipt_data["type"] == "Minting":
                end_time = receipt_data["period"]["end"]
                
                period_key = f"period_{end_time}"
                if period_key not in periods:
                    periods[period_key] = []
                
                if rhash not in receipts:
                    receipts[rhash] = receipt_data

                periods[period_key].append({
                    "hash": rhash,
                    "receipt": receipt_data,
                    "node_id": node_id
                })

    return periods

def render_receipt(rhash, r, node_id, details=True):
    uptime = round(r["measured_uptime"] / (30.45 * 24 * 60 * 60) * 100, 2)
    tft_minted = r["reward"]["tft"] / 1e7

    # Check if both uptime and TFT minted are 0.0, if so, return None
    if uptime == 0.0 and tft_minted == 0.0:
        return None

    if details:
        row = Tr(
            hx_get="/details",
            hx_target="#result",
            hx_trigger="click",
            hx_vals={"rhash": rhash},
            hx_push_url=f"/node/{r['node_id']}/{rhash}",
        )
    else:
        row = Tr()
    return row(
        Td(node_id),
        Td(datetime.fromtimestamp(r["period"]["start"]).date()),
        Td(datetime.fromtimestamp(r["period"]["end"]).date()),
        Td(f"{uptime:.2f}%"),
        Td(f"{tft_minted:.7f}"),
    )

def render_receipt_row2(r):
    return [
        Tr(
            Th(Br(), Strong("CU")),
            Th(Br(), Strong("SU")),
            Th(Br(), Strong("NU")),
            Th(Br(), Strong("Certification")),
        ),
        Tr(
            Td(r["cloud_units"]["cu"]),
            Td(r["cloud_units"]["su"]),
            Td(r["cloud_units"]["nu"]),
            Td(r["node_type"]),
        ),
    ]

def receipt_header():
    return Tr(
        Th(Strong("Node ID")),
        Th(Strong("Period Start")),
        Th(Strong("Period End")),
        Th(Strong("Uptime")),
        Th(Strong("TFT Minted")),
    )

serve()
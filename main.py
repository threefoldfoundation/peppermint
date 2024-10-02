import sqlite3, concurrent.futures
from datetime import datetime
from typing import Tuple

import requests
from fasthtml.common import *
import grid3.network, grid3.minting

import minting_lite

RECEIPTS_URL = "https://alpha.minting.tfchain.grid.tf/api/v1/"

mainnet = grid3.network.GridNetwork()
app, rt = fast_app(live=True)


# Keep a cash of known receipts, keyed by hash. Ideally we'd also keep record of which node ids have been queried and when, to know if there's a possibility of more recipts we didn't cache yet
receipts = {}


@rt("/")
def get():
    return render_main()


@rt("/{select}/{id_input}")
def get(req, select: str, id_input: int):
    if select == "node":
        results = [render_receipts(fetch_node_receipts(id_input))]

    elif select == "farm":
        farm_receipts = fetch_farm_receipts(id_input)
        results = []
        for node, receipts in farm_receipts:
            if receipts:
                results.append(H2(f"Node {node}"))
                results.append(render_receipts(receipts))

    has_result = False
    for result in results:
        if result:
            has_result = True

    if not has_result:
        results = "No receipts found."

    if "hx-request" in req.headers:
        return results
    else:
        return render_main(select, id_input, results)


@rt("/node/{node_id}/{rhash}")
def get(node_id: int, rhash: str):
    details = render_details(rhash)

    # Details can be an error which is a string. Also the receipt might not be cached before we call render_details above. This whole thing is kinda ugly... TODO: refactor the caching and error handling
    if type(details) is not str:
        if receipts[rhash]["node_id"] != node_id:
            details = "Hash doesn't match node id"

    return render_main(id_input=node_id, result=details)


@rt("/details")
def get(rhash: str):
    return render_details(rhash)


def fetch_farm_receipts(farm_id: int) -> List[Tuple[int, list | None]]:
    nodes = mainnet.graphql.nodes(["nodeID"], farmID_eq=farm_id)
    node_ids = [node["nodeID"] for node in nodes]
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=20)
    responses = []
    for node_id in node_ids:
        url = RECEIPTS_URL + f"node/{node_id}"
        responses.append((node_id, pool.submit(requests.get, url)))

    processed_responses = []
    for node_id, response in responses:
        try:
            response = response.result().json()
            response = process_node_receipts(response)
        except requests.exceptions.JSONDecodeError:
            response = None

        processed_responses.append((node_id, response))

    # Sorts by node id
    return sorted(processed_responses)


def fetch_node_receipts(node_id):
    try:
        response = requests.get(RECEIPTS_URL + f"node/{node_id}").json()
        response = process_node_receipts(response)
    except requests.exceptions.JSONDecodeError:
        response = None

    return response


def process_receipt(rhash, receipt):
    # Flatten receipts so the type is an attribute, and also include the hash
    if "Minting" in receipt:
        r = receipt["Minting"]
        r["type"] = "Minting"
    elif "Fixup" in receipt:
        r = receipt["Fixup"]
        r["type"] = "Fixup"
    r["hash"] = rhash
    return r


def process_node_receipts(items):
    return [process_receipt(item["hash"], item["receipt"]) for item in items]


def render_details(rhash):
    if rhash not in receipts:
        response = requests.get(
            f"https://alpha.minting.tfchain.grid.tf/api/v1/receipt/{rhash}"
        )
        if not response.ok:
            return "Hash not found"
        else:
            receipts[rhash] = process_receipt(rhash, response.json())

    receipt = receipts[rhash]
    return [
        H2(f"Node {receipt['node_id']} Details"),
        Table(
            receipt_header(),
            render_receipt(receipt, False),
            *render_receipt_row2(receipt),
        ),
        Br(),
        H3("Uptime Events"),
        mintinglite(receipt),
    ]


def render_main(select="node", id_input=None, result=""):
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
                    const path = '/' + sel + '/' + id;
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
                    ),
                    style="display: inline-block",
                ),
                Div(
                    Button("Go", type="submit"),
                    style="display: inline-block",
                ),
            ),
            # CheckboxX(id="fixups", label="Show fixups"),
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
    )


def render_receipts(receipts):
    rows = [receipt_header()]
    for receipt in receipts:
        if receipt["type"] == "Minting":
            rows.append(render_receipt(receipt))
    return Table(*rows)


def render_receipt(r, details=True):
    uptime = round(r["measured_uptime"] / (30.45 * 24 * 60 * 60) * 100, 2)
    if details:
        row = Tr(
            hx_get="/details",
            hx_target="#result",
            hx_trigger="click",
            hx_vals={"rhash": r["hash"]},
            hx_push_url=f"/node/{r['node_id']}/{r['hash']}",
        )
    else:
        row = Tr()
    return row(
        Td(datetime.fromtimestamp(r["period"]["start"]).date()),
        Td(datetime.fromtimestamp(r["period"]["end"]).date()),
        Td(f"{uptime}%"),
        Td(r["reward"]["tft"] / 1e7),
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
        Th(Strong("Period Start")),
        Th(Strong("Period End")),
        Th(Strong("Uptime")),
        Th(Strong("TFT Minted")),
    )


def mintinglite(receipt):
    con = sqlite3.connect("tfchain.db")
    # Check if our db contains all events for the period in question
    wiggle = 12  # Two blocks
    node_id = receipt["node_id"]
    has_start = con.execute(
        "SELECT 1 FROM PowerState WHERE node_id=? AND timestamp>=?  AND timestamp<=?",
        [
            node_id,
            (receipt["period"]["start"] - wiggle),
            (receipt["period"]["start"] + wiggle),
        ],
    ).fetchone()

    has_end = con.execute(
        "SELECT 1 FROM PowerState WHERE node_id=? AND timestamp>=?  AND timestamp<=?",
        [
            node_id,
            (receipt["period"]["end"] - wiggle),
            (receipt["period"]["end"] + wiggle),
        ],
    ).fetchone()

    if not has_start and has_end:
        return None

    period = grid3.minting.Period(receipt["period"]["start"] + wiggle)
    node = minting_lite.check_node(con, node_id, period, False)
    header = Tr(
        *[
            Th(Strong(label))
            for label in [
                "Date",
                "Timestamp",
                "Uptime credited",
                "Elapsed time",
                "Downtime",
                "Note",
            ]
        ]
    )
    rows = [header]
    for e in node.events:
        rows.append(Tr(*[Th(item) for item in e]))
    return Table(*rows)


serve()

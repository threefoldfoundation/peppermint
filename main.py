import sqlite3, concurrent.futures, threading, os
from datetime import datetime
from typing import Tuple

import requests
from fasthtml.common import *
import grid3.network, grid3.minting.period, grid3.minting.mintingnode

from lightdark import LightDarkScript, LightLink, DarkLink
from receipts import ReceiptHandler

RECEIPTS_URL = "https://alpha.minting.tfchain.grid.tf/api/v1/"
CSV_DIR = "csvs"

os.makedirs(CSV_DIR, exist_ok=True)

mainnet = grid3.network.GridNetwork()
# We can run into some trouble with multiple threads trying to use gql at the same time. Bit primitive, but we just lock it for now
gql_lock = threading.Lock()
app, rt = fast_app(live=True)

receipt_handler = ReceiptHandler()


@rt("/")
def get(select: str = "node", id_input: int = None):
    if not id_input:
        return render_main(select)
    else:
        page = render_main(select, id_input, loading=True)
        headers = HtmxResponseHeaders(push_url=f"/{select}/{id_input}")
        return page, headers


@rt("/{select}/")
def get(select: str):
    return render_main(select)


@rt("/csv/{rhash}")
def get(rhash: str):
    node = mintinglite(receipt_handler(rhash))
    filename = f"node{node.id}.csv"
    path = "csvs/" + filename
    node.write_csv(path)
    return FileResponse(path, filename=filename)


@rt("/{select}/{id_input}")
def get(req, select: str, id_input: int):
    if select == "node":
        results = [render_receipts(receipt_handler.get_node_receipts(id_input))]

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
def get(req, node_id: int, rhash: str):
    details = render_details(rhash)

    # Details can be an error which is a string. Also the receipt might not be cached before we call render_details above. This whole thing is kinda ugly... TODO: refactor the caching and error handling
    if type(details) is not str:
        if receipt_handler.get_receipt(rhash)["node_id"] != node_id:
            details = "Hash doesn't match node id"

    if "hx-request" in req.headers:
        return details
    else:
        return render_main(id_input=node_id, result=details)


def fetch_farm_receipts(farm_id: int) -> List[Tuple[int, list | None]]:
    with gql_lock:
        nodes = mainnet.graphql.nodes(["nodeID"], farmID_eq=farm_id)

    node_ids = [node["nodeID"] for node in nodes]
    if sum([1 for n in node_ids if not receipt_handler.has_node_receipts(n)]) > 1:
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=20)
        receipt_lists = list(pool.map(receipt_handler.get_node_receipts, node_ids))
    else:
        receipt_lists = [receipt_handler.get_node_receipts(n) for n in node_ids]

    processed_responses = []
    for receipt_list in receipt_lists:
        node_id = receipt_list[0]['node_id']
        processed_responses.append((node_id, receipt_list))

    # Sorts by node id
    return sorted(processed_responses)


def render_details(rhash):
    receipt = receipt_handler.get_receipt(rhash)
    node = mintinglite(receipt)
    heading = H3("Uptime Events")
    if node:
        details = [
            Div(style="display: flex; align-items: baseline;")(
                heading,
                A(style="margin-left:auto;", href=f"/csv/{rhash}", download=True)(
                    "Download CSV"
                ),
            ),
            render_minting_events(node),
        ]
    else:
        details = [
            heading,
            "Data not available for this period",
        ]

    response = [
        H2(f"Node {receipt['node_id']} Details"),
        Table(
            receipt_header(),
            render_receipt(receipt, False),
            *render_receipt_row2(receipt),
        ),
        Br(),
        *details,
    ]

    return response


def render_main(select="node", id_input=None, result="", loading=False):
    # If the user hit the /node or /farm paths, we want to set the drop down
    # but clear the url since state on page can diverge
    if not id_input:
        onload = "history.replaceState(null, '', '/')"
    else:
        onload = ""

    # Lazy load results and display loading message
    if loading:
        result = [
            P(
                hx_get=f"/{select}/{id_input}",
                # hx_target="body",
                hx_swap="outerHTML",
                hx_trigger="load",
            )("Loading...")
        ]

    title = "Fetch Minting Receipts"
    return (
        Title(title),
        Body(onload=onload)(
            # PicoCSS container, for centered view
            Main(cls="container")(
                Div(id="header-bar", hx_preserve=True, style="display: flex")(
                    H1(title),
                    Div(style="display:flex; flex-direction:column; margin-left:auto")(
                        Div(style="align-self:center")("Join the:"),
                        Div(LightLink("Light side"), " | ", DarkLink("Dark side")),
                        LightDarkScript(),
                    ),
                ),
                Form(
                    hx_get="/",
                    hx_target="body",
                    hx_trigger="submit",
                )(
                    Fieldset(role="group", style="width:0px")(
                        Input(
                            type="number",
                            id="id_input",
                            placeholder=42,
                            value=id_input,
                            required="true",
                            style="width: 150px",
                        ),
                        Select(
                            Option("Node ID", value="node", selected=select == "node"),
                            Option("Farm ID", value="farm", selected=select == "farm"),
                            id="select",
                            style="width: 150px",
                        ),
                        Button("Go", type="submit"),
                    ),
                    # CheckboxX(id="fixups", label="Show fixups"),
                ),
                Br(),
                Div(*result, id="result"),
                Style(
                    """
            table.hover tr:hover td {
            background: var(--pico-text-selection-color);
            cursor: pointer;
            }
            """
                ),
            )
        ),
    )


def render_minting_events(node):
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
        rows.append(Tr(*[Td(item) for item in e]))
    return Table(*rows)


def render_receipts(receipts):
    receipts = reversed(sorted(receipts, key=lambda x: x["period"]["start"]))
    rows = [receipt_header()]
    for receipt in receipts:
        if receipt["type"] == "Minting":
            rows.append(render_receipt(receipt))
    return Table(*rows, cls="hover")


def render_receipt(r, details=True):
    uptime = round(r["measured_uptime"] / (30.45 * 24 * 60 * 60) * 100, 2)
    if details:
        row = Tr(
            hx_get=f"/node/{r['node_id']}/{r['hash']}",
            hx_target="#result",
            hx_trigger="click",
            hx_push_url="true",
            hx_swap="show:top",
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
    # For testing without db file present
    if not os.path.exists("tfchain.db"):
        return None

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

    # has_end = con.execute(
    #     "SELECT 1 FROM PowerState WHERE node_id=? AND timestamp>=?  AND timestamp<=?",
    #     [
    #         node_id,
    #         (receipt["period"]["end"] - wiggle),
    #         (receipt["period"]["end"] + wiggle),
    #     ],
    # ).fetchone()

    # Generally we won't have any partial periods, except for the ongoing period, due to default behavior of the minting data ingester. So this is a sufficient check that there's some data to show
    if not has_start:
        return None
    else:
        period = grid3.minting.period.Period(receipt["period"]["start"] + wiggle)
        node = grid3.minting.mintingnode.check_node(con, node_id, period)
        return node


serve()

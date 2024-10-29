import sqlite3, concurrent.futures, threading, os
from datetime import datetime
from typing import Tuple

from fasthtml.common import *
import grid3.network, grid3.minting.mintingnode
from grid3.minting.period import Period

from lightdark import LightDarkScript, LightLink, DarkLink
from receipts import STANDARD_PERIOD_DURATION, ReceiptHandler, make_period_receipts

try:
    from config import LIVE_RELOAD
except:
    print("Error loading config file, setting live reload to True")
    LIVE_RELOAD = True


RECEIPTS_URL = "https://alpha.minting.tfchain.grid.tf/api/v1/"
CSV_DIR = "csvs"

# Technically our notion of when minting periods start and end should be
# identical to the minting code, but just in case, we use a wiggle factor to
# smooth out any small deviations
WIGGLE = 10
TFT_DIVISOR = 1e7  # Number of decimal places, as used on tfchain


os.makedirs(CSV_DIR, exist_ok=True)

mainnet = grid3.network.GridNetwork()
# We can run into some trouble with multiple threads trying to use gql at the
# same time. Bit primitive, but we just lock it for now
gql_lock = threading.Lock()
app, rt = fast_app(live=LIVE_RELOAD)

receipt_handler = ReceiptHandler()


@rt("/")
def get(
    select: str = "node",
    id_input: int = None,
    sort_by: str = "node",
    show_empty: bool = False,
):
    if not id_input:
        return render_main(select)
    else:
        page = render_main(select, id_input, show_empty, sort_by, loading=True)
        headers = HtmxResponseHeaders(
            push_url=make_url(select, id_input, show_empty, sort_by)
        )
        return page, headers


@rt("/{select}/")
def get(select: str):
    return render_main(select)


@rt("/csv/{rhash}")
def get(rhash: str):
    node = mintinglite(receipt_handler.get_receipt(rhash))
    filename = f"node{node.id}.csv"
    path = "csvs/" + filename
    node.write_csv(path)
    return FileResponse(path, filename=filename)


@rt("/node/{node_id}")
def get(req, node_id: int, show_empty: bool = False):
    receipts = make_period_receipts(receipt_handler.get_node_receipts(node_id))
    if not receipts:
        results = "No receipts found."
    else:
        results = [
            H2(f"Node {node_id}"),
            render_receipt_overview(receipts, "node", show_empty),
        ]

    if "hx-request" in req.headers:
        return results
    else:
        return render_main("node", node_id, result=results)


@rt("/farm/{farm_id}")
def get(req, farm_id: int, sort_by: str = "node", show_empty: bool = False):
    farm_receipts = fetch_farm_receipts(farm_id)
    results = []
    if sort_by == "node":
        for node_id, receipts in farm_receipts:
            if receipts:
                results.append(H2(f"Node {node_id}"))
                results.append(render_receipt_overview(receipts, sort_by, show_empty))

    elif sort_by == "period":
        receipts_by_period = {}
        for node, receipts in farm_receipts:
            for receipt in receipts:
                receipts_by_period.setdefault(receipt.period.offset, []).append(receipt)
        for offset, receipts in reversed(sorted(receipts_by_period.items())):
            period = Period(offset=offset)
            results.append(H2(f"{period.month_name} {period.year}"))
            results.append(render_receipt_overview(receipts, sort_by, show_empty))
    if not results:
        results = "No receipts found."

    if "hx-request" in req.headers:
        return results
    else:
        return render_main("farm", farm_id, show_empty, sort_by, results)


@rt("/node/{node_id}/{rhash}")
def get(req, node_id: int, rhash: str):
    details = render_details(rhash)

    # Details can be an error which is a string. Also the receipt might not be cached before we call render_details above. This whole thing is kinda ugly... TODO: refactor error handling
    if type(details) is not str:
        if receipt_handler.get_receipt(rhash)["node_id"] != node_id:
            details = "Hash doesn't match node id"

    if "hx-request" in req.headers:
        return details
    else:
        return render_main(id_input=node_id, result=details)


def make_url(select, id_input, show_empty, sort_by):
    if select == "node":
        return f"/{select}/{id_input}?show_empty={show_empty}"
    elif select == "farm":
        return f"/{select}/{id_input}?sort_by={sort_by}&show_empty={show_empty}"


def fetch_farm_receipts(farm_id: int) -> List[Tuple[int, list | None]]:
    with gql_lock:
        nodes = mainnet.graphql.nodes(["nodeID"], farmID_eq=farm_id)

    node_ids = [node["nodeID"] for node in nodes]

    # If all or most of the nodes are caught up in the cache, then the thread pool might do more harm than good. But this is nice and simple
    if len(node_ids) > 1:
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=20)
        receipt_lists = list(pool.map(receipt_handler.get_node_receipts, node_ids))
    else:
        # TODO: This throws an error if the nodes list is empty. We need to alert the user about the problem
        receipt_lists = [receipt_handler.get_node_receipts(node_ids[0])]

    processed_responses = []
    for receipt_list in receipt_lists:
        node_id = receipt_list[0]["node_id"]
        processed_responses.append((node_id, make_period_receipts(receipt_list)))

    # Sorts by node id
    return sorted(processed_responses)


def render_main(
    select="node",
    id_input=None,
    show_empty=False,
    sort_by="node",
    result="",
    loading=False,
):
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
                hx_get=make_url(select, id_input, show_empty, sort_by),
                # hx_target="body",
                hx_swap="outerHTML",
                hx_trigger="load",
            )("Loading...")
        ]

    title = "Fetch Minting Receipts"
    return (
        Title(title),
        Body(onload=onload, style="height: 100vh;")(
            # PicoCSS container, for centered view
            Main(cls="container")(
                Div(id="header-bar", hx_preserve=True, style="display: flex")(
                    H1(title),
                    Div(style="display:flex; flex-direction:column; margin-left:auto")(
                        Small(style="align-self:center")("Join the:"),
                        Small(LightLink("Light side"), " | ", DarkLink("Dark side")),
                        LightDarkScript(),
                    ),
                ),
                Form(
                    hx_get="/",
                    hx_target="body",
                    hx_trigger="submit",
                )(
                    Fieldset(role="group", style="width: fit-content")(
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
                            onchange="document.getElementById('sort_by').hidden = this.value != 'farm'",
                        ),
                        Button("Go", type="submit"),
                    ),
                    CheckboxX(
                        id="show_empty",
                        value="True",
                        label="Show empty periods",
                        checked=show_empty,
                    ),
                    Fieldset(id="sort_by", hidden=select != "farm")(
                        Legend("Sort by:"),
                        Input(
                            type="radio",
                            id="node",
                            value="node",
                            name="sort_by",
                            checked=sort_by == "node",
                        ),
                        Label("Node", fr="node"),
                        Input(
                            type="radio",
                            id="period",
                            value="period",
                            name="sort_by",
                            checked=sort_by == "period",
                        ),
                        Label("Period", fr="period"),
                    ),
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
            ),
            Footer(
                cls="container",
                style="position: sticky; top: 100vh; text-align: center",
                # Only show the footer on the "home page"
                hidden=bool(id_input),
            )(
                Small(
                    Strong("Peppermint "),
                    "- ",
                    Em("ThreeFold minting data with a bit of spice"),
                    Br(),
                    "Made with ❤️, FastHTML, htmx, and PicoCSS",
                    Br(),
                    "For source code and issues: ",
                    A(
                        href="https://github.com/scottyeager/peppermint",
                        target="_blank",
                        rel="noopener noreferrer",
                    )("Github"),
                ),
            ),
        ),
    )


def render_receipt_overview(receipts, sort_by, show_empty):
    if not show_empty:
        receipts = [r for r in receipts if not r.empty]
    if sort_by == "node":
        receipts = reversed(sorted(receipts, key=lambda x: x.period.start))
        rows = [receipt_header_node()]
        last_year = None
        for receipt in receipts:
            show_year = last_year != receipt.period.year
            rows.append(render_receipt_row(receipt, sort_by, show_year))
            last_year = receipt.period.year

    elif sort_by == "period":
        receipts = sorted(receipts, key=lambda x: x.node_id)
        rows = [receipt_header_period()]
        for receipt in receipts:
            rows.append(render_receipt_row(receipt, sort_by))

    return Table(*rows, cls="hover")


def render_receipt_row(period_receipt, sort_by="node", show_year=True):
    # The fixup receipt contains the hashes of the original and corrected
    # receipts, so using its hash here is a convenient way to encode that

    if period_receipt.fixup_receipt:
        rhash = period_receipt.fixup_receipt["hash"]
    else:
        rhash = period_receipt.minted_receipt["hash"]

    # For rendering the details on the overview, we'll use the corrected
    # receipt if available, since that represents the final state of the node's
    # minting for the period. Sometimes only the correct_rectip is available,
    # for whatever reason

    if period_receipt.correct_receipt:
        r = period_receipt.correct_receipt
    elif period_receipt.minted_receipt:
        r = period_receipt.minted_receipt
    else:
        # So far I didn't see this one
        return Tr(Td("Data not available"))

    row = Tr(
        hx_get=f"/node/{r['node_id']}/{rhash}",
        hx_target="#result",
        hx_trigger="click",
        hx_push_url="true",
        hx_swap="show:top",
    )

    if sort_by == "node":
        if show_year:
            elements = [
                Td(f"{period_receipt.period.year}"),
                Td(period_receipt.period.month_name),
            ]
        else:
            elements = [
                Td(),
                Td(period_receipt.period.month_name),
            ]
    elif sort_by == "period":
        elements = [
            Td(r["node_id"]),
        ]

    uptime = round(r["measured_uptime"] / STANDARD_PERIOD_DURATION * 100, 2)
    if uptime <= 100:
        elements.append(Td(f"{uptime}%"))
    else:
        # Some receipts have uptime figures that are way too large
        elements.append(Td("Data not available"))

    elements.append(Td(round(r["reward"]["tft"] / TFT_DIVISOR, 2)))
    elements.append(Td("✔️" if period_receipt.fixup_receipt else ""))
    return row(*elements)


def render_details(rhash):
    receipt = receipt_handler.get_receipt(rhash)
    response = [H2(f"Node {receipt['node_id']} Details")]
    if receipt["type"] == "Fixup":
        minted_receipt = receipt_handler.get_receipt(receipt["minted_receipt"])
        correct_receipt = receipt_handler.get_receipt(receipt["correct_receipt"])

        # It's possible that either the minted_receipt or the correct_receipt
        # is missing from the receipt API. In that case, fill in the details we
        # have from the fixup receipt
        response.append(H3("Corrected Receipt"))
        if correct_receipt:
            response.append(render_receipt_detail(correct_receipt))
        else:
            response.append(render_fixup_detail(receipt, "correct"))

        response.append(H3("Original Receipt"))
        if minted_receipt:
            response.append(render_receipt_detail(minted_receipt))
        else:
            response.append(render_fixup_detail(receipt, "minted"))
    else:
        response.append(render_receipt_detail(receipt))

    response.append(Br())

    node = mintinglite(receipt)
    heading = H3("Uptime Events")
    if node:
        uptime_events = [
            Div(style="display: flex; align-items: baseline;")(
                heading,
                A(style="margin-left:auto;", href=f"/csv/{rhash}", download=True)(
                    "Download CSV"
                ),
            ),
            render_uptime_events(node),
        ]
    else:
        uptime_events = [
            heading,
            "Data not available for this period",
        ]

    response.extend(uptime_events)

    return response


def render_receipt_detail(r):
    rows = [receipt_header_details()]
    uptime = round(r["measured_uptime"] / STANDARD_PERIOD_DURATION * 100, 2)

    rows.append(
        Tr(
            Td(datetime.fromtimestamp(r["period"]["start"]).date()),
            Td(datetime.fromtimestamp(r["period"]["end"]).date()),
            # Some receipts have uptime figures that are way too large
            Td(f"{uptime}%") if uptime <= 100 else Td("Data not available"),
            Td(round(r["reward"]["tft"] / TFT_DIVISOR, 2)),
        )
    )

    rows.append(
        Tr(
            Th(Br(), Strong("CU")),
            Th(Br(), Strong("SU")),
            Th(Br(), Strong("NU")),
            Th(Br(), Strong("Certification")),
        )
    )

    rows.append(
        Tr(
            Td(r["cloud_units"]["cu"]),
            Td(r["cloud_units"]["su"]),
            Td(r["cloud_units"]["nu"]),
            Td(r["node_type"]),
        )
    )
    return Table(*rows)


def render_fixup_detail(r, rtype):
    """Some receipts are missing from the API. In that case, render a limited version of what the receipt would contain based on the info in the fixup receipt."""
    rows = [receipt_header_details()]

    rows.append(
        Tr(
            Td(datetime.fromtimestamp(r["period"]["start"]).date()),
            Td(datetime.fromtimestamp(r["period"]["end"]).date()),
            Td("Data not available"),
            Td(round(r["correct_reward"]["tft"] / TFT_DIVISOR, 2)),
        )
    )

    rows.append(
        Tr(
            Th(Br(), Strong("CU")),
            Th(Br(), Strong("SU")),
            Th(Br(), Strong("NU")),
            Th(Br(), Strong("Certification")),
        )
    )

    rows.append(
        Tr(
            Td(r[f"{rtype}_cloud_units"]["cu"]),
            Td(r[f"{rtype}_cloud_units"]["su"]),
            Td(r[f"{rtype}_cloud_units"]["nu"]),
            Td("Data not available"),
        )
    )
    return Table(*rows)


def render_uptime_events(node):
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


def receipt_header_node():
    return Tr(
        Th(Strong("Year")),
        Th(Strong("Month")),
        Th(Strong("Uptime")),
        Th(Strong("TFT Minted")),
        Th(Strong("Fixup")),
    )


def receipt_header_period():
    return Tr(
        Th(Strong("Node ID")),
        Th(Strong("Uptime")),
        Th(Strong("TFT Minted")),
        Th(Strong("Fixup")),
    )


def receipt_header_details():
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
    node_id = receipt["node_id"]
    has_start = con.execute(
        "SELECT 1 FROM PowerState WHERE node_id=? AND timestamp>=?  AND timestamp<=?",
        [
            node_id,
            (receipt["period"]["start"] - WIGGLE),
            (receipt["period"]["start"] + WIGGLE),
        ],
    ).fetchone()

    # has_end = con.execute(
    #     "SELECT 1 FROM PowerState WHERE node_id=? AND timestamp>=?  AND timestamp<=?",
    #     [
    #         node_id,
    #         (receipt["period"]["end"] - WIGGLE),
    #         (receipt["period"]["end"] + WIGGLE),
    #     ],
    # ).fetchone()

    # Generally we won't have any partial periods, except for the ongoing period, due to default behavior of the minting data ingester. So this is a sufficient check that there's some data to show
    if not has_start:
        return None
    else:
        period = Period(receipt["period"]["start"] + WIGGLE)
        node = grid3.minting.mintingnode.check_node(con, node_id, period)
        return node


serve()

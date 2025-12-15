import concurrent.futures
import logging
import sqlite3

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s"
)
import os
import threading
import time
from datetime import datetime, timedelta
from typing import List, Tuple

import grid3.graphql
import grid3.minting.mintingnode
from fasthtml.common import *
from gql import gql
from grid3.minting.period import Period

from lightdark import DarkLink, LightDarkScript, LightLink
from receipts import STANDARD_PERIOD_DURATION, ReceiptHandler, make_node_minting_periods

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

graphql_url = "https://graphql.grid.tf/graphql"
graphql = grid3.graphql.GraphQL(graphql_url, fetch_schema=False)
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


@rt("/csv/{node_id}/{period_slug}")
def get(node_id: int, period_slug: str):
    period = slug_to_period(period_slug)
    minting_node = mintinglite(node_id, period)
    filename = f"node{minting_node.id}.csv"
    path = "csvs/" + filename
    minting_node.write_csv(path)
    return FileResponse(path, filename=filename)


@rt("/node/{node_id}")
def get(req, node_id: int, show_empty: bool = False):
    try:
        receipts = make_node_minting_periods(
            node_id, receipt_handler.get_node_receipts(node_id)
        )
        if not receipts:
            results = "No receipts found."
        else:
            results = [
                H2(f"Node {node_id}"),
                render_receipt_overview(receipts, "node", show_empty),
            ]
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e):
            logging.error(f"Database locked error while accessing node {node_id}")
            results = P("Error retrieving receipts data. Please try again.")
        else:
            logging.error(f"Database error while accessing node {node_id}: {str(e)}")
            results = P("Error retrieving receipts data. Please try again.")
    except Exception as e:
        logging.error(f"Unexpected error while accessing node {node_id}: {str(e)}")
        results = P("Error retrieving receipts data. Please try again.")

    if "hx-request" in req.headers:
        return results
    else:
        return render_main("node", node_id, result=results)


@rt("/farm/{farm_id}")
def get(req, farm_id: int, sort_by: str = "node", show_empty: bool = False):
    try:
        farm_receipts = fetch_farm_receipts(farm_id)
        
        # Handle case where farm has no nodes
        if not farm_receipts:
            results = P(f"Farm {farm_id} has no nodes or nodes not found.")
        else:
            results = []
            if sort_by == "node":
                for node_id, receipts in farm_receipts:
                    if receipts:
                        results.append(H2(f"Node {node_id}"))
                        results.append(
                            render_receipt_overview(receipts, sort_by, show_empty)
                        )

            elif sort_by == "period":
                receipts_by_period = {}
                for _, receipts in farm_receipts:
                    for receipt in receipts:
                        receipts_by_period.setdefault(receipt.period.offset, []).append(
                            receipt
                        )
                for offset, receipts in reversed(sorted(receipts_by_period.items())):
                    period = Period(offset=offset)
                    results.append(H2(f"{period.month_name} {period.year}"))
                    results.append(render_receipt_overview(receipts, sort_by, show_empty))
            
            if not results:
                results = "No receipts found."

    except sqlite3.OperationalError as e:
        if "database is locked" in str(e):
            logging.error(f"Database locked error while accessing farm {farm_id}")
            results = P("Error retrieving receipts data. Please try again.")
        else:
            logging.error(f"Database error while accessing farm {farm_id}: {str(e)}")
            results = P("Error retrieving receipts data. Please try again.")
    except Exception as e:
        logging.error(f"Unexpected error while accessing farm {farm_id}: {str(e)}")
        results = P("Error retrieving receipts data. Please try again.")

    if "hx-request" in req.headers:
        return results
    else:
        return render_main("farm", farm_id, show_empty, sort_by, results)


@rt("/node/{node_id}/{period_slug}")
def get(req, node_id: int, period_slug: str):
    details = render_details(node_id, period_slug)

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
        nodes = graphql.nodes(["nodeID"], farmID_eq=farm_id)

    node_ids = [node["nodeID"] for node in nodes]
    
    # Simply fetch receipts from database for each node
    processed_responses = []
    for node_id in node_ids:
        receipt_list = receipt_handler.get_node_receipts(node_id)
        processed_responses.append(
            (node_id, make_node_minting_periods(node_id, receipt_list))
        )
    
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
                Script(
                    r"""
                function toggleZeroDowntime() {
                    const checkbox = document.getElementById('show_zero_downtime');
                    const hiddenRows = document.querySelectorAll('table[id^="uptime-"] tr[data-hidden="true"]');
                    const summaryRows = document.querySelectorAll('table[id^="uptime-"] tr[data-hidden-summary="true"]');

                    hiddenRows.forEach(row => {
                        row.style.display = checkbox.checked ? 'none' : '';
                    });

                    summaryRows.forEach(row => {
                        row.style.display = checkbox.checked ? '' : 'none';
                    });
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
            row = render_receipt_row(receipt, sort_by, show_empty, show_year)
            if row:
                rows.append(row)
                last_year = receipt.period.year

    elif sort_by == "period":
        receipts = sorted(receipts, key=lambda x: x.node_id)
        rows = [receipt_header_period()]
        for receipt in receipts:
            row = render_receipt_row(receipt, sort_by, show_empty)
            if row:
                rows.append(row)

    return Table(*rows, cls="hover")


def render_receipt_row(node_period, sort_by, show_empty, show_year=True):
    node_id = node_period.node_id
    period = node_period.period

    if not node_period.has_receipt:
        minting_node = mintinglite(node_id, period)
        if not minting_node or not minting_node.events:
            # We have no receipts and no minting events for this node in this
            # period. Either the node has been inactive or we don't have a
            # tfchain db to read from. That makes this another case of an
            # "empty" period, but we had to run mintinglite to know that since
            # the receipts for this period haven't been published yet (or are
            # missing due to some error)
            if not show_empty:
                return None
            else:
                uptime = None
        else:
            uptime = minting_node.uptime

        reward = "Data not available"
    else:
        if node_period.correct_receipt:
            receipt = node_period.correct_receipt
        elif node_period.minted_receipt:
            receipt = node_period.minted_receipt
        else:
            # TODO: apparently there are some fixups where neither the original
            # hash nor "correct" has lead to a receipt. In that case, we can
            # still at least render the reward amount (maybe use
            # render_fixup_detail)
            return Tr(Td("Data not available"), Td(), Td(), Td())

        uptime = receipt["measured_uptime"]
        if receipt["stellar_payout_address"] == "":
            reward = "0.0"
        else:
            reward = round(receipt["reward"]["tft"] / TFT_DIVISOR, 2)

    now = time.time()
    if now > period.end and uptime is not None:
        uptime_percent = round(uptime / STANDARD_PERIOD_DURATION * 100, 2)
    elif uptime is not None:
        scaled_period_duration = now - period.start
        uptime_percent = round(uptime / scaled_period_duration * 100, 2)
    else:
        uptime_percent = None

    row = Tr(
        hx_get=f"/node/{node_id}/{period_to_slug(period)}",
        hx_target="#result",
        hx_trigger="click",
        hx_push_url="true",
        hx_swap="show:top",
    )

    if sort_by == "node":
        if show_year:
            elements = [
                Td(f"{period.year}"),
                Td(period.month_name),
            ]
        else:
            elements = [
                Td(),
                Td(period.month_name),
            ]
    elif sort_by == "period":
        elements = [
            Td(f"{node_id}"),
        ]

    if uptime_percent is not None and uptime_percent <= 100:
        elements.append(Td(f"{uptime_percent}%"))
    else:
        # Some receipts have uptime figures that are way too large
        elements.append(Td("Data not available"))

    elements.append(Td(reward))
    elements.append(Td("✔️" if node_period.fixup_receipt else ""))
    return row(*elements)


def render_details(node_id, period_slug):
    period = Period(
        (datetime.strptime(period_slug, "%B-%Y") + timedelta(days=15)).timestamp()
    )

    receipts = receipt_handler.get_node_period_receipts(node_id, period)
    minting_node = mintinglite(node_id, period)

    # Get contract billing revenue
    contract_revenue = get_contract_billing_revenue(node_id, period)
    # Convert to decimal format
    if contract_revenue > 0:
        contract_revenue = round(contract_revenue / TFT_DIVISOR, 2)

    response = [H2(f"Node {node_id} - {period.month_name} {period.year}")]
    if receipts:
        receipts_by_hash = {}
        fixup = None
        for receipt in receipts:
            receipts_by_hash[receipt["hash"]] = receipt
            if receipt["type"] == "Fixup":
                fixup = receipt

        if fixup:
            minted_receipt = receipt_handler.get_receipt(fixup["minted_receipt"])
            correct_receipt = receipt_handler.get_receipt(fixup["correct_receipt"])

            # It's possible that either the minted_receipt or the
            # correct_receipt is missing from the receipt API. In that case,
            # fill in the details we have from the fixup receipt
            response.append(H3("Corrected Receipt"))
            if correct_receipt:
                response.append(
                    render_receipt_detail(correct_receipt, contract_revenue)
                )
            else:
                response.append(
                    render_fixup_detail(receipt, "correct", contract_revenue)
                )

            response.append(H3("Original Receipt"))
            if minted_receipt:
                response.append(render_receipt_detail(minted_receipt, contract_revenue))
            else:
                response.append(
                    render_fixup_detail(receipt, "minted", contract_revenue)
                )
        else:
            response.append(render_receipt_detail(receipt, contract_revenue))
    elif minting_node:
        if time.time() < period.end:
            response.append(
                Small(
                    Em(
                        "This minting period has not ended yet. Uptime data is approximate and does not reflect the final data that will be used for minting."
                    )
                )
            )
        else:
            response.append(
                Small(
                    Em(
                        "Minting period has ended, but minting receipts aren't yet available. Uptime data is approximate and does not reflect the final data that will be used for minting."
                    )
                )
            )

        response.append(Br())
        response.append(Br())

        response.append(render_no_receipt_detail(minting_node, contract_revenue))
    else:
        response.append(P("Data not available for this period"))
    response.append(Br())

    heading = H3("Uptime Events")
    if minting_node:
        uptime_events = [
            Div(style="display: flex; align-items: baseline;")(
                heading,
                A(
                    style="margin-left:auto;",
                    href=f"/csv/{node_id}/{period_to_slug(period)}",
                    download=True,
                )("Download CSV"),
            ),
            Div(style="display: flex; align-items: center;")(
                Input(
                    type="checkbox",
                    id="show_zero_downtime",
                    checked=False,
                    onchange="toggleZeroDowntime()",
                ),
                Label(
                    "Summary view",
                    fr="show_zero_downtime",
                    style="margin-left: 5px",
                ),
            ),
            Br(),
            render_uptime_events(minting_node, node_id, period_to_slug(period)),
        ]
    else:
        uptime_events = [
            heading,
            "Data not available for this period",
        ]

    response.extend(uptime_events)

    return response


def render_receipt_detail(r, contract_revenue=0):
    rows = [receipt_header_details()]
    uptime = round(r["measured_uptime"] / STANDARD_PERIOD_DURATION * 100, 2)

    if r["stellar_payout_address"] == "":
        reward = ["0.0 ", I("(payout address missing)")]
    else:
        reward = [round(r["reward"]["tft"] / TFT_DIVISOR, 2)]

    # Add contract revenue to the existing row
    contract_revenue_display = (
        f"{contract_revenue} TFT" if contract_revenue > 0 else "0 TFT"
    )

    rows.append(
        Tr(
            Td(datetime.fromtimestamp(r["period"]["start"]).date()),
            Td(datetime.fromtimestamp(r["period"]["end"]).date()),
            # Some receipts have uptime figures that are way too large
            Td(f"{uptime}%") if uptime <= 100 else Td("Data not available"),
            Td(*reward),
            Td(contract_revenue_display),
        )
    )

    rows.append(
        Tr(
            Th(Br(), Strong("CU")),
            Th(Br(), Strong("SU")),
            Th(Br(), Strong("NU")),
            Th(Br(), Strong("Certification")),
            Th(),
        )
    )

    rows.append(
        Tr(
            Td(r["cloud_units"]["cu"]),
            Td(r["cloud_units"]["su"]),
            Td(r["cloud_units"]["nu"]),
            Td(r["node_type"]),
            Td(),
        )
    )

    return Table(*rows)


def render_fixup_detail(r, rtype, contract_revenue=0):
    """Some receipts are missing from the API. In that case, render a limited version of what the receipt would contain based on the info in the fixup receipt."""
    rows = [receipt_header_details()]

    rows.append(
        Tr(
            Td(datetime.fromtimestamp(r["period"]["start"]).date()),
            Td(datetime.fromtimestamp(r["period"]["end"]).date()),
            Td("Data not available"),
            Td(round(r["correct_reward"]["tft"] / TFT_DIVISOR, 2)),
            Td(f"{contract_revenue} TFT" if contract_revenue > 0 else "0 TFT"),
        )
    )

    rows.append(
        Tr(
            Th(Br(), Strong("CU")),
            Th(Br(), Strong("SU")),
            Th(Br(), Strong("NU")),
            Th(Br(), Strong("Certification")),
            Th(),
        )
    )

    rows.append(
        Tr(
            Td(r[f"{rtype}_cloud_units"]["cu"]),
            Td(r[f"{rtype}_cloud_units"]["su"]),
            Td(r[f"{rtype}_cloud_units"]["nu"]),
            Td("Data not available"),
            Td(),
        )
    )

    return Table(*rows)


def render_no_receipt_detail(node, contract_revenue=0):
    """For periods with no receipt yet, render a limited details table from the
    MintingNode data."""
    if isinstance(node, dict) and "error" in node:
        return P(node["error"])

    rows = [
        Tr(
            Th(Strong("Period Start")),
            Th(Strong("Period End")),
            Th(Strong("Uptime")),
            Th(Strong("Downtime")),
            Th(Strong("Contract Revenue")),
        )
    ]

    now = time.time()
    if now < node.period.end:
        scaled_period_duration = now - node.period.start
        uptime_percent = round(node.uptime / scaled_period_duration * 100, 2)
    else:
        uptime_percent = round(node.uptime / STANDARD_PERIOD_DURATION * 100, 2)

    rows.append(
        Tr(
            Td(datetime.fromtimestamp(node.period.start).date()),
            Td(datetime.fromtimestamp(node.period.end).date()),
            Td(f"{uptime_percent}%"),
            Td(format_duration(node.downtime)),
            Td(f"{contract_revenue} TFT" if contract_revenue > 0 else "0 TFT"),
        )
    )

    return Table(*rows)


def render_uptime_events(minting_node, node_id, period_slug):
    table_id = f"uptime-{node_id}-{period_slug}"
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

    hidden_block = []
    visible_block = []

    def flush_hidden_block():
        nonlocal hidden_block
        if hidden_block:
            rows.append(
                Tr(
                    data_hidden_summary="true",
                    style="display: none; text-align:center; font-style:italic; color:var(--pico-muted-color)",
                )(
                    Td(colspan="6")(
                        f"{len(hidden_block)} {'event' if len(hidden_block) == 1 else 'events'} hidden (less than 10s downtime)"
                    )
                )
            )
            rows.extend(hidden_block)
            hidden_block = []

    def flush_visible_block():
        nonlocal visible_block
        rows.extend(visible_block)
        visible_block = []

    first_entry = True
    total_events = len(minting_node.events)
    for idx, e in enumerate(minting_node.events):
        downtime_val = str(e[4])
        try:
            downtime_seconds = int(downtime_val.split()[0])
            is_near_zero_downtime = abs(downtime_seconds) <= 10
        except (ValueError, IndexError):
            is_near_zero_downtime = False

        tr = Tr(*[Td(item) for item in e])
        # Ensure the very first and very last entries are always visible, even if near-zero downtime
        is_last_entry = idx == total_events - 1
        if first_entry or is_last_entry:
            if hidden_block:
                flush_hidden_block()
            visible_block.append(tr)
            if first_entry:
                first_entry = False
            continue

        if is_near_zero_downtime:
            tr.attrs["data-hidden"] = "true"
            if visible_block:
                flush_visible_block()
            hidden_block.append(tr)
        else:
            if hidden_block:
                flush_hidden_block()
            visible_block.append(tr)

    # flush any remaining blocks
    flush_hidden_block()
    flush_visible_block()

    # final entry if node stopped reporting
    if minting_node.events:
        last_event = minting_node.events[-1]
        last_timestamp = last_event[1]
        period_end = minting_node.period.end
        if last_timestamp < period_end:
            downtime_seconds = period_end - last_timestamp
            final_entry = [
                datetime.fromtimestamp(period_end).strftime("%Y-%m-%d %H:%M:%S"),
                int(period_end),
                "0",
                "0",
                str(int(downtime_seconds)),
                "Node stopped reporting before period end",
            ]
            rows.append(Tr(*[Td(item) for item in final_entry]))

    return Table(*rows, id=table_id)


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
        Th(Strong("Contract Revenue")),
    )


def mintinglite(node_id, period):
    # For testing without db file present
    if not os.path.exists("tfchain.db"):
        return None

    try:
        con = sqlite3.connect("tfchain.db")
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e):
            logging.warning(
                f"Database locked error while accessing node {node_id} for period {period}"
            )
            return {
                "error": "The database is currently locked. Please try again in a few moments.",
                "events": [],
            }
        raise

    # The code below probably adds a decent efficiency gain for periods where
    # we don't have the tfchain data (and that's currently quite a lot of the
    # periods, since we never went back and indexed the whole chain). But it
    # also doesn't work for the period when the node joined the network. Since
    # there's no PowerState for that node at the beginning of the period, this
    # logic skips the period entirely

    # TODO: This is another place where knowing the node creation time would be
    # relevant. Then we could also check if the period is the node's creation
    # period. It might also be helpful to keep a second checkpoint,
    # representing the oldest block we've processed and its timestamp.

    # Check if our db contains all events for the period in question
    # has_start = con.execute(
    #     "SELECT 1 FROM PowerState WHERE node_id=? AND timestamp>=?  AND timestamp<=?",
    #     [
    #         node_id,
    #         (period.start - WIGGLE),
    #         (period.start + WIGGLE),
    #     ],
    # ).fetchone()

    # has_end = con.execute(
    #     "SELECT 1 FROM PowerState WHERE node_id=? AND timestamp>=?  AND timestamp<=?",
    #     [
    #         node_id,
    #         (receipt["period"]["end"] - WIGGLE),
    #         (receipt["period"]["end"] + WIGGLE),
    #     ],
    # ).fetchone()

    # Generally we won't have any partial periods, except for the ongoing period, due to default behavior of the minting data ingester. So this is a sufficient check that there's some data to show
    # if not has_start:
    #     return None
    # else:
    #     node = grid3.minting.mintingnode.check_node(con, node_id, period)
    #     return node

    node = grid3.minting.mintingnode.check_node(con, node_id, period)
    return node


def slug_to_period(slug):
    return Period((datetime.strptime(slug, "%B-%Y") + timedelta(days=15)).timestamp())


def period_to_slug(period):
    return f"{period.month_name.lower()}-{period.year}"


def format_duration(seconds):
    decmal_places = 2
    if seconds == 0:
        return "0 seconds"

    # Define time units in seconds
    minute = 60
    hour = minute * 60
    day = hour * 24

    if seconds < minute:
        # Less than a minute
        return f"{seconds} {'second' if seconds == 1 else 'seconds'}"

    elif seconds < hour:
        # Convert to minutes
        minutes = round(seconds / minute, decmal_places)
        if minutes.is_integer():
            minutes = int(minutes)
        return f"{minutes} {'minute' if minutes == 1 else 'minutes'}"

    elif seconds < (hour * 48):  # Switch to days after 48 hours
        # Convert to hours
        hours = round(seconds / hour, decmal_places)
        if hours.is_integer():
            hours = int(hours)
        return f"{hours} {'hour' if hours == 1 else 'hours'}"

    else:
        # Convert to days
        days = round(seconds / day, decmal_places)
        if days.is_integer():
            days = int(days)
        return f"{days} {'day' if days == 1 else 'days'}"


def get_contract_billing_revenue(node_id: int, period: Period) -> float:
    """Query contract billing data for a node during a specific period and return total revenue."""
    # First query: Get all contracts for the node
    contracts_query = gql("""
        query NodeContracts($nodeId: Int!) {
            nodeContracts(where: {nodeID_eq: $nodeId}) {
                contractID
            }
        }
    """)

    with gql_lock:
        try:
            # Execute the contracts query
            contracts_result = graphql.client.execute(
                contracts_query, variable_values={"nodeId": node_id}
            )

            # Extract contract IDs
            contract_ids = []
            if (
                "nodeContracts" in contracts_result
                and contracts_result["nodeContracts"]
            ):
                contract_ids = [
                    contract["contractID"]
                    for contract in contracts_result["nodeContracts"]
                ]

            # If no contracts found, return 0
            if not contract_ids:
                return 0.0

            # Second query: Get bill reports for all contracts within the period
            bill_reports_query = gql("""
                query ContractBilling($contractIds: [BigInt!], $timestampGt: BigInt!, $timestampLt: BigInt!) {
                    contractBillReports(
                        where: {
                            contractID_in: $contractIds,
                            timestamp_gt: $timestampGt,
                            timestamp_lt: $timestampLt
                        }
                    ) {
                        contractID
                        amountBilled
                    }
                }
            """)

            # Execute the bill reports query
            bill_reports_result = graphql.client.execute(
                bill_reports_query,
                variable_values={
                    "contractIds": contract_ids,
                    "timestampGt": str(period.start),
                    "timestampLt": str(period.end),
                },
            )

            # Sum up all the billed amounts
            total_revenue = 0.0
            for report in bill_reports_result["contractBillReports"]:
                total_revenue += float(report["amountBilled"])

            print(total_revenue)

            return total_revenue
        except Exception as e:
            logging.error(
                f"Error querying contract billing data for node {node_id}: {str(e)}"
            )
            return 0.0


serve()

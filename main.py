from datetime import datetime
import urllib.parse
import requests
from fasthtml.common import *
import grid3.network

mainnet = grid3.network.GridNetwork()
app, rt = fast_app(live=True)


@rt("/")
def get(id_input: int = None, select: str = "Node ID"):
    if id_input:
        result = render_receipts(id_input)
    else:
        result = None

    return Titled(
        "Fetch Minting Receipts",
        Form(
            hx_get="/submit",
            hx_target="#result",
            hx_trigger="submit",
            hx_indicator="#loading",
        )(
            Div(
                Div(
                    Input(
                        type="int",
                        id="id_input",
                        placeholder=42,
                        value=id_input,
                    ),
                    style="display: inline-block",
                ),
                Div(
                    Select(
                        Option("Node ID"),
                        Option("Farm ID"),
                        id="select",
                        value=select,
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
        # Br(),
        Div("Loading...", id="loading", cls="htmx-indicator"),
        Div(result, id="result"),
        Style(
            """
            .htmx-indicator{
            opacity:0;
            transition: opacity 500ms ease-in;
            }
            .htmx-request.htmx-indicator{
            opacity:1;
            }
            """
        ),
    )


@rt("/submit")
def get(id_input: int, select: str):
    if select == "Node ID":
        response = render_receipts(id_input)
    elif select == "Farm ID":
        nodes = mainnet.graphql.nodes(["nodeID"], farmID_eq=id_input)
        node_ids = sorted([node["nodeID"] for node in nodes])
        response = []
        for node in node_ids:
            response.append(H2("Node " + str(node)))
            response.append(render_receipts(node))
    params = urllib.parse.urlencode({"id_input": id_input, "select": select})
    return response, HtmxResponseHeaders(push_url="/?" + params)


def render_receipts(node_id):
    if node_id:
        try:
            node_id = int(node_id)
        except ValueError:
            return "Please enter a valid node id"
    else:
        return "Please enter a valid node id"

    receipts = requests.get(
        f"https://alpha.minting.tfchain.grid.tf/api/v1/node/{node_id}"
    ).json()
    header = Tr(
        Th(Strong("Period Start")),
        Th(Strong("Period End")),
        Th(Strong("Uptime")),
        Th(Strong("TFT Minted")),
    )
    rows = [header]
    for r in receipts:
        if "Minting" in r["receipt"]:
            r = r["receipt"]["Minting"]
            uptime = round(r["measured_uptime"] / (30.45 * 24 * 60 * 60) * 100, 2)
            rows.append(
                Tr(
                    Td(datetime.fromtimestamp(r["period"]["start"]).date()),
                    Td(datetime.fromtimestamp(r["period"]["end"]).date()),
                    Td(f"{uptime}%"),
                    Td(r["reward"]["tft"] / 1e7),
                )
            )
    return Table(*rows)


serve()

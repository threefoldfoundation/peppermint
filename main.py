from datetime import datetime
import urllib.parse
import requests
from fasthtml.common import *
import grid3.network

mainnet = grid3.network.GridNetwork()
app, rt = fast_app(live=True)


@rt("/")
def get():
    return render_main()


@rt("/{select}/{id_input}")
def get(req, select: str, id_input: int):
    if select == "node":
        results = [render_receipts(id_input)]
    elif select == "farm":
        nodes = mainnet.graphql.nodes(["nodeID"], farmID_eq=id_input)
        node_ids = sorted([node["nodeID"] for node in nodes])
        results = []
        for node in node_ids:
            result.append(H2("Node " + str(node)))
            result.append(render_receipts(node))

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


def render_main(select="node", id_input=None, result=""):
    return Titled(
        "Fetch Minting Receipts",
        Form(
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
    )


def render_receipts(node_id):
    if node_id:
        try:
            node_id = int(node_id)
        except ValueError:
            return "Please enter a valid node id"
    else:
        return "Please enter a valid node id"

    try:
        receipts = requests.get(
            f"https://alpha.minting.tfchain.grid.tf/api/v1/node/{node_id}"
        ).json()
    except requests.exceptions.JSONDecodeError:
        return None

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

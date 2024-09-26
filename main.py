from datetime import datetime
import requests
from fasthtml.common import *

app, rt = fast_app(live=True)


@rt("/")
def get():
    return Titled(
        "Fetch Node Minting Receipts",
        Form(hx_post="/submit", hx_target="#result", hx_trigger="submit")(
            Input(type="int", id="node_id", placeholder=42),
            Button("Go", type="submit"),
        ),
        Div(id="result"),
    )


@rt("/submit")
def post(d: dict):
    try:
        node_id = int(d["node_id"])
    except ValueError:
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

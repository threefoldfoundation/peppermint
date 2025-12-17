## Quickstart

While not required, [uv](https://docs.astral.sh/uv/getting-started/installation/) is recommended for development.

```
git clone https://github.com/scottyeager/peppermint.git

cd peppermint
uv venv
uv pip install -r requirements.txt

# With live reload, changes to the code will be served automatically
LIVE_RELOAD=True uv run main.py
```

A link to visit the dev server will appear in your terminal.

## Deploy

FastHTML ships with Uvicorn, which is used for the development server when executing `main.py` directly.

For production, it's best to disable the websocket based live code reloading and use a bit more robust configuration. A number of options are discussed [here](https://www.uvicorn.org/deployment/). The built in process manager is what I'll show below.

Then run it:

```
uv run uvicorn main:app --host 0.0.0.0 --port 80 --workers 1
```

That's for HTTP only. To enable HTTPS via a reverse proxy, such as Caddy, binding to a different port on localhost might be preferred.

## Receipts Database

Peppermint requires a prepopulated Sqlite database of minting receipts. A script is included to generate and update such a database:

```
uv run receipts.py
```

The script will continue to run forever and check for new receipts periodically (this won't be needed for much longer though with the end of TFT minting).

Both the receipts script and Peppermint itself can be configured to use a different location for the receipts database (default is pwd):

```
uv run receipts.py --db-path /path/to/receipts.db
DB_PATH=/path/to/receipts.db uv run uvicorn main:app --host 0.0.0.0 --port 80 --workers 1
```

There doesn't seem to be a way to pass CLI args through when calling uvicorn like this, thus the use of an env var.

## Contributing

Code style is [Black](https://github.com/psf/black).

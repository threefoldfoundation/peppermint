## Quickstart

```
git clone https://github.com/scottyeager/peppermint.git

cd peppermint
python3 -m venv venv
source venv/bin/activate # Choose .fish for fish shell
pip install python-fasthtml requests grid3

python3 main.py
```

A link to visit the dev server will appear in your terminal.

## Deploy

FastHTML ships with Uvicorn, which is used for the development server when executing `main.py` directly.

For production, it's best to disable the websocket based live code reloading and use a bit more robust configuration. A number of options are discussed [here](https://www.uvicorn.org/deployment/). I've had best results using Gunicorn.

Use a `config.py` file to disable live code reloading:

```
echo "LIVE_RELOAD = False" > config.py
```

Then run install and start Gunicorn:

```
source venv/bin/activate
pip install gunicorn uvicorn-worker
gunicorn -b 0.0.0.0:80 -w 4 -k uvicorn.workers.UvicornWorker
```

That's for HTTP only. To enable HTTPS via a reverse proxy, such as Caddy, binding to a different port on localhost might be preferred.

## Contributing

Code style is [Black](https://github.com/psf/black).

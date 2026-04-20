# Trading Bot Setup (No Supabase)

## 1) Create and activate a virtual environment

### Windows
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Linux / macOS
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 2) Configure environment variables

Copy `.env.example` to `.env` and fill in your Binance Futures API keys.

```bash
cp .env.example .env
```

If you want paper trading, keep `TESTNET=true`.

## 3) Run the bot

```bash
python main.py
```

## 4) Open the local API

When the bot is running, these endpoints are available:

- `http://localhost:5000/proxy/stats`
- `http://localhost:5000/proxy/open_positions`
- `http://localhost:5000/proxy/trades`
- `http://localhost:5000/proxy/fapi/v2/account`

## Notes

- Trade history is stored locally in `trade_log.csv`.
- If Supabase credentials are empty, the bot will still run.
- `step4_telegram.py` is a stub so the project starts cleanly.
- The frontend dashboard code is not included in the uploaded files, so this zip contains the bot backend only.


## 5) Open the dashboard

Open:

- `http://localhost:5000/`

This dashboard is included in this package and reads from the local Flask API.

## 6) Fix 401 Unauthorized on testnet

If you see `401 Unauthorized` for `/proxy/fapi/v2/account`, your keys are not valid for the selected mode.

For testnet futures keys:
- Create them at `https://testnet.binancefuture.com`
- Keep `TESTNET=true`
- Paste those keys into `.env`

For live futures keys:
- Use your normal Binance Futures API keys
- Set `TESTNET=false`
- Make sure Futures permission is enabled

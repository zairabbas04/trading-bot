# Fix Binance API 401 Unauthorized

## Testnet
1. Open Binance Futures testnet.
2. Create a new API key there.
3. In `.env`, set:
   - `TESTNET=true`
   - `BINANCE_API_KEY=<testnet key>`
   - `BINANCE_API_SECRET=<testnet secret>`
4. Restart the bot.

## Live
1. Use a live Binance Futures API key.
2. Enable Futures trading permission.
3. In `.env`, set `TESTNET=false`.
4. Restart the bot.

## Quick checks
- Key and secret pasted exactly.
- No extra spaces or quotes.
- IP restriction disabled or your server IP is whitelisted.
- System clock is correct.

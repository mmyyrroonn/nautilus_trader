# nautilus-aster

[NautilusTrader](https://nautilustrader.io) market-data adapter for the
[Aster DEX](https://www.asterdex.com/) perpetual futures exchange.

Aster exposes a Binance-USD-M-compatible Futures API, so this crate is a thin
configuration and factory layer over `nautilus-binance`: it routes the USD-M data path at
Aster's REST and WebSocket endpoints and pins the Nautilus venue to `ASTER`. No Binance
protocol code is duplicated here.

Supported today:

- USD-M perpetual market data (quotes, trades, order book deltas, mark price, funding rates)
- Crypto perps (e.g. `BTCUSDT-PERP.ASTER`) and US-stock perps (e.g. `NVDAUSDT-PERP.ASTER`)

Execution is **not** provided by this crate.

"""
main.py
-------
Main entry point for live trading and data pipeline initialization.

Steps:
1. Login to Kite Connect (manual or saved token)
2. Retrieve instrument tokens for NIFTY 100 (or custom list)
3. Backfill historical data if missing
4. Start WebSocket for live ticks
5. Initialize in-memory buffers for live candles
6. Fetch ready-to-use inputs using data_loader.get_inputs()
7. (Future) Pass data to feature_engineering, ML, and execution modules
"""

from datetime import datetime, timedelta, timezone
from src.login import kite_login
from src.instrument_lookup import InstrumentLookup
from src.live_websocket import LiveWebSocket
from src import data_loader

# ===============================
# 1️⃣ Login to Kite
# ===============================
def main():
    kite, used_saved_token = kite_login()
    if not kite:
        print("❌ Could not connect to Kite.")
        return

    print("\n✅ Logged in successfully using",
          "saved token." if used_saved_token else "manual login.")

    profile = kite.profile()
    print(f"\n👤 User: {profile['user_name']} ({profile['user_id']})")
    print(f"📧 Email: {profile['email']}")
    print(f"🌐 Broker: {profile['broker']}\n")

    # ===============================
    # 2️⃣ Get Instrument Tokens
    # ===============================
    print("🔍 Fetching instrument tokens...")
    lookup = InstrumentLookup(kite)
    symbols = ["INFY", "TCS", "HDFCBANK", "RELIANCE"]  # example subset; can load full NIFTY 100
    token_map = lookup.get_tokens(symbols)
    print(f"✅ Retrieved {len(token_map)} tokens.\n")

    # ===============================
    # 3️⃣ Backfill Historical Data
    # ===============================
    start_dt = datetime.now(timezone.utc) - timedelta(days=2)
    end_dt = datetime.now(timezone.utc)
    print("📦 Backfilling last 2 days of data (if not present)...")
    data_loader.backfill_history(kite, token_map, start_dt, end_dt)

    # ===============================
    # 4️⃣ Initialize Live WebSocket
    # ===============================
    print("\n📡 Starting Live WebSocket Stream...")
    instrument_tokens = list(token_map.values())
    ws = LiveWebSocket(kite.api_key, kite.access_token, instrument_tokens)
    ws.start()

    # ===============================
    # 5️⃣ Initialize Buffers
    # ===============================
    print("\n🧠 Initializing in-memory buffers for live updates...")
    for sym in symbols:
        data_loader.init_live_buffer(sym)

    # ===============================
    # 6️⃣ Get Inputs for Next Layer
    # ===============================
    print("\n📊 Fetching latest OHLCV inputs...")
    inputs = data_loader.get_inputs(symbols, lookback=500, include_live=True)

    # Show preview
    for sym, df in inputs.items():
        print(f"\n📈 {sym} — {len(df)} rows loaded. Latest 3 candles:")
        print(df.tail(3).to_string(index=False))

    # ===============================
    # 7️⃣ (Future Hook)
    # ===============================
    # from src.feature_engineering import FeatureGenerator
    # fg = FeatureGenerator()
    # feature_set = fg.generate(inputs)
    # print(feature_set.head())

    print("\n🚀 System initialized and streaming live data.")


if __name__ == "__main__":
    main()

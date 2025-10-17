from src.setup_path import enable_project_imports
enable_project_imports()


from src.live_websocket import LiveWebSocket
from src.login import kite_login
print("✅ Imports working perfectly!")

from src.login import kite_login
from src.instrument_lookup import InstrumentLookup

# Login to Kite
kite, _ = kite_login()

# Example list (could be NIFTY 100, or any custom universe)
symbols = ["INFY", "TCS", "RELIANCE", "HDFCBANK"]

lookup = InstrumentLookup(kite)
tokens = lookup.get_tokens(symbols)

print(tokens)

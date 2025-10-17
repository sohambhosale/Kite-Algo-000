"""
instrument_lookup.py
--------------------
Generic, scalable instrument token fetcher and cache manager.

Usage Example:
--------------
from src.login import kite_login
from src.instrument_lookup import InstrumentLookup

kite, _ = kite_login()
symbols = ["INFY", "TCS", "RELIANCE", "HDFCBANK"]

lookup = InstrumentLookup(kite)
tokens = lookup.get_tokens(symbols)
print(tokens)
"""

import os
import json
import pandas as pd
from kiteconnect import KiteConnect


class InstrumentLookup:
    """
    A class for downloading, caching, and retrieving instrument tokens from Zerodha Kite.
    Works with any list of stock symbols (e.g. NIFTY 100, custom universe, etc.)
    """

    def __init__(self, kite: KiteConnect, exchange: str = "NSE"):
        """
        Initialize with an authenticated KiteConnect object.

        Parameters
        ----------
        kite : KiteConnect
            Authenticated KiteConnect object.
        exchange : str
            Market exchange (default: "NSE")
        """
        self.kite = kite
        self.exchange = exchange
        self.instrument_path = os.path.join("data", "raw", "instruments.csv")
        self.token_cache_path = os.path.join("data", "raw", "instrument_tokens.txt")

    # ------------------------- Core Functions -------------------------

    def download_instruments(self) -> pd.DataFrame:
        """Download full instrument dump for the given exchange."""
        print(f"📡 Downloading full {self.exchange} instrument list from Kite...")
        instruments = self.kite.instruments(self.exchange)
        df = pd.DataFrame(instruments)
        os.makedirs(os.path.dirname(self.instrument_path), exist_ok=True)
        df.to_csv(self.instrument_path, index=False)
        print(f"✅ Saved {len(df)} instruments to {self.instrument_path}")
        return df

    def load_instruments(self) -> pd.DataFrame:
        """Load local instrument dump (download if missing)."""
        if not os.path.exists(self.instrument_path):
            print("⚠ No local instrument file found. Downloading...")
            return self.download_instruments()
        return pd.read_csv(self.instrument_path)

    def get_token(self, symbol: str, instruments: pd.DataFrame) -> int | None:
        """Fetch single instrument token for a symbol."""
        row = instruments.loc[instruments["tradingsymbol"] == symbol]
        if not row.empty:
            return int(row.iloc[0]["instrument_token"])
        print(f"⚠ Token not found for {symbol}")
        return None

    def get_tokens(self, symbols: list[str], save_cache: bool = True) -> dict:
        """
        Get instrument tokens for a list of stock symbols.
        Optionally caches the mapping to a .txt file.

        Parameters
        ----------
        symbols : list[str]
            List of stock symbols to fetch tokens for.
        save_cache : bool
            Save {symbol: token} mapping as a text file (default: True)

        Returns
        -------
        dict
            Mapping of {symbol: instrument_token}
        """
        instruments = self.load_instruments()

        # Filter for active NSE equities
        instruments = instruments[
            (instruments["exchange"] == self.exchange)
            & (instruments["instrument_type"] == "EQ")
        ]

        mapping = {}
        for sym in symbols:
            token = self.get_token(sym, instruments)
            if token:
                mapping[sym] = token

        print(f"✅ Retrieved {len(mapping)}/{len(symbols)} tokens successfully.")

        if save_cache:
            self.save_token_list(mapping)

        return mapping

    # ------------------------- Cache Management -------------------------

    def save_token_list(self, mapping: dict):
        """Save symbol-token mapping to text file in JSON format."""
        os.makedirs(os.path.dirname(self.token_cache_path), exist_ok=True)
        with open(self.token_cache_path, "w") as f:
            json.dump(mapping, f, indent=4)
        print(f"💾 Saved token mapping to {self.token_cache_path}")

    def load_token_list(self) -> dict:
        """Load cached symbol-token mapping from text file."""
        if not os.path.exists(self.token_cache_path):
            raise FileNotFoundError("⚠ Token cache not found. Please run get_tokens() first.")
        with open(self.token_cache_path, "r") as f:
            data = json.load(f)
        print(f"✅ Loaded {len(data)} cached tokens from {self.token_cache_path}")
        return data

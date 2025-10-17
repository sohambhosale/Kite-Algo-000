"""
live_websocket.py
-------------
Handles real-time tick data streaming via Kite Connect WebSocket (KiteTicker).

Enhancements:
- Heartbeat system to monitor data flow
- Auto-reconnect every 60s if heartbeat lost or connection fails
- Connection risk alerts to console
"""

from kiteconnect import KiteTicker
import threading
import time
import traceback
from datetime import datetime


class LiveWebSocket:
    """
    A resilient WebSocket client for Zerodha Kite Connect API.

    Features:
    - Auto-reconnect if connection drops or stalls
    - Heartbeat monitor for tick activity
    - Connection risk detection and recovery
    """

    def __init__(self, api_key: str, access_token: str, instrument_tokens: list[int]):
        self.api_key = api_key
        self.access_token = access_token
        self.instrument_tokens = instrument_tokens
        self.kws = None

        # State variables
        self.connected = False
        self.last_tick_time = None
        self.missed_heartbeats = 0
        self.max_missed_heartbeats = 3  # triggers reconnect
        self.heartbeat_interval = 20    # seconds between heartbeats
        self.reconnect_interval = 60    # retry connection every 60s

    # ------------------------- Callbacks -------------------------

    def on_ticks(self, ws, ticks):
        """Triggered when new tick data arrives."""
        try:
            if ticks:
                self.last_tick_time = time.time()
                self.missed_heartbeats = 0
                print(f"✅ {len(ticks)} tick(s) received at {datetime.now().strftime('%H:%M:%S')}")
                for tick in ticks[:3]:
                    print(f"→ {tick['instrument_token']}: ₹{tick.get('last_price')}")
        except Exception:
            print("⚠ Error in on_ticks callback:")
            traceback.print_exc()

    def on_connect(self, ws, response):
        """Triggered upon successful WebSocket connection."""
        self.connected = True
        print(f"🔗 Connected to Kite WebSocket at {datetime.now().strftime('%H:%M:%S')}.")
        if self.instrument_tokens:
            ws.subscribe(self.instrument_tokens)
            ws.set_mode(ws.MODE_FULL, self.instrument_tokens)
            print(f"📡 Subscribed to {len(self.instrument_tokens)} instruments.")

    def on_close(self, ws, code, reason):
        """Triggered when WebSocket closes."""
        self.connected = False
        print(f"❌ WebSocket closed (code: {code}) — reason: {reason}")
        self._schedule_reconnect()

    def on_error(self, ws, code, reason):
        """Triggered when a WebSocket error occurs."""
        self.connected = False
        if "403" in str(reason):
            print("⚠ WebSocket rejected: Market may be closed or token expired.")
        else:
            print(f"⚠ WebSocket error (code: {code}) — reason: {reason}")
        self._schedule_reconnect()

    # ------------------------- Internal Utilities -------------------------

    def _start_heartbeat_monitor(self):
        """Periodically checks for missed heartbeats and triggers reconnects if needed."""
        def monitor():
            while True:
                time.sleep(self.heartbeat_interval)
                if not self.connected:
                    continue
                if self.last_tick_time is None:
                    continue

                elapsed = time.time() - self.last_tick_time
                if elapsed > self.heartbeat_interval:
                    self.missed_heartbeats += 1
                    print(f"⚠ Heartbeat missed ({self.missed_heartbeats}/{self.max_missed_heartbeats}). No ticks for {elapsed:.1f}s.")
                    if self.missed_heartbeats >= self.max_missed_heartbeats:
                        print("🚨 Connection stale — attempting to reconnect...")
                        self._reconnect()
                else:
                    print(f"💓 Heartbeat OK at {datetime.now().strftime('%H:%M:%S')}")

        threading.Thread(target=monitor, daemon=True).start()

    def _schedule_reconnect(self):
        """Waits and retries connection after a pause."""
        print(f"🔁 Retrying connection in {self.reconnect_interval}s...")
        time.sleep(self.reconnect_interval)
        self._reconnect()

    def _reconnect(self):
        """Reconnects WebSocket safely."""
        try:
            print("🧠 Reconnecting WebSocket...")
            self.connected = False
            self.start()
        except Exception as e:
            print(f"❌ Reconnect attempt failed: {e}")
            time.sleep(self.reconnect_interval)
            self._reconnect()

    # ------------------------- Main Runner -------------------------

    def start(self):
        """Start the WebSocket and heartbeat monitor."""
        try:
            self.kws = KiteTicker(self.api_key, self.access_token)
            self.kws.on_ticks = self.on_ticks
            self.kws.on_connect = self.on_connect
            self.kws.on_close = self.on_close
            self.kws.on_error = self.on_error

            # Launch heartbeat monitor in background
            threading.Thread(target=self._start_heartbeat_monitor, daemon=True).start()

            print("🚀 Starting WebSocket connection...")
            self.kws.connect(threaded=True)
        except Exception as e:
            print(f"❌ Failed to start WebSocket: {e}")
            self._schedule_reconnect()

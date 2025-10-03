from kiteconnect import KiteConnect
import webbrowser
import os
import json

# Save access token in project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TOKEN_FILE = os.path.join(PROJECT_ROOT, "access_token.json")

def kite_login():
    """
    Login to Kite Connect and return KiteConnect object and a flag:
    used_saved_token = True if loaded from saved token, False if manual login.
    """

    api_key = input("Enter your API key: ")
    api_secret = input("Enter your API secret: ")

    kite = KiteConnect(api_key=api_key)

    # Try loading saved token
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f:
                data = json.load(f)
            kite.set_access_token(data["access_token"])
            return kite, True
        except Exception:
            print("⚠ Saved token invalid or expired. Logging in manually.")

    # Manual login
    login_url = kite.login_url()
    print("\nGo to this URL, login, and authorize access:")
    print(login_url)
    webbrowser.open(login_url)

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        request_token = input(f"\nPaste the request token (Attempt {attempt}/3): ")
        try:
            data = kite.generate_session(request_token, api_secret=api_secret)
            kite.set_access_token(data["access_token"])

            # Save token
            with open(TOKEN_FILE, "w") as f:
                json.dump(data, f)

            return kite, False
        except Exception as e:
            print(f"❌ Invalid token or login failed. Error: {str(e)}")
            if attempt == max_attempts:
                print("❌ Maximum attempts reached. Exiting.")
                return None, False
            print("Try again.")

from kiteconnect import KiteConnect
import webbrowser
import os

# -------------------------
# Config
# -------------------------
PROJECT_ROOT = r"C:\EPAT_Project"
TOKEN_FILE = os.path.join(PROJECT_ROOT, "access_token.txt")


def kite_login():
    """
    Login to Kite Connect and return a connected KiteConnect object.
    - Tries saved access token first.
    - If invalid or absent, does manual login with 3 attempts.
    - Saves only API key and access token in TXT file.
    """
    kite = None

    # -------------------------
    # Step 1: Try saved token
    # -------------------------
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f:
                lines = f.read().splitlines()
                if len(lines) >= 2:
                    api_key = lines[0].strip()
                    access_token = lines[1].strip()

                    kite = KiteConnect(api_key=api_key)
                    kite.set_access_token(access_token)

                    # Test connection
                    try:
                        kite.profile()  # raises error if token invalid
                        print("✅ Connected to Kite with saved access token!")
                        return kite, True
                    except Exception:
                        print("⚠ Saved token expired or invalid. Logging in manually.")
        except Exception as e:
            print("⚠ Could not read saved token:", str(e))

    # -------------------------
    # Step 2: Manual login
    # -------------------------
    api_key = input("Enter your API key: ")
    api_secret = input("Enter your API secret: ")

    kite = KiteConnect(api_key=api_key)
    login_url = kite.login_url()
    print("\nGo to this URL, login, and authorize access:")
    print(login_url)
    webbrowser.open(login_url)

    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        request_token = input(f"\nPaste the request token (Attempt {attempt}/3): ").strip()
        try:
            # Generate session and set access token
            data = kite.generate_session(request_token, api_secret=api_secret)
            kite.set_access_token(data["access_token"])

            # Save only api_key + access_token in TXT
            with open(TOKEN_FILE, "w") as f:
                f.write(f"{api_key}\n{data['access_token']}\n")

            print("✅ Login successful and token saved!")
            return kite, False

        except Exception as e:
            print(f"❌ Invalid token. Error: {e}")
            if attempt == max_attempts:
                print("❌ Maximum attempts reached. Exiting.")
                return None, False
            print("Try again.")

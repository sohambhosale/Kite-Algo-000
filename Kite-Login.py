from kiteconnect import KiteConnect

# Step 1: Enter your API key and secret
api_key = input("Enter your API key: ")
api_secret = input("Enter your API secret: ")

# Initialize KiteConnect
kite = KiteConnect(api_key=api_key)

# Step 2: Get login URL and print it
print("\n1. Go to this login URL, login, and authorize access:")
print(kite.login_url())

# Step 3: Paste the request token from redirect URL
request_token = input("\n2. Paste the request token here: ")

# Step 4: Generate access token
try:
    data = kite.generate_session(request_token, api_secret=api_secret)
    kite.set_access_token(data["access_token"])

    print("\n✅ Login successful!")
    print("Your Access Token is:", data["access_token"])

except Exception as e:
    print("\n❌ Login failed! Error:", str(e))

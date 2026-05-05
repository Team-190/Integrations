import time
import hashlib
import hmac
import base64
from urllib.parse import urlparse
import requests
import json
import pandas
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import base64

# Replace with your Onshape API credentials
ACCESS_KEY = "yRdKtl15ro3N8A9ChtJAq6Nd"
SECRET_KEY = "uV1e9CVNkDEvn5ViaQZHqODksAIcFVkNitLy0oBihzUXdtdt"

# Replace with the document, workspace, and element IDs
BASE_URL = "https://cad.onshape.com"
DOCUMENT_ID = "8a2726d5d7faa8a8c8571d66"
WORKSPACE_ID = "362971ca0c838fb2288e74e5"
ASSEMBLY_ID = "e6d0b198899ea73519561b9f"

def create_headers(method, url, query_string="", body=""):
    """Generate signed headers for Onshape API."""
    current_time = str(int(time.time() * 1000))  # Current time in milliseconds
    url_parts = urlparse(url)
    request_path = url_parts.path
    if query_string:
        request_path += "?" + query_string

    # Construct the HMAC signature
    prehash_string = (method + "\n" + current_time + "\n" + request_path + "\n" + body).lower()
    signature = hmac.new(SECRET_KEY.encode('utf-8'), prehash_string.encode('utf-8'), hashlib.sha256).digest()
    signature_base64 = base64.b64encode(signature).decode('utf-8')

    headers = {
        "Authorization": f"On {ACCESS_KEY}:HmacSHA256:{signature_base64}",
        "On-Nonce": current_time,
        "Date": current_time,
        "Content-Type": "application/json",
    }
    return headers

def fetch_bom():
    """Fetch the BOM from an Onshape assembly."""
    endpoint = f"{BASE_URL}/api/assemblies/d/{DOCUMENT_ID}/w/{WORKSPACE_ID}/e/{ASSEMBLY_ID}/bom?indented=false&multiLevel=false&generateIfAbsent=false&includeItemMicroversions=false&includeTopLevelAssemblyRow=false&thumbnail=false"
    method = "GET"
    headers = create_headers(method, endpoint)
    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching BOM: {response.status_code}, {response.text}")
        return None

def save_to_json(data, filename="bom.json"):
    """Save data to a JSON file."""
    with open(filename, "w") as file:
        json.dump(data, file, indent=4)
    print(f"BOM saved to {filename}")

def parse_json(file_path):
    try:
        # Load the JSON file
        with open(file_path, 'r') as file:
            data = json.load(file)

        filtered_data = [datum for datum in data['bomTable']['items'] if datum.get('partNumber') and datum.get('partNumber').startswith('P-25')]

        for item in filtered_data:
            if "material" in item and "id" in item["material"]:
                item["material"] = item["material"]["id"]

        if filtered_data:
            save_to_json(filtered_data, "filtered_bom.json")
            print("Filtered BOM saved to filtered_bom.json")
        else:
            print("No items with partNumber found.")

    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except json.JSONDecodeError:
        print("Invalid JSON file.")

def get_dataframe():
    df = pandas.read_json("filtered_bom.json")
    cols = ['name', 'description', 'vendor', 'partNumber', 'material', 'quantity', 'revision', 'manufacturingmethod']
    print(df.loc[:, cols])
    return df.loc[:, cols]

def append_to_google_sheet(df):
    # Define the scope
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    # Add credentials to the account
    creds = ServiceAccountCredentials.from_json_keyfile_name("cred.json", scope)

    # Authorize the clientsheet 
    client = gspread.authorize(creds)

    # Get the sheet
    sheet = client.open("2025GammaBOM")
    worksheet = sheet.worksheet("Sheet1")

    # Append each row of the DataFrame to the Google Sheet
    worksheet.update([df.columns.values.tolist()] + df.values.tolist())


def main():
    bom_data = fetch_bom()
    if bom_data:
        save_to_json(bom_data)
    parse_json("bom.json")
    df = get_dataframe()
    append_to_google_sheet(df)
    exit()

if __name__ == "__main__":
    main()

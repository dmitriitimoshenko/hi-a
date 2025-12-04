import requests

sheet_names = [
    "applied",
    "meeting_inv",
    "meeting_crt",
    "meeting_upd",
    "meeting_cncl",
    "offer",
    "denied",
    "pending",
]

gsa_url = "http://localhost:8083/api/sheet/get"
mp_url = "http://localhost:8081/api/embd_lrn/create"
headers = {"content-type": "application/json", "x-api-version": "1"}

for sheet in sheet_names:
    data = {
        "page": f"{sheet}",
        "ceil_from": "A2",
        "ceil_to": "D1000",
        "id": "1aL-YihxtzedaIT2kPBPAXiQ4C5iKAI_ANjJTJvVbYng",
    }
    print(f"request_data: {data}")

    response = requests.post(gsa_url, headers=headers, json=data)
    if response.status_code == 200 or response.json()["status"]:
        print(f"Sheet: {sheet} was read")
    else:
        print(f"Error reading sheet '{sheet}': {response.status_code}")
        exit(1)

    if sheet == "meeting_crt" or sheet == "meeting_upd" or sheet == "meeting_cncl":
        sheet = "meeting_action"

    data = {"label": sheet, "items": response.json().get("data", [])}
    response = requests.post(mp_url, headers=headers, json=data)
    if response.status_code == 201:
        print(f"Data for label '{sheet}' was successfully sent to mail processor")
    else:
        print(
            f"Error sending data for sheet '{sheet}': {response.status_code}, {response.text}"
        )
        exit(1)

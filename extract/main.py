import os
import hashlib
import requests

baseUrl = os.getenv("DEYE_BASE_URL", "https://india-developer.deyecloud.com")
AppId = os.getenv("DEYE_APP_ID")
appSecret = os.getenv("DEYE_APP_SECRET")
email = os.getenv("DEYE_EMAIL")
password = os.getenv("DEYE_PASSWORD")
hashed_password = hashlib.sha256(password.encode()).hexdigest()


def postApi():
    url = f"{baseUrl}/v1.0/account/token?appId={AppId}"
    headers = {"Content-Type": "application/json"}
    data = {
        "appSecret": appSecret,
        "email": email,
        "password": hashed_password,
    }

    res = requests.post(url, headers=headers, json=data)
    data = res.json()
    access_token = data.get("accessToken")
    res.raise_for_status()
    return access_token, res


access_token, res = postApi()


def getApi_raw():
    device_sn = os.getenv("DEYE_DEVICE_SN")
    url = f"{baseUrl}/v1.0/device/latest"
    headers = {
        "Authorization": f"bearer {access_token}",
        "Content-Type": "application/json;charset=UTF-8",
    }
    data = {
        "deviceList": [device_sn],
    }
    res = requests.post(url, headers=headers, json=data)
    print(res.status_code)
    print(res.json())
    return res


def getApi_his():
    device_sn = os.getenv("DEYE_DEVICE_SN")
    url = f"{baseUrl}/v1.0/device/history"
    headers = {
        "Authorization": f"bearer {access_token}",
        "Content-Type": "application/json;charset=UTF-8",
    }
    data = {
        "deviceSn": device_sn,
        "endAt": "2026-04-29",
        "granularity": 1,
        "measurePoints": ["DCVoltagePV1", "DCCurrentPV1", "DCPowerPV1"],
        "startAt": "2026-04-29",
    }
    res = requests.post(url, headers=headers, json=data)
    print(res.status_code)
    print(res.json())
    return res


if __name__ == "__main__":
    data = getApi_his()
    print(data)

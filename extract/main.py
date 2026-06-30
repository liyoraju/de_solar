import os
import hashlib
import requests
import time
from datetime import datetime


class extract:
    baseUrl = os.getenv("DEYE_BASE_URL", "https://india-developer.deyecloud.com")
    AppId = os.getenv("DEYE_APP_ID")
    appSecret = os.getenv("DEYE_APP_SECRET")
    email = os.getenv("DEYE_EMAIL")
    password = os.getenv("DEYE_PASSWORD")
    hashed_password = hashlib.sha256(password.encode()).hexdigest()

    def __init__(self):
        self.access_token = None

    def extract_token(self):
        url = f"{self.baseUrl}/v1.0/account/token?appId={self.AppId}"
        headers = {"Content-Type": "application/json"}
        data = {
            "appSecret": self.appSecret,
            "email": self.email,
            "password": self.hashed_password,
        }

        res = requests.post(url, headers=headers, json=data)
        data = res.json()
        self.access_token = data.get("accessToken")
        res.raise_for_status()
        return self.access_token

    def extract_raw(self):
        device_sn = os.getenv("DEYE_DEVICE_SN")
        url = f"{self.baseUrl}/v1.0/device/latest"
        headers = {
            "Authorization": f"bearer {self.access_token}",
            "Content-Type": "application/json;charset=UTF-8",
        }
        data = {
            "deviceList": [device_sn],
        }
        res = requests.post(url, headers=headers, json=data)
        print(res.status_code)
        print(res.json())
        return res

    def extract_history(
        self, startAt=None, endAt=None, granularity=1, measurePoints=None
    ):
        device_sn = os.getenv("DEYE_DEVICE_SN")
        url = f"{self.baseUrl}/v1.0/device/history"
        headers = {
            "Authorization": f"bearer {self.access_token}",
            "Content-Type": "application/json;charset=UTF-8",
        }
        data = {
            "deviceSn": device_sn,
            "endAt": endAt,
            "granularity": granularity,
            "startAt": startAt,
        }
        if measurePoints:
            data["measurePoints"] = measurePoints
        res = requests.post(url, headers=headers, json=data)
        print(res.status_code)
        print(res.json())
        return res


if __name__ == "__main__":
    try:
        n = 0
        ext = extract()
        while n != 6:
            ext.extract_token()
            data = ext.extract_history(
                startAt=f"{datetime.today():%Y-%m-%d}",
                measurePoints=["DCVoltagePV1", "DCCurrentPV1", "DCPowerPV1"],
                endAt=f"{datetime.today():%Y-%m-%d}",
            )
            print(data)
            time.sleep(10)
            n += 1
    except KeyboardInterrupt:
        print("Stopped")

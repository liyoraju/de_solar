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
            # data = ext.extract_history(
            #     startAt=f"{datetime.today():%Y-%m-%d}",
            #     measurePoints=["DCVoltagePV1", "DCCurrentPV1", "DCPowerPV1"],
            #     endAt=f"{datetime.today():%Y-%m-%d}",
            # )
            data = ext.extract_raw()
            print(data)
            time.sleep(10)
            n += 1
    except KeyboardInterrupt:
        print("Stopped")


data = {
    "code": "1000000",
    "msg": "success",
    "success": True,
    "requestId": "dddf59fee5b671be",
    "deviceDataList": [
        {
            "deviceSn": "2601078791",
            "deviceType": "INVERTER",
            "deviceState": 1,
            "collectionTime": 1782887125,
            "dataList": [
                {"key": "RatedPower", "value": "3000.00", "unit": "W"},
                {"key": "DCVoltagePV1", "value": "179.70", "unit": "V"},
                {"key": "DCVoltagePV2", "value": "0.00", "unit": "V"},
                {"key": "DCVoltagePV3", "value": "0.00", "unit": "V"},
                {"key": "DCVoltagePV4", "value": "0.00", "unit": "V"},
                {"key": "DCVoltagePV5", "value": "0.00", "unit": "V"},
                {"key": "DCVoltagePV6", "value": "0.00", "unit": "V"},
                {"key": "DCVoltagePV7", "value": "0.00", "unit": "V"},
                {"key": "DCVoltagePV8", "value": "0.00", "unit": "V"},
                {"key": "DCCurrentPV1", "value": "15.60", "unit": "A"},
                {"key": "DCCurrentPV2", "value": "0.00", "unit": "A"},
                {"key": "DCCurrentPV3", "value": "0.00", "unit": "A"},
                {"key": "DCCurrentPV4", "value": "0.00", "unit": "A"},
                {"key": "DCCurrentPV5", "value": "0.00", "unit": "A"},
                {"key": "DCCurrentPV6", "value": "0.00", "unit": "A"},
                {"key": "DCCurrentPV7", "value": "0.00", "unit": "A"},
                {"key": "DCCurrentPV8", "value": "0.00", "unit": "A"},
                {"key": "DCPowerPV1", "value": "2803.32", "unit": "W"},
                {"key": "DCPowerPV2", "value": "0.00", "unit": "W"},
                {"key": "DCPowerPV3", "value": "0.00", "unit": "W"},
                {"key": "DCPowerPV4", "value": "0.00", "unit": "W"},
                {"key": "DCPowerPV5", "value": "0.00", "unit": "W"},
                {"key": "DCPowerPV6", "value": "0.00", "unit": "W"},
                {"key": "DCPowerPV7", "value": "0.00", "unit": "W"},
                {"key": "DCPowerPV8", "value": "0.00", "unit": "W"},
                {"key": "ACVoltageRUA", "value": "249.30", "unit": "V"},
                {"key": "ACVoltageSVB", "value": "0.00", "unit": "V"},
                {"key": "ACVoltageTWC", "value": "0.00", "unit": "V"},
                {"key": "ACCurrentRUA", "value": "10.30", "unit": "A"},
                {"key": "ACCurrentSVB", "value": "0.00", "unit": "A"},
                {"key": "ACCurrentTWC", "value": "0.00", "unit": "A"},
                {"key": "ACOutputFrequencyR", "value": "49.93", "unit": "Hz"},
                {"key": "TotalActiveACOutputPower", "value": "2507.40", "unit": "W"},
                {"key": "ABLineVoltage", "value": "249.30", "unit": "V"},
                {"key": "BCLineVoltage", "value": "0.00", "unit": "V"},
                {"key": "ACLineVoltage", "value": "0.00", "unit": "V"},
                {"key": "TotalActiveProduction", "value": "674.60", "unit": "kWh"},
                {"key": "DailyActiveProduction", "value": "3.80", "unit": "kWh"},
                {"key": "InverterOutputPowerL1", "value": "0", "unit": "W"},
                {"key": "InverterOutputPowerL2", "value": "0", "unit": "W"},
                {"key": "InverterOutputPowerL3", "value": "0", "unit": "W"},
                {"key": "TotalGridFeedIn", "value": "0.00", "unit": "kWh"},
                {"key": "TotalEnergyPurchased", "value": "0.00", "unit": "kWh"},
                {"key": "TotalConsumptionPower", "value": "2586", "unit": "W"},
                {"key": "TotalConsumption", "value": "0.00", "unit": "kWh"},
            ],
        }
    ],
}

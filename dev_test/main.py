import json
import os
import hashlib
from datetime import datetime
import requests
import time
from validate.history import DeviceData
from validate.raw import Response
import logging
from pydantic import ValidationError
import time
from confluent_kafka import Producer
from validate.raw import flattern_data, InverterData, Response
import logging
import json
from validate.history import DeviceData


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
        return res.json()

    def extract_history(self, startAt=None, endAt=None, granularity=1):
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
        dataList = {
            1: [
                "RatedPower",
                "DCVoltagePV1",
                "DCVoltagePV2",
                "DCVoltagePV3",
                "DCVoltagePV4",
            ],
            2: [
                "DCVoltagePV5",
                "DCVoltagePV6",
                "DCVoltagePV7",
                "DCVoltagePV8",
                "DCCurrentPV1",
            ],
            3: [
                "DCCurrentPV2",
                "DCCurrentPV3",
                "DCCurrentPV4",
                "DCCurrentPV5",
                "DCCurrentPV6",
            ],
            4: [
                "DCCurrentPV7",
                "DCCurrentPV8",
                "DCPowerPV1",
                "DCPowerPV2",
                "DCPowerPV3",
            ],
            5: [
                "DCPowerPV4",
                "DCPowerPV5",
                "DCPowerPV6",
                "DCPowerPV7",
                "DCPowerPV8",
            ],
            6: [
                "ACVoltageRUA",
                "ACVoltageSVB",
                "ACVoltageTWC",
                "ACCurrentRUA",
                "ACCurrentSVB",
            ],
            7: [
                "ACCurrentTWC",
                "ACOutputFrequencyR",
                "TotalActiveACOutputPower",
                "ABLineVoltage",
                "BCLineVoltage",
            ],
            8: [
                "ACLineVoltage",
                "TotalActiveProduction",
                "DailyActiveProduction",
                "InverterOutputPowerL1",
                "InverterOutputPowerL2",
            ],
            9: [
                "InverterOutputPowerL3",
                "TotalGridFeedIn",
                "TotalEnergyPurchased",
                "TotalConsumptionPower",
                "TotalConsumption",
            ],
        }
        # if granularity == 1:
        #     data["measurePoints"] = [
        #         "RatedPower",
        #     ]
        all_data = []
        if granularity == 1:
            for measure_points in dataList.values():
                data["measurePoints"] = measure_points
                res = requests.post(url, headers=headers, json=data)
                if res.status_code == 200:
                    device_data = DeviceData.model_validate(res.json())
                    all_data.append(device_data)
                else:
                    print(f"failed due to {res.raise_for_status}: {res.text}")
            return all_data

        res = requests.post(url, headers=headers, json=data)
        print(res.status_code)
        return res.json()


if __name__ == "__main__":
    # try:
    #     ext = extract()
    #     ext.extract_token()
    #     raw_data = ext.extract_raw()

    #     h_data = ext.extract_history(
    #         startAt="2026",
    #         endAt="2026",
    #         granularity=4,
    #     )
    #     h_data = DeviceData.model_validate(h_data)
    #     print(h_data)

    # with open("history_data_valid.json", "w") as f:
    #     json.dump([d.model_dump() for d in h_data], f, indent=4)
    def history_push_to_kafka(startAt: str, endAt: str, granularity: int):
        try:
            ext = extract()
            logging.info("deye-poller started")

            ext.extract_token()
            data = ext.extract_history(
                startAt=startAt,
                endAt=endAt,
                granularity=granularity,
            )
            if granularity == 1:
                for d in data:
                    value = json.dumps(d, default=str).encode("utf-8")

                    # producer.produce(
                    #     topic=topic, value=value, callback=delivery_message
                    # )
                    # producer.poll(0)
            else:
                value = DeviceData.model_validate(data)
                value = value.model_dump_json().encode("utf-8")
            # producer.produce(topic=topic, value=value, callback=delivery_message)

        except ValidationError as ve:
            logging.error(f"Validation error : {ve}")
            # Create a minimal dead letter record with available data
        except KeyboardInterrupt:
            logging.info("deye-poller stopped")
            # producer.flush()
        # except Exception as e:
        #     logging.error(f"Fatal error: {e}", exc_info=True)
        #     producer.flush()
        #     raise
        return 0

    history_push_to_kafka("2026-06-04", "2026-07-03", 1)
    # except KeyboardInterrupt:
    #     print("Stopped")
dataList = [
    {
        1: [
            "RatedPower",
            "DCVoltagePV1",
            "DCVoltagePV2",
            "DCVoltagePV3",
            "DCVoltagePV4",
        ]
    },
    {
        2: [
            "DCVoltagePV5",
            "DCVoltagePV6",
            "DCVoltagePV7",
            "DCVoltagePV8",
            "DCCurrentPV1",
        ]
    },
    {
        3: [
            "DCCurrentPV2",
            "DCCurrentPV3",
            "DCCurrentPV4",
            "DCCurrentPV5",
            "DCCurrentPV6",
        ]
    },
    {
        4: [
            "DCCurrentPV7",
            "DCCurrentPV8",
            "DCPowerPV1",
            "DCPowerPV2",
            "DCPowerPV3",
        ]
    },
    {
        5: [
            "DCPowerPV4",
            "DCPowerPV5",
            "DCPowerPV6",
            "DCPowerPV7",
            "DCPowerPV8",
        ]
    },
    {
        6: [
            "ACVoltageRUA",
            "ACVoltageSVB",
            "ACVoltageTWC",
            "ACCurrentRUA",
            "ACCurrentSVB",
        ]
    },
    {
        7: [
            "ACCurrentTWC",
            "ACOutputFrequencyR",
            "TotalActiveACOutputPower",
            "ABLineVoltage",
            "BCLineVoltage",
        ]
    },
    {
        8: [
            "ACLineVoltage",
            "TotalActiveProduction",
            "DailyActiveProduction",
            "InverterOutputPowerL1",
            "InverterOutputPowerL2",
        ]
    },
    {
        9: [
            "InverterOutputPowerL3",
            "TotalGridFeedIn",
            "TotalEnergyPurchased",
            "TotalConsumptionPower",
            "TotalConsumption",
        ]
    },
]

raw_data = {
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


hist_1 = {
    "code": "1000000",
    "msg": "success",
    "success": True,
    "requestId": "af0dd7ab349437d3",
    "deviceSn": "2601078791",
    "deviceId": 373268,
    "deviceType": "INVERTER",
    "granularity": 1,
    "dataList": [
        {
            "time": "1780407509",
            "itemList": [
                {"unit": "V", "value": "48.00", "key": "DCVoltagePV1"},
                {"unit": "W", "value": "3000.00", "key": "RatedPower"},
            ],
        }
    ],
}

hist_2_daily = {
    "code": "1000000",
    "msg": "success",
    "success": True,
    "requestId": "7d0e6ca1cd8b26c0",
    "deviceSn": "2601078791",
    "deviceId": 373268,
    "deviceType": "INVERTER",
    "granularity": 2,
    "dataList": [
        {
            "time": "2026-05-30",
            "itemList": [
                {"unit": "kWh", "value": "12.70", "key": "Production"},
                {"unit": "kWh", "value": "0.00", "key": "GridFeed-in"},
                {"unit": "kWh", "value": "0.00", "key": "Consumption"},
                {"unit": "kWh", "value": "0.00", "key": "ElectricityPurchasing"},
            ],
        },
    ],
}

hist_3_monthly = {
    "code": "1000000",
    "msg": "success",
    "success": True,
    "requestId": "a11ebcd9a26d36d7",
    "deviceSn": "2601078791",
    "deviceId": 373268,
    "deviceType": "INVERTER",
    "granularity": 3,
    "dataList": [
        {
            "time": "2026-5",
            "itemList": [
                {"unit": "kWh", "value": "352.10", "key": "Production"},
                {"unit": "kWh", "value": "0.00", "key": "GridFeed-in"},
                {"unit": "kWh", "value": "0.00", "key": "Consumption"},
                {"unit": "kWh", "value": "0.00", "key": "ElectricityPurchasing"},
            ],
        },
        {
            "time": "2026-6",
            "itemList": [
                {"unit": "kWh", "value": "290.30", "key": "Production"},
                {"unit": "kWh", "value": "0.00", "key": "GridFeed-in"},
                {"unit": "kWh", "value": "0.00", "key": "Consumption"},
                {"unit": "kWh", "value": "0.00", "key": "ElectricityPurchasing"},
            ],
        },
        {
            "time": "2026-7",
            "itemList": [
                {"unit": "kWh", "value": "14.70", "key": "Production"},
                {"unit": "kWh", "value": "0.00", "key": "GridFeed-in"},
                {"unit": "kWh", "value": "0.00", "key": "Consumption"},
                {"unit": "kWh", "value": "0.00", "key": "ElectricityPurchasing"},
            ],
        },
    ],
}
hist_4_yearly = {
    "code": "1000000",
    "msg": "success",
    "success": True,
    "requestId": "ec19188d90bd00a7",
    "deviceSn": "2601078791",
    "deviceId": 373268,
    "deviceType": "INVERTER",
    "granularity": 4,
    "dataList": [
        {
            "time": "2026",
            "itemList": [
                {"unit": "kWh", "name": "Production", "value": "680.80"},
                {"unit": "kWh", "name": "GridFeed-in", "value": "0.00"},
                {"unit": "kWh", "name": "Consumption", "value": "0.00"},
                {"unit": "kWh", "name": "ElectricityPurchasing", "value": "0.00"},
            ],
        }
    ],
}

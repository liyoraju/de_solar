from collections import defaultdict
import os
import hashlib
import json
from pydantic import ValidationError
import requests
import time
from confluent_kafka import Producer
from validate.history import DeviceData
from validate.raw import flattern_data, InverterData, Response
import logging

# Validate required environment variables at startup
required_env_vars = [
    "DEYE_APP_ID",
    "DEYE_APP_SECRET",
    "DEYE_EMAIL",
    "DEYE_PASSWORD",
    "DEYE_DEVICE_SN",
]

missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
    print(f"ERROR: {error_msg}")
    raise EnvironmentError(error_msg)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


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
        res.raise_for_status()
        data = res.json()
        self.access_token = data.get("accessToken")
        logging.info("Token acquired successfully")
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
        res.raise_for_status()
        logging.info(f"Raw data fetched for device {device_sn}")
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

            merged = defaultdict(
                lambda: {
                    "device_sn": None,
                    "device_type": None,
                    "granularity": None,
                    "collection_time": None,
                }
            )

            for device_data in all_data:
                for time_record in device_data.dataList:
                    ts = time_record.time

                    # set identity fields once
                    merged[ts]["device_sn"] = device_data.deviceSn
                    merged[ts]["device_type"] = device_data.deviceType
                    merged[ts]["granularity"] = device_data.granularity
                    merged[ts]["collection_time"] = ts

                    # merge all measurePoints for this timestamp
                    for item in time_record.itemList:
                        merged[ts][item.key] = float(item.value)

            return list(merged.values())

        res = requests.post(url, headers=headers, json=data)
        print(res.status_code)
        return res.json()


def dead_letter(data, ve):
    dead_data = data.copy()
    dead_data["_error"] = str(ve)
    dead_value = json.dumps(dead_data).encode("utf-8")
    producer.produce(
        topic="dead_letter",
        value=dead_value,
        callback=delivery_message,
    )


def delivery_message(err, msg):
    if err:
        logging.error(f"Kafka delivery failed: {err}")
    else:
        logging.info("Kafka delivery succeeded")


def merge_chunks(all_data: list) -> list[dict]:
    """
    Merge 9 chunked responses into one dict per timestamp.
    all_data = list[DeviceDatah1]
    """
    # merged[ts] = all fields accumulated across 9 chunks
    merged = defaultdict(
        lambda: {
            "device_sn": None,
            "device_type": None,
            "granularity": None,
        }
    )

    for device_data in all_data:
        for time_record in device_data.dataList:
            ts = time_record.time

            # set identity fields once
            merged[ts]["device_sn"] = device_data.deviceSn
            merged[ts]["device_type"] = device_data.deviceType
            merged[ts]["granularity"] = device_data.granularity
            merged[ts]["ts"] = ts

            # merge all measurePoints for this timestamp
            for item in time_record.itemList:
                merged[ts][item.key] = float(item.value)

    return list(merged.values())


if __name__ == "__main__":
    producer_config = {"bootstrap.servers": "broker:29092"}

    producer = Producer(producer_config)

    # Near-real-time data extraction loop
    try:
        ext = extract()
        logging.info("deye-poller started")
        while True:
            ext.extract_token()
            data = ext.extract_raw()
            try:
                response = Response.model_validate(data)
                if not response.success:
                    logging.error(f"Failed to fetch data: {response.msg}")
                    time.sleep(60)
                    continue

                for device_data in response.deviceDataList:
                    device_flat = flattern_data(device_data)
                    try:
                        main_data = InverterData.model_validate(device_flat)
                        value = main_data.model_dump_json().encode("utf-8")
                        producer.produce(
                            topic="raw_data", value=value, callback=delivery_message
                        )
                        logging.info(
                            f"Produced to Kafka: {main_data.device_sn} | DC PV1: {main_data.dc_power_pv1}W"
                        )
                    except ValidationError as ve:
                        logging.error(
                            f"Validation error for device {device_data.deviceSn}: {ve}"
                        )
                        # Create a minimal dead letter record with available data
                        dead_letter(data, ve)

            except ValidationError as ve:
                logging.error(f"Validation error: {ve}")
                dead_letter(data, ve)
                time.sleep(60)
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("deye-poller stopped")
        producer.flush()
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        producer.flush()
        raise

    # Historical data extraction (one-time run)
    try:
        ext = extract()
        logging.info("deye-poller started")

        ext.extract_token()
        data = ext.extract_raw()
        try:
            response = Response.model_validate(data)
            if not response.success:
                logging.error(f"Failed to fetch data: {response.msg}")
            for device_data in response.deviceDataList:
                device_flat = flattern_data(device_data)
                try:
                    main_data = InverterData.model_validate(device_flat)
                    value = main_data.model_dump_json().encode("utf-8")
                    producer.produce(
                        topic="raw_data", value=value, callback=delivery_message
                    )
                    logging.info(
                        f"Produced to Kafka: {main_data.device_sn} | DC PV1: {main_data.dc_power_pv1}W"
                    )
                except ValidationError as ve:
                    logging.error(
                        f"Validation error for device {device_data.deviceSn}: {ve}"
                    )
                    # Create a minimal dead letter record with available data

        except ValidationError as ve:
            logging.error(f"Validation error: {ve}")
    except KeyboardInterrupt:
        logging.info("deye-poller stopped")
        producer.flush()
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        producer.flush()
        raise

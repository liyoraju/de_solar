from pydantic import ValidationError
import time
from confluent_kafka import Producer
from validate.raw import flattern_data, InverterData, Response
import logging
from main_def import delivery_message, dead_letter, extract
import json
from validate.history import DeviceData
from history import get_last_date, save_last_date
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


producer_config = {"bootstrap.servers": "broker:29092"}
producer = Producer(producer_config)


def history_push_to_kafka(startAt: str, endAt: str, granularity: int, topic: str):
    try:
        ext = extract()
        logging.info(f"Backfill [{topic}] {startAt} -> {endAt}")
        ext.extract_token()
        data = ext.extract_history(
            startAt=startAt, endAt=endAt, granularity=granularity
        )
        if granularity == 1:
            for d in data:
                value = json.dumps(d, default=str).encode("utf-8")
                producer.produce(topic=topic, value=value, callback=delivery_message)
                producer.poll(0)
        else:
            value = DeviceData.model_validate(data)
            value = value.model_dump_json().encode("utf-8")
            producer.produce(topic=topic, value=value, callback=delivery_message)
    except ValidationError as ve:
        logging.error(f"Validation error : {ve}")
    except KeyboardInterrupt:
        logging.info("deye-poller stopped")
    return 0


def backfill_granularity_1():
    last = get_last_date(1)
    if not last:
        return
    current = datetime.strptime(last, "%Y-%m-%d").date()
    today = datetime.today().date()
    while current <= today:
        date_str = current.strftime("%Y-%m-%d")
        history_push_to_kafka(date_str, date_str, 1, "h1_data")
        save_last_date(1, date_str)
        current += timedelta(days=1)


def backfill_granularity_2():
    last = get_last_date(2)
    if not last:
        return
    current = datetime.strptime(last, "%Y-%m-%d").date()
    today = datetime.today().date()
    while current <= today:
        date_str = current.strftime("%Y-%m-%d")
        history_push_to_kafka(date_str, date_str, 2, "h2_data")
        save_last_date(2, date_str)
        current += timedelta(days=1)


def backfill_granularity_3():
    last = get_last_date(3)
    if not last:
        return
    current = datetime.strptime(last, "%Y-%m")
    today = datetime.today()
    while current <= today:
        month_str = current.strftime("%Y-%m")
        history_push_to_kafka(month_str, month_str, 3, "h3_data")
        save_last_date(3, month_str)
        current += relativedelta(months=1)


def backfill_granularity_4():
    last = get_last_date(4)
    if not last:
        return
    current = datetime.strptime(last, "%Y")
    today = datetime.today()
    while current <= today:
        year_str = current.strftime("%Y")
        history_push_to_kafka(year_str, year_str, 4, "h4_data")
        save_last_date(4, year_str)
        current += relativedelta(years=1)


backfill_functions = [
    backfill_granularity_1,
    backfill_granularity_2,
    backfill_granularity_3,
    backfill_granularity_4,
]

for backfill in backfill_functions:
    try:
        backfill()
    except Exception as e:
        # Log the specific function that failed, but keep the loop moving
        logging.error(f"{backfill.__name__} failed: {e}", exc_info=True)

# Flush everything at the end of the batch
producer.flush()


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

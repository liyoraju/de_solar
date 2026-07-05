from pydantic import ValidationError
import time
from confluent_kafka import Producer
from validate.raw import flattern_data, InverterData, Response
import logging
from main_def import delivery_message, dead_letter, extract
import json
from validate.history import DeviceData


producer_config = {"bootstrap.servers": "broker:29092"}

producer = Producer(producer_config)


# Historical granularity 1 data extraction (one-time run)
def history_push_to_kafka(startAt: str, endAt: str, granularity: int, topic: str):
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
                producer.produce(topic=topic, value=value, callback=delivery_message)
                producer.poll(0)
        else:
            value = DeviceData.model_validate(data)
            value = value.model_dump_json().encode("utf-8")
            producer.produce(topic=topic, value=value, callback=delivery_message)

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

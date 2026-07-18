from pydantic import BaseModel, field_validator, Field, AliasChoices
from collections import defaultdict
valid_keys = {
    "RatedPower",
    "DCVoltagePV1",
    "DCVoltagePV2",
    "DCVoltagePV3",
    "DCVoltagePV4",
    "DCVoltagePV5",
    "DCVoltagePV6",
    "DCVoltagePV7",
    "DCVoltagePV8",
    "DCCurrentPV1",
    "DCCurrentPV2",
    "DCCurrentPV3",
    "DCCurrentPV4",
    "DCCurrentPV5",
    "DCCurrentPV6",
    "DCCurrentPV7",
    "DCCurrentPV8",
    "DCPowerPV1",
    "DCPowerPV2",
    "DCPowerPV3",
    "DCPowerPV4",
    "DCPowerPV5",
    "DCPowerPV6",
    "DCPowerPV7",
    "DCPowerPV8",
    "ACVoltageRUA",
    "ACVoltageSVB",
    "ACVoltageTWC",
    "ACCurrentRUA",
    "ACCurrentSVB",
    "ACCurrentTWC",
    "ACOutputFrequencyR",
    "TotalActiveACOutputPower",
    "ABLineVoltage",
    "BCLineVoltage",
    "ACLineVoltage",
    "TotalActiveProduction",
    "DailyActiveProduction",
    "InverterOutputPowerL1",
    "InverterOutputPowerL2",
    "InverterOutputPowerL3",
    "TotalGridFeedIn",
    "TotalEnergyPurchased",
    "TotalConsumptionPower",
    "TotalConsumption",
    "Production",
    "GridFeed-in",
    "Consumption",
    "ElectricityPurchasing",
}


class ItemList(BaseModel):
    model_config = {"extra": "forbid"}
    key: str = Field(validation_alias=AliasChoices("name", "key"))
    value: float = 0.0
    unit: str

    @field_validator("key")
    def validate_key(cls, v):
        if v not in valid_keys:
            raise ValueError(f"Invalid key: {v}")
        return v


class DataList(BaseModel):
    model_config = {"extra": "forbid"}
    time: str
    itemList: list[ItemList]


class DeviceData(BaseModel):
    model_config = {"extra": "ignore"}
    code: str
    msg: str
    success: bool
    requestId: str
    deviceSn: str
    deviceId: int
    deviceType: str
    granularity: int
    dataList: list[DataList]

key_map = {
    "RatedPower": "rated_power",
    "DCVoltagePV1": "dc_voltage_pv1",
    "DCVoltagePV2": "dc_voltage_pv2",
    "DCVoltagePV3": "dc_voltage_pv3",
    "DCVoltagePV4": "dc_voltage_pv4",
    "DCVoltagePV5": "dc_voltage_pv5",
    "DCVoltagePV6": "dc_voltage_pv6",
    "DCVoltagePV7": "dc_voltage_pv7",
    "DCVoltagePV8": "dc_voltage_pv8",
    "DCCurrentPV1": "dc_current_pv1",
    "DCCurrentPV2": "dc_current_pv2",
    "DCCurrentPV3": "dc_current_pv3",
    "DCCurrentPV4": "dc_current_pv4",
    "DCCurrentPV5": "dc_current_pv5",
    "DCCurrentPV6": "dc_current_pv6",
    "DCCurrentPV7": "dc_current_pv7",
    "DCCurrentPV8": "dc_current_pv8",
    "DCPowerPV1": "dc_power_pv1",
    "DCPowerPV2": "dc_power_pv2",
    "DCPowerPV3": "dc_power_pv3",
    "DCPowerPV4": "dc_power_pv4",
    "DCPowerPV5": "dc_power_pv5",
    "DCPowerPV6": "dc_power_pv6",
    "DCPowerPV7": "dc_power_pv7",
    "DCPowerPV8": "dc_power_pv8",
    "ACVoltageRUA": "ac_voltage_rua",
    "ACVoltageSVB": "ac_voltage_svb",
    "ACVoltageTWC": "ac_voltage_twc",
    "ACCurrentRUA": "ac_current_rua",
    "ACCurrentSVB": "ac_current_svb",
    "ACCurrentTWC": "ac_current_twc",
    "ACOutputFrequencyR": "ac_output_frequency_r",
    "TotalActiveACOutputPower": "total_active_ac_output_power",
    "ABLineVoltage": "ab_line_voltage",
    "BCLineVoltage": "bc_line_voltage",
    "ACLineVoltage": "ac_line_voltage",
    "TotalActiveProduction": "total_active_production",
    "DailyActiveProduction": "daily_active_production",
    "InverterOutputPowerL1": "inverter_output_power_l1",
    "InverterOutputPowerL2": "inverter_output_power_l2",
    "InverterOutputPowerL3": "inverter_output_power_l3",
    "TotalGridFeedIn": "total_grid_feed_in",
    "TotalEnergyPurchased": "total_energy_purchased",
    "TotalConsumptionPower": "total_consumption_power",
    "TotalConsumption": "total_consumption",
}

def flattern(all_data):
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
                if item.key in key_map:
                    merged[ts][key_map[item.key]] = float(item.value)
    return list(merged.values())
        
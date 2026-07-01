from pydantic import BaseModel


class DataPoint(BaseModel):
    key: str
    value: str
    unit: str


class DeviceData(BaseModel):
    deviceSn: str
    deviceType: str
    deviceState: int
    collectionTime: int
    dataList: list[DataPoint]


class Response(BaseModel):
    code: str
    msg: str
    success: bool
    requestId: str
    deviceDataList: list[DeviceData]


class InverterData(BaseModel):
    device_sn: str
    device_type: str
    device_state: int
    collection_time: int
    rated_power: float
    dc_voltage_pv1: float
    dc_voltage_pv2: float = 0.0
    dc_voltage_pv3: float = 0.0
    dc_voltage_pv4: float = 0.0
    dc_voltage_pv5: float = 0.0
    dc_voltage_pv6: float = 0.0
    dc_voltage_pv7: float = 0.0
    dc_voltage_pv8: float = 0.0
    dc_current_pv1: float
    dc_current_pv2: float = 0.0
    dc_current_pv3: float = 0.0
    dc_current_pv4: float = 0.0
    dc_current_pv5: float = 0.0
    dc_current_pv6: float = 0.0
    dc_current_pv7: float = 0.0
    dc_current_pv8: float = 0.0
    dc_power_pv1: float
    dc_power_pv2: float = 0.0
    dc_power_pv3: float = 0.0
    dc_power_pv4: float = 0.0
    dc_power_pv5: float = 0.0
    dc_power_pv6: float = 0.0
    dc_power_pv7: float = 0.0
    dc_power_pv8: float = 0.0
    ac_voltage_rua: float
    ac_voltage_svb: float = 0.0
    ac_voltage_twc: float = 0.0
    ac_current_rua: float
    ac_current_svb: float = 0.0
    ac_current_twc: float = 0.0
    ac_output_frequency_r: float
    total_active_ac_output_power: float
    ab_line_voltage: float
    bc_line_voltage: float = 0.0
    ac_line_voltage: float = 0.0
    total_active_production: float
    daily_active_production: float
    inverter_output_power_l1: float = 0.0
    inverter_output_power_l2: float = 0.0
    inverter_output_power_l3: float = 0.0
    total_grid_feed_in: float = 0.0
    total_energy_purchased: float = 0.0
    total_consumption_power: float
    total_consumption: float = 0.0


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


def flattern_data(device: DeviceData):
    flat = {
        "device_sn": device.deviceSn,
        "device_type": device.deviceType,
        "device_state": device.deviceState,
        "collection_time": device.collectionTime,
    }
    for point in device.dataList:
        if point.key in key_map.keys():
            flat[key_map[point.key]] = float(point.value)

    return flat

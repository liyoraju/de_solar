from pydantic import BaseModel, field_validator, Field, AliasChoices
import logging

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

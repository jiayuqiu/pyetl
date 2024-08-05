"""
 @Author: qjy20472
 @Email: qjy20472@snat.com
 @FileName: unit.py
 @DateTime: 3/17/23 11:09 AM
 @SoftWare: PyCharm
 @Desc: 单位转换集合
"""

# import numpy as np
# import pandas as pd

SPEED_UNIT_LIST = ['km', 'm', 'cm']  # done
VOLTAGE_UNIT_LIST = ['v', 'mv']  # done
TIME_UNIT_LIST = ['s', 'min', 'h']  # done
AIR_FLOW_UNIT_LIST = ['kg/h', 'm³/h', 'm3/h']  # for etl v1.1
PRESSURE_UNIT_LIST = ['kpa', 'hpa', 'mpa']  # done
MASS_TIME_LIST = ['g/min', 'mg/s']  # TODO
VOLUME_TIME_LIST = ['m3/h', 'm^3/h']  # TODO
MASS_HUB_LIST = ['mg/hub', 'mg/cyl']  # TODO
VOLUME_HUB_LIST = ['mm3/st']  # TODO
ANGLE_LIST = ['degc', 'ca']


class BaseConvert(object):
    def __init__(self, from_unit, to_unit) -> None:
        self.from_unit = from_unit
        self.to_unit = to_unit


class VoltageConvertor(BaseConvert):
    def to_v(self, ):
        if (self.from_unit == 'mv'):
            return 1e-3
        elif self.from_unit == 'v':
            return 1
        else:
            return 0

    def get_precision(self, ):
        if self.to_unit == 'v':
            return self.to_v()
        else:
            return 0


class SpeedConvertor(BaseConvert):
    def to_km(self, ):
        if (self.from_unit == 'km') | (self.from_unit is None):
            return 1
        elif self.from_unit == 'm':
            return 1e-3
        elif self.from_unit == 'cm':
            return 1e-6
        else:
            return 0

    def to_m(self, ):
        if self.from_unit == 'km':
            return 1e3
        elif (self.from_unit == 'm') | (self.from_unit is None):
            return 1
        elif self.from_unit == 'cm':
            return 1e-3
        else:
            return 0

    def get_precision(self, ):
        if self.to_unit == 'km':
            return self.to_km()
        elif self.to_unit == 'm':
            return self.to_m()
        else:
            return 0


class TimeConvertor(BaseConvert):
    def to_h(self, ):
        if (self.from_unit == 'h') | (self.from_unit is None):
            return 1
        elif self.from_unit == 'min':
            return 1/60
        elif self.from_unit == 's':
            return 1/3600
        else:
            return 0
    
    def to_min(self, ):
        if (self.from_unit == 'min') | (self.from_unit is None):
            return 1
        elif self.from_unit == 'h':
            return 60
        elif self.from_unit == 's':
            return 1/60
        else:
            return 0
    
    def to_s(self, ):
        if (self.from_unit == 's') | (self.from_unit is None):
            return 1
        elif self.from_unit == 'min':
            return 60
        elif self.from_unit == 'h':
            return 3600
        else:
            return 0
    
    def get_precision(self, ):
        if self.to_unit == 'h':
            return self.to_h()
        elif self.to_unit == 'min':
            return self.to_min()
        elif self.to_unit == 's':
            return self.to_s()
        else:
            return 0


class AirFlowConvertor(BaseConvert):
    def to_kgh(self, ):
        """转换为kg/h"""
        if (self.from_unit == 'kg/h') | (self.from_unit is None):
            return 1
        elif (self.from_unit == 'm3/h') | (self.from_unit == 'm³/h'):
            # 通常情况下环境温度300k, 大气压强101325，V/M = 0.85566
            return 1/0.85566
        else:
            return 0
    
    def to_m3h(self, ):
        """转换为m³/h"""
        if (self.from_unit == 'm3/h') | (self.from_unit == 'm³/h') | (self.from_unit is None):
            return 1
        elif self.from_unit == 'kg/h':
            # 通常情况下环境温度300k, 大气压强101325，V/M = 0.85566
            return 0.85566
        else:
            return 0

    def get_precision(self, ):
        if self.to_unit == 'kg/h':
            return self.to_kgh()
        elif (self.to_unit == 'm3/h') | (self.to_unit == 'm³/h'):
            return self.to_m3h()
        else:
            return 0


class PressureConvertor(BaseConvert):
    def to_hpa(self, ):
        """
        转换为百帕
        :return:
        """
        if self.from_unit == 'hpa':
            return 1
        elif self.from_unit == 'kpa':
            return 10
        elif self.from_unit == 'mpa':
            return 10000
        else:
            return 0

    def to_kpa(self, ):
        """
        转换千帕
        :return:
        """
        if self.from_unit == 'hpa':
            return 0.1
        elif self.from_unit == 'kpa':
            return 1
        elif self.from_unit == 'mpa':
            return 1000
        else:
            return 0

    def to_mpa(self, ):
        """
        转换兆帕
        :return:
        """
        if self.from_unit == 'hpa':
            return 0.0001
        elif self.from_unit == 'kpa':
            return 0.001
        elif self.from_unit == 'mpa':
            return 1
        else:
            return 0

    def get_precision(self, ) -> float:
        """
        获取转换系数
        :return:
        """
        if self.to_unit == 'hpa':
            return self.to_hpa()
        elif self.to_unit == 'kpa':
            return self.to_kpa()
        elif self.to_unit == 'mpa':
            return self.to_mpa()
        else:
            return 0


class MassTimeConvertor(BaseConvert):
    def to_g_min(self, ):
        """
        转换为克/分钟
        :return:
        """
        if self.from_unit == 'mg/s':
            return 0.06
        elif self.from_unit == 'g/min':
            return 1
        else:
            return 0

    def to_mg_s(self, ):
        """
        转换为毫克/秒
        :return:
        """
        if self.from_unit == 'mg/s':
            return 1
        elif self.from_unit == 'g/min':
            return 16.67
        else:
            return 0

    def get_precision(self, ):
        if self.to_unit == 'g/min':
            return self.to_g_min()
        elif self.to_unit == 'mg/s':
            return self.to_mg_s()
        else:
            return 0


class VolumeTimeConvertor(BaseConvert):
    def to_m3_h(self, ):
        """
        转换为立方米/小时
        :return:
        """
        if self.from_unit in ['m3/h', 'm^3/h']:
            return 1
        else:
            return 0

    def get_precision(self, ):
        if self.to_unit in ['m3/h', 'm^3/h']:
            return self.to_m3_h()
        else:
            return 0


class MassHubConvertor(BaseConvert):
    def to_mg_hub(self, ):
        """
        转换为毫克/冲程
        :return:
        """
        if self.from_unit in ['mg/hub', 'mg/cyl']:
            return 1
        else:
            return 0

    def get_precision(self, ):
        if self.to_unit in ['mg/hub', 'mg/cyl']:
            return self.to_mg_hub()
        else:
            return 0


class VolumeHubConvertor(BaseConvert):
    def to_mm3_hub(self, ):
        """
        转换为立方毫米/冲程
        :return:
        """
        if self.from_unit in ['mm3/st']:
            return 1
        else:
            return 0

    def get_precision(self, ):
        if self.to_unit in ['mm3/st']:
            return self.to_mm3_hub()
        else:
            return 0


class AngleConvertor(BaseConvert):
    def to_degc(self, ):
        """
        转为度
        :return:
        """
        if self.from_unit in ['degc', 'ca']:
            return 1
        else:
            return 0

    def get_precision(self, ):
        if self.to_unit in ['degc', 'ca']:
            return self.to_degc()
        else:
            return 0


def convert_unit_precision(from_unit, to_unit):
    # print(f"from_unit -> {from_unit}, to_unit -> {to_unit}")
    from_unit = from_unit.lower()
    to_unit = to_unit.lower()
    if from_unit == to_unit:
        return 1
    
    if to_unit in SPEED_UNIT_LIST:
        # 速度单位
        Convertor = SpeedConvertor(from_unit, to_unit)
        return Convertor.get_precision()
    elif to_unit in VOLTAGE_UNIT_LIST:
        # 电压单位
        Convertor = VoltageConvertor(from_unit, to_unit)
        return Convertor.get_precision()
    elif to_unit in TIME_UNIT_LIST:
        # 时间单位
        Convertor = TimeConvertor(from_unit, to_unit)
        return Convertor.get_precision()
    elif to_unit in PRESSURE_UNIT_LIST:
        # 压强单位, pa
        Convertor = PressureConvertor(from_unit, to_unit)
        return Convertor.get_precision()
    elif to_unit in MASS_TIME_LIST:
        # 质量 时间 单位
        Convertor = MassTimeConvertor(from_unit, to_unit)
        return Convertor.get_precision()
    elif to_unit in VOLUME_TIME_LIST:
        # 体积 时间 单位
        Convertor = VolumeTimeConvertor(from_unit, to_unit)
        return Convertor.get_precision()
    elif to_unit in MASS_HUB_LIST:
        # 体积 时间 单位
        Convertor = MassHubConvertor(from_unit, to_unit)
        return Convertor.get_precision()
    elif to_unit in VOLUME_HUB_LIST:
        # 体积 时间 单位
        Convertor = VolumeHubConvertor(from_unit, to_unit)
        return Convertor.get_precision()
    elif to_unit in ANGLE_LIST:
        # 角度单位
        Convertor = AngleConvertor(from_unit, to_unit)
        return Convertor.get_precision()
    elif to_unit in AIR_FLOW_UNIT_LIST:
        # 空速，仅在v1.1版本中生效
        Convertor = AirFlowConvertor(from_unit, to_unit)
        return Convertor.get_precision()
    return 0

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
@author: macs
@contact: sohu.321@qq.com
@version: 1.0.0
@license: Apache Licence
@file: SchemaV2.py
@time: 2023/8/25 下午2:32
"""


from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import FloatType, DoubleType, LongType, DoubleType
from pyspark.sql.types import TimestampType, DateType


ETL_SCHEMA_TEST = StructType([
    StructField("ein", StringType(), True),
    StructField("vin", StringType(), True),
    StructField("data_type", StringType(), True),
    StructField("data_source", StringType(), True),
    StructField("hos_series", StringType(), True),
    StructField("hos_mat_id", StringType(), True),
    StructField("lng", FloatType(), True),
    StructField("lat", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("clt_timestamp", StringType(), True),
    StructField("clt_time", StringType(), True),
    StructField("clt_date", StringType(), True),
    StructField('fuel_injection_mass_hub', DoubleType(), True),  # 106
])


ETL_SCHEMA = StructType([
    StructField("ein", StringType(), True),
    StructField("vin", StringType(), True),
    StructField("data_type", StringType(), True),
    StructField("data_source", StringType(), True),
    StructField("hos_series", StringType(), True),
    StructField("hos_mat_id", StringType(), True),
    StructField("lng", FloatType(), True),
    StructField("lat", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("clt_timestamp", LongType(), True),
    StructField("clt_time", StringType(), True),
    StructField("clt_date", StringType(), True),
    StructField('Driving_regeneration', DoubleType(), True),  # 1
    StructField('InjCrv_qPoI3Set', DoubleType(), True),  # 2
    StructField('PFltRgn_lSnceRgn', DoubleType(), True),  # 3
    StructField('Parking_regeneration', DoubleType(), True),  # 4
    StructField('XDRP_1_voltage', DoubleType(), True),  # 5
    StructField('XDRP_2_voltage', DoubleType(), True),  # 6
    StructField('ac_switch', DoubleType(), True),  # 7
    StructField('accpp', DoubleType(), True),  # 8
    StructField('actual_engine_advance_angle_1', DoubleType(), True),  # 9
    StructField('actual_engine_tq_per', DoubleType(), True),  # 10
    StructField('after_exhaust_dew_point', DoubleType(), True),  # 11
    StructField('after_intake_dew_point', DoubleType(), True),  # 12
    StructField('after_oxygen_air_fuel_ratio', DoubleType(), True),  # 13
    StructField('after_oxygen_voltage', DoubleType(), True),  # 14
    StructField('air_intake_per_cylinder_mass_hub', DoubleType(), True),  # 15
    StructField('air_leakage_ratio', DoubleType(), True),  # 16
    StructField('atmospheric_pressure', DoubleType(), True),  # 17
    StructField('atmospheric_temperature', DoubleType(), True),  # 18
    StructField('auto_regeneration_complete_counter', DoubleType(), True),  # 19
    StructField('auto_regeneration_fail_counter', DoubleType(), True),  # 20
    StructField('average_engine_fuel_economy', DoubleType(), True),  # 21
    StructField('average_gas_consumption', DoubleType(), True),  # 22
    StructField('battery_alarm', DoubleType(), True),  # 23
    StructField('battery_voltage', DoubleType(), True),  # 24
    StructField('brake_light_control', DoubleType(), True),  # 25
    StructField('brake_request', DoubleType(), True),  # 26
    StructField('brake_switch', DoubleType(), True),  # 27
    StructField('catalyst_concentration', DoubleType(), True),  # 28
    StructField('catalyst_flow_rate_mass_time', DoubleType(), True),  # 29
    StructField('catalyst_injection_duty_cycle', DoubleType(), True),  # 30
    StructField('catalyst_injection_status', DoubleType(), True),  # 31
    StructField('catalyst_level', DoubleType(), True),  # 32
    StructField('catalyst_pump_duty_cycle', DoubleType(), True),  # 33
    StructField('catalyst_pump_pressure', DoubleType(), True),  # 34
    StructField('catalyst_pump_temperature', DoubleType(), True),  # 35
    StructField('catalyst_tank_temperature', DoubleType(), True),  # 36
    StructField('catalyst_temperature', DoubleType(), True),  # 37
    StructField('ccvp_voltage', DoubleType(), True),  # 38
    StructField('circulation_intake', DoubleType(), True),  # 39
    StructField('closed_loop_coefficient', DoubleType(), True),  # 40
    StructField('clutch_switch', DoubleType(), True),  # 41
    StructField('coolant_temperature', DoubleType(), True),  # 42
    StructField('crankcase_pressure_2', DoubleType(), True),  # 43
    StructField('crankshaft_signal_status', DoubleType(), True),  # 44
    StructField('cruise_control_status', DoubleType(), True),  # 45
    StructField('cylinder_1_knock_factor', DoubleType(), True),  # 46
    StructField('cylinder_2_knock_factor', DoubleType(), True),  # 47
    StructField('cylinder_3_knock_factor', DoubleType(), True),  # 48
    StructField('cylinder_4_knock_factor', DoubleType(), True),  # 49
    StructField('cylinder_5_knock_factor', DoubleType(), True),  # 50
    StructField('cylinder_6_knock_factor', DoubleType(), True),  # 51
    StructField('cylinder_brake_request_switch', DoubleType(), True),  # 52
    StructField('cylinder_ignition_timing', DoubleType(), True),  # 53
    StructField('demand_throttle_opening', DoubleType(), True),  # 54
    StructField('desired_phi', DoubleType(), True),  # 55
    StructField('door_lock_switch', DoubleType(), True),  # 56
    StructField('down_nox_concentration', DoubleType(), True),  # 57
    StructField('down_stream_air_cooler_temperature', DoubleType(), True),  # 58
    StructField('dpf_flow', DoubleType(), True),  # 59
    StructField('dpf_fuel', DoubleType(), True),  # 60
    StructField('dpf_pressure_diff', DoubleType(), True),  # 61
    StructField('dpf_regeneration_disable_indicator', DoubleType(), True),  # 62
    StructField('dpf_soot_mass', DoubleType(), True),  # 63
    StructField('dpf_state', DoubleType(), True),  # 64
    StructField('dpf_switch', DoubleType(), True),  # 65
    StructField('driver_alarm_system_indicates_status', DoubleType(), True),  # 66
    StructField('driver_alarm_system_severity_rating', DoubleType(), True),  # 67
    StructField('driver_warning_light_status', DoubleType(), True),  # 68
    StructField('driving_voltage', DoubleType(), True),  # 69
    StructField('egr_air_mass_per_cylinder', DoubleType(), True),  # 70
    StructField('egr_cooler_temperature', DoubleType(), True),  # 71
    StructField('egr_flow_mass_time', DoubleType(), True),  # 72
    StructField('egr_pressure', DoubleType(), True),  # 73
    StructField('egr_rate', DoubleType(), True),  # 74
    StructField('egr_valve_actual_position', DoubleType(), True),  # 75
    StructField('egr_valve_duty_ratio', DoubleType(), True),  # 76
    StructField('egr_valve_position', DoubleType(), True),  # 77
    StructField('egr_valve_position_control_setting', DoubleType(), True),  # 78
    StructField('egr_valve_target_position', DoubleType(), True),  # 79
    StructField('egr_vlv_actuator_position', DoubleType(), True),  # 80
    StructField('egrv_actual_value', DoubleType(), True),  # 81
    StructField('egrv_command_value', DoubleType(), True),  # 82
    StructField('engine_air_filter_restriction_alarm', DoubleType(), True),  # 83
    StructField('engine_crankcase_pressure', DoubleType(), True),  # 84
    StructField('engine_exhaust_air_flow_mass_time', DoubleType(), True),  # 85
    StructField('engine_exhaust_air_flow_volumn_time', DoubleType(), True),  # 86
    StructField('engine_fuel_shutdown_control', DoubleType(), True),  # 87
    StructField('engine_intake_air_mass_hub', DoubleType(), True),  # 88
    StructField('engine_intake_air_mass_time', DoubleType(), True),  # 89
    StructField('engine_intake_air_temperature', DoubleType(), True),  # 90
    StructField('engine_oil_pressure', DoubleType(), True),  # 91
    StructField('engine_operation_mode', DoubleType(), True),  # 92
    StructField('engine_run_time_since_last_successful_regeneration', DoubleType(), True),  # 93
    StructField('engine_running_time_since_last_regeneration', DoubleType(), True),  # 94
    StructField('engine_speed', DoubleType(), True),  # 95
    StructField('engine_state', DoubleType(), True),  # 96
    StructField('engine_torque_mode', DoubleType(), True),  # 97
    StructField('ewma_three_way_catalytic_oxygen_storage', DoubleType(), True),  # 98
    StructField('exhaust_brake_switch', DoubleType(), True),  # 99
    StructField('fan_speed', DoubleType(), True),  # 100
    StructField('fire1_fire_rate', DoubleType(), True),  # 101
    StructField('fire2_fire_rate', DoubleType(), True),  # 102
    StructField('fire3_fire_rate', DoubleType(), True),  # 103
    StructField('fire4_fire_rate', DoubleType(), True),  # 104
    StructField('fire5_fire_rate', DoubleType(), True),  # 105
    StructField('fire6_fire_rate', DoubleType(), True),  # 106
    StructField('forbid_regeneration_switch', DoubleType(), True),  # 107
    StructField('friction_tq_per', DoubleType(), True),  # 108
    StructField('fuel_injection_advance_angle', DoubleType(), True),  # 109
    StructField('fuel_injection_mass_hub', DoubleType(), True),  # 110
    StructField('fuel_injection_volume_hub', DoubleType(), True),  # 111
    StructField('fuel_level', DoubleType(), True),  # 112
    StructField('fuel_pressure', DoubleType(), True),  # 113
    StructField('fuel_rate_volume_time', DoubleType(), True),  # 114
    StructField('fuel_temperature', DoubleType(), True),  # 115
    StructField('gas_rate_mass_time', DoubleType(), True),  # 116
    StructField('gear', DoubleType(), True),  # 117
    StructField('gear_signal_switch', DoubleType(), True),  # 118
    StructField('general_driver_alarm_dcu', DoubleType(), True),  # 119
    StructField('goc_exhaust_temperature', DoubleType(), True),  # 120
    StructField('goc_intake_temperature', DoubleType(), True),  # 121
    StructField('handbrake_switch', DoubleType(), True),  # 122
    StructField('hci_fuel_injection_quantity_mass_time', DoubleType(), True),  # 123
    StructField('hci_fuel_injection_quantity_volume_hub', DoubleType(), True),  # 124
    StructField('highest_value_of_postocegt', DoubleType(), True),  # 125
    StructField('idle_switch', DoubleType(), True),  # 126
    StructField('instantaneous_gas_consumption', DoubleType(), True),  # 127
    StructField('intake_manifold_pressure', DoubleType(), True),  # 128
    StructField('intake_manifold_temperature', DoubleType(), True),  # 129
    StructField('key_maximum_voltage', DoubleType(), True),  # 130
    StructField('key_voltage', DoubleType(), True),  # 131
    StructField('load_percentage', DoubleType(), True),  # 132
    StructField('low_dpf_entrance_temperature_caused_parking_regeneration_failure_counter', DoubleType(), True),  # 133
    StructField('low_dpf_inlet_temperature_causes_regeneration_failure', DoubleType(), True),  # 134
    StructField('manual_regeneration_switch', DoubleType(), True),  # 135
    StructField('mat_maximum', DoubleType(), True),  # 136
    StructField('maximum_drvp_voltage', DoubleType(), True),  # 137
    StructField('maximum_ect_value', DoubleType(), True),  # 138
    StructField('mil_indicator_status', DoubleType(), True),  # 139
    StructField('mil_status_dcu', DoubleType(), True),  # 140
    StructField('ne_active', DoubleType(), True),  # 141
    StructField('neutral_switch', DoubleType(), True),  # 142
    StructField('no_regeneration_state', DoubleType(), True),  # 143
    StructField('oil_temperature', DoubleType(), True),  # 144
    StructField('oil_water_separator', DoubleType(), True),  # 145
    StructField('oil_water_separator_switch', DoubleType(), True),  # 146
    StructField('oxygen_sensor_temperature', DoubleType(), True),  # 147
    StructField('parking_regen_complete_flag', DoubleType(), True),  # 148
    StructField('parking_signal_switch', DoubleType(), True),  # 149
    StructField('part_regeneration_success_counter', DoubleType(), True),  # 150
    StructField('percentage_of_retarder_torque_required_by_driver', DoubleType(), True),  # 151
    StructField('percentage_of_torque_required_by_driver', DoubleType(), True),  # 152
    StructField('pfav_duty_cycle', DoubleType(), True),  # 153
    StructField('pfav_location', DoubleType(), True),  # 154
    StructField('pfav_set_value', DoubleType(), True),  # 155
    StructField('power_on_switch', DoubleType(), True),  # 156
    StructField('pto', DoubleType(), True),  # 157
    StructField('pto_accpp', DoubleType(), True),  # 158
    StructField('pto_control_status', DoubleType(), True),  # 159
    StructField('pto_switch', DoubleType(), True),  # 160
    StructField('rail_pressure', DoubleType(), True),  # 161
    StructField('rail_pressure_setpoint', DoubleType(), True),  # 162
    StructField('real_time_engine_fuel_economy', DoubleType(), True),  # 163
    StructField('reference_engine_tq', DoubleType(), True),  # 164
    StructField('regeneration_request_counter', DoubleType(), True),  # 165
    StructField('regeneration_success_counter', DoubleType(), True),  # 166
    StructField('release_valve_opening', DoubleType(), True),  # 167
    StructField('retarder_control_switch', DoubleType(), True),  # 168
    StructField('retarder_input', DoubleType(), True),  # 169
    StructField('rpm_maximum', DoubleType(), True),  # 170
    StructField('scr_downstream_exhaust_temperature', DoubleType(), True),  # 171
    StructField('scr_front_nox', DoubleType(), True),  # 172
    StructField('scr_main_status', DoubleType(), True),  # 173
    StructField('scr_sub_status', DoubleType(), True),  # 174
    StructField('scr_upstream_exhaust_temperature', DoubleType(), True),  # 175
    StructField('self_learning_coefficient', DoubleType(), True),  # 176
    StructField('serious_driver_alarm_dcu', DoubleType(), True),  # 177
    StructField('service_regen_abort_counter', DoubleType(), True),  # 178
    StructField('service_regen_complete_flag', DoubleType(), True),  # 179
    StructField('service_regen_fail_flag', DoubleType(), True),  # 180
    StructField('service_regen_request_counter', DoubleType(), True),  # 181
    StructField('service_regen_success_counter', DoubleType(), True),  # 182
    StructField('since_the_last_successful_regeneration', DoubleType(), True),  # 183
    StructField('smart_spark_plug_replace_request', DoubleType(), True),  # 184
    StructField('smart_spark_plug_replace_success', DoubleType(), True),  # 185
    StructField('spark_plug_gap', DoubleType(), True),  # 186
    StructField('speed', DoubleType(), True),  # 187
    StructField('speed_limit_countdown', DoubleType(), True),  # 188
    StructField('start_battery_level_low', DoubleType(), True),  # 189
    StructField('start_battery_voltage_low', DoubleType(), True),  # 190
    StructField('start_rail_pressure_reponse_fault', DoubleType(), True),  # 191
    StructField('start_switch', DoubleType(), True),  # 192
    StructField('start_swtich', DoubleType(), True),  # 193
    StructField('start_time', DoubleType(), True),  # 194
    StructField('starter_fault', DoubleType(), True),  # 195
    StructField('status_service_regeneration_activate', DoubleType(), True),  # 196
    StructField('svs_indicator_status', DoubleType(), True),  # 197
    StructField('t4_temperature', DoubleType(), True),  # 198
    StructField('t5_temperature', DoubleType(), True),  # 199
    StructField('t6_temperature', DoubleType(), True),  # 200
    StructField('tachographvehicle_speed', DoubleType(), True),  # 201
    StructField('target_boost_pressure', DoubleType(), True),  # 202
    StructField('target_egr_rate', DoubleType(), True),  # 203
    StructField('target_intake_air_flow_mass_time', DoubleType(), True),  # 204
    StructField('target_rail_pressure', DoubleType(), True),  # 205
    StructField('the_highest_egt_value', DoubleType(), True),  # 206
    StructField('three_way_catalytic_oxygen_storage', DoubleType(), True),  # 207
    StructField('throat_pressure', DoubleType(), True),  # 208
    StructField('throttle_actuator_control_instruction', DoubleType(), True),  # 209
    StructField('throttle_actuator_position', DoubleType(), True),  # 210
    StructField('throttle_duty_ratio', DoubleType(), True),  # 211
    StructField('throttle_pedal_low_idle_switch', DoubleType(), True),  # 212
    StructField('throttle_position', DoubleType(), True),  # 213
    StructField('throttle_position_control_set_value', DoubleType(), True),  # 214
    StructField('throttle_position_set', DoubleType(), True),  # 215
    StructField('total_catalyst_consumption', DoubleType(), True),  # 216
    StructField('total_distance', DoubleType(), True),  # 217
    StructField('total_engine_round', DoubleType(), True),  # 218
    StructField('total_fuel', DoubleType(), True),  # 219
    StructField('total_gas_consumption', DoubleType(), True),  # 220
    StructField('total_run_time', DoubleType(), True),  # 221
    StructField('total_start_counter', DoubleType(), True),  # 222
    StructField('total_start_failure_counter', DoubleType(), True),  # 223
    StructField('tq_limit_countdown', DoubleType(), True),  # 224
    StructField('trip_catalyst_consumption', DoubleType(), True),  # 225
    StructField('trip_fuel_consumption', DoubleType(), True),  # 226
    StructField('trip_mileage', DoubleType(), True),  # 227
    StructField('up_nox_concentration', DoubleType(), True),  # 228
    StructField('prepare_data', DoubleType(), True), # prepare_data
    StructField('fusion_data', DoubleType(), True),
    StructField('add_static_data', DoubleType(), True)
])

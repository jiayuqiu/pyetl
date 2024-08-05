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
from pyspark.sql.types import FloatType, IntegerType, DecimalType, LongType
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
    StructField('fuel_injection_mass_hub', DecimalType(12, 7), True),  # 106
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
    StructField('Driving_regeneration', StringType(), True),  # 1
    StructField('InjCrv_qPoI3Set', DecimalType(20, 7), True),  # 2
    StructField('PFltRgn_lSnceRgn', DecimalType(20, 7), True),  # 3
    StructField('Parking_regeneration', StringType(), True),  # 4
    StructField('XDRP_1_voltage', DecimalType(20, 7), True),  # 5
    StructField('XDRP_2_voltage', DecimalType(20, 7), True),  # 6
    StructField('ac_switch', StringType(), True),  # 7
    StructField('accpp', DecimalType(20, 7), True),  # 8
    StructField('actual_engine_advance_angle_1', DecimalType(20, 7), True),  # 9
    StructField('actual_engine_tq_per', DecimalType(20, 7), True),  # 10
    StructField('after_exhaust_dew_point', StringType(), True),  # 11
    StructField('after_intake_dew_point', StringType(), True),  # 12
    StructField('after_oxygen_air_fuel_ratio', StringType(), True),  # 13
    StructField('after_oxygen_voltage', DecimalType(20, 7), True),  # 14
    StructField('air_intake_per_cylinder_mass_hub', DecimalType(20, 7), True),  # 15
    StructField('ash_accumulation_volume', DecimalType(20, 7), True),  # 16
    StructField('atmospheric_pressure', DecimalType(20, 7), True),  # 17
    StructField('atmospheric_temperature', DecimalType(20, 7), True),  # 18
    StructField('auto_regeneration_complete_counter', StringType(), True),  # 19
    StructField('auto_regeneration_fail_counter', StringType(), True),  # 20
    StructField('average_engine_fuel_economy', DecimalType(20, 7), True),  # 21
    StructField('average_gas_consumption', DecimalType(20, 7), True),  # 22
    StructField('battery_voltage', DecimalType(20, 7), True),  # 23
    StructField('booster_actuator_duty_cycle', DecimalType(20, 7), True),  # 24
    StructField('brake_light_control', StringType(), True),  # 25
    StructField('brake_light_switch', StringType(), True),  # 26
    StructField('brake_request', StringType(), True),  # 27
    StructField('brake_switch', StringType(), True),  # 28
    StructField('carbon_load_correction_factor_during_regeneration', StringType(), True),  # 29
    StructField('carbon_smoke_simulation_calculated_mass', DecimalType(20, 7), True),  # 30
    StructField('catalyst_concentration', DecimalType(20, 7), True),  # 31
    StructField('catalyst_flow_rate_mass_time', DecimalType(20, 7), True),  # 32
    StructField('catalyst_injection_duty_cycle', DecimalType(20, 7), True),  # 33
    StructField('catalyst_injection_status', StringType(), True),  # 34
    StructField('catalyst_level', DecimalType(20, 7), True),  # 35
    StructField('catalyst_pump_duty_cycle', DecimalType(20, 7), True),  # 36
    StructField('catalyst_pump_pressure', DecimalType(20, 7), True),  # 37
    StructField('catalyst_tank_temperature', DecimalType(20, 7), True),  # 38
    StructField('catalyst_temperature', DecimalType(20, 7), True),  # 39
    StructField('ccvp_voltage', DecimalType(20, 7), True),  # 40
    StructField('closed_loop_coefficient', StringType(), True),  # 41
    StructField('clutch_switch', StringType(), True),  # 42
    StructField('continuous_dpf_carbon_load_model_values', DecimalType(20, 7), True),  # 43
    StructField('coolant_temperature', DecimalType(20, 7), True),  # 44
    StructField('crankshaft_signal_status', StringType(), True),  # 45
    StructField('cruise_control_status', StringType(), True),  # 46
    StructField('cruise_master_switch', StringType(), True),  # 47
    StructField('cruise_switch_1', StringType(), True),  # 48
    StructField('cruise_switch_2', StringType(), True),  # 49
    StructField('cruise_switch_3', StringType(), True),  # 50
    StructField('cylinder_1_knock_factor', StringType(), True),  # 51
    StructField('cylinder_2_knock_factor', StringType(), True),  # 52
    StructField('cylinder_3_knock_factor', StringType(), True),  # 53
    StructField('cylinder_4_knock_factor', StringType(), True),  # 54
    StructField('cylinder_5_knock_factor', StringType(), True),  # 55
    StructField('cylinder_6_knock_factor', StringType(), True),  # 56
    StructField('cylinder_brake_request_switch', StringType(), True),  # 57
    StructField('cylinder_ignition_timing', DecimalType(20, 7), True),  # 58
    StructField('dcu_fault', StringType(), True),  # 59
    StructField('demand_engine_operation_mode', StringType(), True),  # 60
    StructField('demand_throttle_opening', DecimalType(20, 7), True),  # 61
    StructField('desired_phi', StringType(), True),  # 62
    StructField('diagnostic_switch', StringType(), True),  # 63
    StructField('discontinuous_dpf_carbon_load_simulation_value', DecimalType(20, 7), True),  # 64
    StructField('doc_downstream_required_temperature', DecimalType(20, 7), True),  # 65
    StructField('doc_upstream_required_temperature', DecimalType(20, 7), True),  # 66
    StructField('door_lock_switch', StringType(), True),  # 67
    StructField('down_nox_concentration', DecimalType(20, 7), True),  # 68
    StructField('down_stream_air_cooler_temperature', DecimalType(20, 7), True),  # 69
    StructField('dpf_downstream_temperature_sensor_original_voltage', DecimalType(20, 7), True),  # 70
    StructField('dpf_flow', StringType(), True),  # 71
    StructField('dpf_fuel', DecimalType(20, 7), True),  # 72
    StructField('dpf_inlet_temperature', DecimalType(20, 7), True),  # 73
    StructField('dpf_life_cycle_fuel_consumption', DecimalType(20, 7), True),  # 74
    StructField('dpf_partially_regenerated_successfully', StringType(), True),  # 75
    StructField('dpf_pressure_diff', DecimalType(20, 7), True),  # 76
    StructField('dpf_regeneration_disable_indicator', StringType(), True),  # 77
    StructField('dpf_soot_mass', DecimalType(20, 7), True),  # 78
    StructField('dpf_state', StringType(), True),  # 79
    StructField('dpf_switch', StringType(), True),  # 80
    StructField('driver_alarm_system_indicates_status', StringType(), True),  # 81
    StructField('driver_alarm_system_severity_rating', StringType(), True),  # 82
    StructField('driver_warning_light_status', StringType(), True),  # 83
    StructField('driving_voltage', DecimalType(20, 7), True),  # 84
    StructField('egr_air_mass_per_cylinder', DecimalType(20, 7), True),  # 85
    StructField('egr_cooler_downstream_temperature_sensor_voltage', DecimalType(20, 7), True),  # 86
    StructField('egr_cooler_temperature', DecimalType(20, 7), True),  # 87
    StructField('egr_flow_mass_time', DecimalType(20, 7), True),  # 88
    StructField('egr_pressure', DecimalType(20, 7), True),  # 89
    StructField('egr_rate', DecimalType(20, 7), True),  # 90
    StructField('egr_valve_actual_position', DecimalType(20, 7), True),  # 91
    StructField('egr_valve_duty_ratio', DecimalType(20, 7), True),  # 92
    StructField('egr_valve_position', DecimalType(20, 7), True),  # 93
    StructField('egr_valve_position_control_deviation', DecimalType(20, 7), True),  # 94
    StructField('egr_valve_position_control_setting', DecimalType(20, 7), True),  # 95
    StructField('egr_valve_position_set_value', DecimalType(20, 7), True),  # 96
    StructField('egr_vlv_actuator_position', DecimalType(20, 7), True),  # 97
    StructField('egrv_actual_value', DecimalType(20, 7), True),  # 98
    StructField('egrv_command_value', DecimalType(20, 7), True),  # 99
    StructField('engine_air_filter_restriction_alarm', StringType(), True),  # 100
    StructField('engine_crankcase_pressure', DecimalType(20, 7), True),  # 101
    StructField('engine_exhaust_air_flow_mass_time', DecimalType(20, 7), True),  # 102
    StructField('engine_exhaust_air_flow_volumn_time', DecimalType(20, 7), True),  # 103
    StructField('engine_fuel_shutdown_control', StringType(), True),  # 104
    StructField('engine_intake_air_flow_offset', DecimalType(20, 7), True),  # 105
    StructField('engine_intake_air_mass_hub', DecimalType(20, 7), True),  # 106
    StructField('engine_intake_air_mass_time', DecimalType(20, 7), True),  # 107
    StructField('engine_intake_air_pressure', DecimalType(20, 7), True),  # 108
    StructField('engine_intake_air_temperature', DecimalType(20, 7), True),  # 109
    StructField('engine_oil_pressure', DecimalType(20, 7), True),  # 110
    StructField('engine_run_time_since_last_successful_regeneration', DecimalType(20, 7), True),  # 111
    StructField('engine_running_time_since_last_regeneration', DecimalType(20, 7), True),  # 112
    StructField('engine_speed', DecimalType(20, 7), True),  # 113
    StructField('engine_speed_status', StringType(), True),  # 114
    StructField('engine_state', StringType(), True),  # 115
    StructField('engine_torque_mode', StringType(), True),  # 116
    StructField('ewma_three_way_catalytic_oxygen_storage', DecimalType(20, 7), True),  # 117
    StructField('exhaust_back_pressure', DecimalType(20, 7), True),  # 118
    StructField('exhaust_brake_relay', StringType(), True),  # 119
    StructField('exhaust_brake_switch', StringType(), True),  # 120
    StructField('fan_speed', DecimalType(20, 7), True),  # 121
    StructField('fault_code_bosch_c81', StringType(), True),  # 122
    StructField('fault_code_bosch_md1', StringType(), True),  # 123
    StructField('fault_code_denso_dc1w', StringType(), True),  # 124
    StructField('fault_code_easy_control_f17', StringType(), True),  # 125
    StructField('fault_code_secm112', StringType(), True),  # 126
    StructField('fire1_fire_rate', DecimalType(20, 7), True),  # 127
    StructField('fire2_fire_rate', DecimalType(20, 7), True),  # 128
    StructField('fire3_fire_rate', DecimalType(20, 7), True),  # 129
    StructField('fire4_fire_rate', DecimalType(20, 7), True),  # 130
    StructField('fire5_fire_rate', DecimalType(20, 7), True),  # 131
    StructField('fire6_fire_rate', DecimalType(20, 7), True),  # 132
    StructField('friction_tq_per', DecimalType(20, 7), True),  # 133
    StructField('fuel_injection_advance_angle', DecimalType(20, 7), True),  # 134
    StructField('fuel_injection_mass_hub', DecimalType(20, 7), True),  # 135
    StructField('fuel_injection_pressure', DecimalType(20, 7), True),  # 136
    StructField('fuel_injection_set_value', DecimalType(20, 7), True),  # 137
    StructField('fuel_injection_volume_hub', DecimalType(20, 7), True),  # 138
    StructField('fuel_level', DecimalType(20, 7), True),  # 139
    StructField('fuel_pressure', DecimalType(20, 7), True),  # 140
    StructField('fuel_rate_volume_time', DecimalType(20, 7), True),  # 141
    StructField('fuel_temperature', DecimalType(20, 7), True),  # 142
    StructField('g_activate', StringType(), True),  # 143
    StructField('gas_rate_mass_time', DecimalType(20, 7), True),  # 144
    StructField('gear', StringType(), True),  # 145
    StructField('gear_signal_switch', StringType(), True),  # 146
    StructField('general_driver_alarm_dcu', StringType(), True),  # 147
    StructField('general_driver_alarm_ecu', StringType(), True),  # 148
    StructField('goc_exhaust_temperature', DecimalType(20, 7), True),  # 149
    StructField('goc_intake_temperature', DecimalType(20, 7), True),  # 150
    StructField('handbrake_switch', StringType(), True),  # 151
    StructField('hci_fuel_injection_quantity_mass_time', DecimalType(20, 7), True),  # 152
    StructField('hci_fuel_injection_quantity_volume_hub', DecimalType(20, 7), True),  # 153
    StructField('highest_value_of_postocegt', DecimalType(20, 7), True),  # 154
    StructField('idle_switch', StringType(), True),  # 155
    StructField('instantaneous_gas_consumption', DecimalType(20, 7), True),  # 156
    StructField('intake_manifold_pressure', DecimalType(20, 7), True),  # 157
    StructField('intake_manifold_temperature', DecimalType(20, 7), True),  # 158
    StructField('key_maximum_voltage', DecimalType(20, 7), True),  # 159
    StructField('key_voltage', DecimalType(20, 7), True),  # 160
    StructField('load_percentage', DecimalType(20, 7), True),  # 161
    StructField('low_dpf_entrance_temperature_caused_parking_regeneration_failure_counter', StringType(), True),  # 162
    StructField('low_dpf_inlet_temperature_causes_regeneration_failure', StringType(), True),  # 163
    StructField('main_relay_condition', StringType(), True),  # 164
    StructField('mat_maximum', DecimalType(20, 7), True),  # 165
    StructField('maximum_drvp_voltage', DecimalType(20, 7), True),  # 166
    StructField('maximum_ect_value', DecimalType(20, 7), True),  # 167
    StructField('mil_indicator_status', StringType(), True),  # 168
    StructField('mil_status_dcu', StringType(), True),  # 169
    StructField('ne_active', StringType(), True),  # 170
    StructField('neutral_switch', StringType(), True),  # 171
    StructField('no_regeneration_state', StringType(), True),  # 172
    StructField('obd_indicator_status', StringType(), True),  # 173
    StructField('oil_pump_learning_end_sign', StringType(), True),  # 174
    StructField('oil_pump_learning_end_value', DecimalType(20, 7), True),  # 175
    StructField('oil_temperature', DecimalType(20, 7), True),  # 176
    StructField('oil_water_separator_switch', StringType(), True),  # 177
    StructField('oxygen_sensor_temperature', DecimalType(20, 7), True),  # 178
    StructField('parking_regeneration_end_sign', StringType(), True),  # 179
    StructField('parking_signal_switch', StringType(), True),  # 180
    StructField('part_regeneration_success_counter', StringType(), True),  # 181
    StructField('particle_filter_pressure_difference_sensor_voltage', DecimalType(20, 7), True),  # 182
    StructField('percentage_of_retarder_torque_required_by_driver', DecimalType(20, 7), True),  # 183
    StructField('percentage_of_torque_required_by_driver', DecimalType(20, 7), True),  # 184
    StructField('pfav_duty_cycle', DecimalType(20, 7), True),  # 185
    StructField('pfav_location', DecimalType(20, 7), True),  # 186
    StructField('pfav_set_value', DecimalType(20, 7), True),  # 187
    StructField('pot_control_status', StringType(), True),  # 188
    StructField('power_on_switch', StringType(), True),  # 189
    StructField('preheating_relay', StringType(), True),  # 190
    StructField('pto', StringType(), True),  # 191
    StructField('pto_accpp', DecimalType(20, 7), True),  # 192
    StructField('pump_drive_duty_ratio', DecimalType(20, 7), True),  # 193
    StructField('rail_pressure', DecimalType(20, 7), True),  # 194
    StructField('rail_pressure_deviation', DecimalType(20, 7), True),  # 195
    StructField('rail_pressure_setpoint', DecimalType(20, 7), True),  # 196
    StructField('real_time_engine_fuel_economy', DecimalType(20, 7), True),  # 197
    StructField('reference_engine_tq', DecimalType(20, 7), True),  # 198
    StructField('regeneration_request_based_on_carbon_burden', StringType(), True),  # 199
    StructField('regeneration_request_counter', StringType(), True),  # 200
    StructField('regeneration_success_counter', StringType(), True),  # 201
    StructField('release_valve_opening', DecimalType(20, 7), True),  # 202
    StructField('retarder_control_switch', StringType(), True),  # 203
    StructField('retarder_input', StringType(), True),  # 204
    StructField('rpm_maximum', DecimalType(20, 7), True),  # 205
    StructField('scr_downstream_exhaust_temperature', DecimalType(20, 7), True),  # 206
    StructField('scr_front_nox', DecimalType(20, 7), True),  # 207
    StructField('scr_main_status', StringType(), True),  # 208
    StructField('scr_sub_status', StringType(), True),  # 209
    StructField('scr_upstream_exhaust_temperature', DecimalType(20, 7), True),  # 210
    StructField('scv_drive_current_value', DecimalType(20, 7), True),  # 211
    StructField('self_learning_coefficient', StringType(), True),  # 212
    StructField('self_learning_correction_value_of_fuel_injection', DecimalType(20, 7), True),  # 213
    StructField('serious_driver_alarm_dcu', StringType(), True),  # 214
    StructField('serious_driver_alarm_ecu', StringType(), True),  # 215
    StructField('service_regen_abort_counter', StringType(), True),  # 216
    StructField('service_regen_complete_flag', StringType(), True),  # 217
    StructField('service_regen_fail_flag', StringType(), True),  # 218
    StructField('service_regen_request_counter', StringType(), True),  # 219
    StructField('service_regen_success_counter', StringType(), True),  # 220
    StructField('service_regenerates_state_machine_status', StringType(), True),  # 221
    StructField('service_regeneration_identification_number', StringType(), True),  # 222
    StructField('since_the_last_successful_regeneration', DecimalType(20, 7), True),  # 223
    StructField('speed', DecimalType(20, 7), True),  # 224
    StructField('start_swtich', StringType(), True),  # 225
    StructField('status_service_regeneration_activate', StringType(), True),  # 226
    StructField('stop_switch', StringType(), True),  # 227
    StructField('supercharger_actuator_position', DecimalType(20, 7), True),  # 228
    StructField('svs_indicator_status', StringType(), True),  # 229
    StructField('system_light_status', StringType(), True),  # 230
    StructField('t4_temperature', DecimalType(20, 7), True),  # 231
    StructField('t5_temperature', DecimalType(20, 7), True),  # 232
    StructField('t6_temperature', DecimalType(20, 7), True),  # 233
    StructField('tachographvehicle_speed', DecimalType(20, 7), True),  # 234
    StructField('target_boost_pressure', DecimalType(20, 7), True),  # 235
    StructField('target_egr_rate', DecimalType(20, 7), True),  # 236
    StructField('target_intake_air_flow_mass_time', DecimalType(20, 7), True),  # 237
    StructField('target_throttle_opening', DecimalType(20, 7), True),  # 238
    StructField('the_highest_egt_value', DecimalType(20, 7), True),  # 239
    StructField('three_way_catalytic_oxygen_storage', DecimalType(20, 7), True),  # 240
    StructField('throat_pressure', DecimalType(20, 7), True),  # 241
    StructField('throttle_actuator_control_instruction', DecimalType(20, 7), True),  # 242
    StructField('throttle_actuator_position', DecimalType(20, 7), True),  # 243
    StructField('throttle_duty_ratio', DecimalType(20, 7), True),  # 244
    StructField('throttle_pedal_low_idle_switch', StringType(), True),  # 245
    StructField('throttle_position', DecimalType(20, 7), True),  # 246
    StructField('throttle_position_control_set_value', DecimalType(20, 7), True),  # 247
    StructField('throttle_sensor_voltage', DecimalType(20, 7), True),  # 248
    StructField('total_catalyst_consumption', DecimalType(20, 7), True),  # 249
    StructField('total_distance', DecimalType(20, 7), True),  # 250
    StructField('total_engine_round', DecimalType(20, 7), True),  # 251
    StructField('total_fuel', DecimalType(20, 7), True),  # 252
    StructField('total_gas_consumption', DecimalType(20, 7), True),  # 253
    StructField('total_run_time', DecimalType(20, 7), True),  # 254
    StructField('trip_catalyst_consumption', DecimalType(20, 7), True),  # 255
    StructField('trip_fuel_consumption', DecimalType(20, 7), True),  # 256
    StructField('trip_mileage', DecimalType(20, 7), True),  # 257
    StructField('up_nox_concentration', DecimalType(20, 7), True),  # 258
])

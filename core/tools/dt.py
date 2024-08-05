# -*- coding: UTF-8 -*-
"""
@Project ：pyetl
@File    ：dt.py
@Author  ：qjy20472
@Date    ：2023/2/15 14:34
@Desc    : 日期相关功能
"""

import datetime
import time
from datetime import timedelta


def unix2timestamp(t: int, fmt="%Y-%m-%d %H:%M:%S") -> str:
    """unix时间转字符串

    Args:
        t (int): Unix时间
        fmt (str, optional): 输出字符串时间格式. Defaults to "%Y-%m-%d %H:%M:%S".

    Returns:
        str: 字符串时间
    """
    time_local = time.localtime(t)
    dt = time.strftime(fmt, time_local)
    return dt


def timestamp2unix(st: str, fmt="%Y-%m-%d %H:%M:%S") -> int:
    """字符串转unix时间

    Args:
        st (str): 字符串时间
        fmt (str, optional): 字符串时间格式. Defaults to "%Y-%m-%d %H:%M:%S".

    Returns:
        int: unix时间
    """
    new_dt = datetime.datetime.strptime(st, fmt)
    return int(time.mktime(new_dt.timetuple()))


def unix2datetime(unix_time: int):
    """unix转datetime类型时间"""
    return datetime.datetime.fromtimestamp(unix_time)


def date_range(start: str, end: str):
    """
    获取两个日期之间的所有日期

    :param start: 开始日期
    :param end: 结束日期
    :return: 日期列表
    """
    start_date = datetime.datetime.strptime(start, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end, '%Y-%m-%d')
    delta = end_date - start_date  # as timedelta
    days = [start_date + timedelta(days=i) for i in range(delta.days + 1)]
    return days


def get_time_list(start_time, end_time, time_step, max_step=3600):
    """
    依据开始时间、结束时间、时间步长，对时间进行分段
    说明：若发生跨天时间，按天进行分割

    example:

    >> input: get_time_list('2022-10-01 10:10:00', '2022-10-01 11:10:00', 1111)
    >> output: ['2022-10-01 10:10:00', '2022-10-01 10:28:31', '2022-10-01 10:47:02',
                '2022-10-01 11:05:33', '2022-10-01 11:10:00']

    >> input: get_time_list('2022-10-01 23:58:00', '2022-10-02 01:15:00', 1111)
    >> output: ['2022-10-01 23:58:00', '2022-10-02 00:00:00', '2022-10-02 00:16:31', '2022-10-02 00:35:02',
                '2022-10-02 00:53:33', '2022-10-02 01:12:04', '2022-10-02 01:15:00']

    :param start_time: 开始时间
    :param end_time: 结束时间
    :param time_step: 时间步长, 单位：秒
    :param max_step: 最大步长, 单位：秒
    :return:
    """
    if len(start_time) == 10:
        start_time += ' 00:00:00'

    if len(end_time) == 10:
        end_time += ' 00:00:00'

    start_datetime = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_datetime = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    if time_step >= max_step:
        print(f"time_step设置过大({time_step})，强制赋值{max_step}")
        time_step = max_step

    now_datetime = start_datetime
    time_list = [now_datetime.strftime("%Y-%m-%d %H:%M:%S")]
    # time_list = []
    while now_datetime < end_datetime:
        # print(1, now_datetime)
        _dt = now_datetime + datetime.timedelta(seconds=time_step)
        # 检查是否会超过end_time
        if _dt > end_datetime:  # 若超过end_time, 赋值end_time
            _dt = end_datetime
        elif _dt == end_datetime:
            time_list.append(_dt.strftime("%Y-%m-%d 00:00:00"))
            break
        # print(2, _dt)
        
        # 检查 + 时间步长后，是否跨天
        if (_dt.day != now_datetime.day) & (_dt.minute != 0) & (_dt.second != 0):
            # 跨天且不为0点，把这段时间分割为两段
            time_list.append(_dt.strftime("%Y-%m-%d 00:00:00"))
            time_list.append(_dt.strftime("%Y-%m-%d %H:%M:%S"))
            now_datetime = _dt
        elif (_dt.day != now_datetime.day) & ((_dt.minute == 0) & (_dt.second == 0)):
            time_list.append(_dt.strftime("%Y-%m-%d %H:%M:%S"))
            now_datetime = _dt
        else:
            time_list.append(_dt.strftime("%Y-%m-%d %H:%M:%S"))
            now_datetime = _dt
    return time_list

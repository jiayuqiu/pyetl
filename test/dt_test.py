"""
 @Author: qjy20472
 @Email: qjy20472@snat.com
 @FileName: dt_test.py
 @DateTime: 3/17/23 2:09 PM
 @SoftWare: PyCharm
 @Desc: 
"""

import sys
sys.path.append('/home/qjy20472/pythonProjects/pyetl/')

from core.tools.dt import get_time_list


print(get_time_list('2023-01-03 23:50:00', '2023-01-04 00:10:00', 400))

# print(dt.get_time_list('2022-10-01 23:58:00', '2022-10-02 01:15:00', 1111))

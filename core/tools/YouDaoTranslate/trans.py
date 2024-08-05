#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project ：pyetl 
@File    ：trans.py.py
@Author  ：qjy20472
@Date    ：8/8/23 1:29 PM 
@Desc    : 有道api翻译接口
"""

from pprint import pprint
import requests
import json

from core.tools.YouDaoTranslate.utils.AuthV3Util import addAuthParams

# 您的应用ID
APP_KEY = '05cf3edfd392c59e'
# 您的应用密钥
APP_SECRET = 'KAAETePGJzfgmLBRKXTXGIUVwcTqxG4d'


def createRequest(q):
    """
    创建q

    :param q:
    :return:
    """
    # q = '待翻译文本'
    lang_from = 'zh-CHS'
    lang_to = 'en'
    # vocab_id = '您的用户词表ID'

    data = {'q': q, 'from': lang_from, 'to': lang_to}

    addAuthParams(APP_KEY, APP_SECRET, data)

    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    res = doCall('https://openapi.youdao.com/api', header, data, 'post')
    res_dict = json.loads(str(res.content, 'utf-8'))
    # pprint(res_dict)
    return res_dict['translation'][0]

def doCall(url, header, params, method):
    if 'get' == method:
        return requests.get(url, params)
    elif 'post' == method:
        return requests.post(url, params, header)


# 网易有道智云翻译服务api调用demo
# api接口: https://openapi.youdao.com/api
if __name__ == '__main__':
    createRequest('进气流量漂移补偿')

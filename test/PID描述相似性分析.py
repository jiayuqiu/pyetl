#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project ：pyetl 
@File    ：PID描述相似性分析.py
@Author  ：qjy20472
@Date    ：2023/8/8 15:50 
@Desc    : 描述相似性分析
"""

import os
import sys
cur_file_path = os.path.abspath(__file__)
pydmp_path = os.path.dirname(os.path.dirname(os.path.dirname(cur_file_path)))
print(f"pyetl path = {pydmp_path}")
sys.path.append(pydmp_path)


import pandas as pd
import numpy as np
import jieba
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from gensim.models import Word2Vec
from core.conf import mysql_engine_prod


def cos():
    """
    jieba分词后计算余弦相似度
    :return:
    """
    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(df['desc_cut'])
    similarity_matrix = cosine_similarity(X)
    indices = np.where(similarity_matrix > 0.6)
    print(indices[0], indices[1])

    #
    for r, c in zip(indices[0], indices[1]):
        if similarity_matrix[r][c] > 0.5:
            if df['target_desc'].values[r] != df['target_desc'].values[c]:
                print(similarity_matrix[r][c], df['target_desc'].values[r], df['target_desc'].values[c])


def w2v(df):
    # 将分词后的文本转换为列表形式
    sentences = [desc.split() for desc in df['seg_desc']]

    # 训练Word2Vec模型
    model = Word2Vec(sentences, vector_size=80, window=2, min_count=1, workers=4)

    # 获取每个元素的Word2Vec向量表示
    vec_desc = df['seg_desc'].apply(lambda x: [model.wv[word] for word in x.split()])

    # 输出每个元素的Word2Vec向量表示
    return vec_desc


def get_cos_similarities(df):
    # 计算每个元素之间的相似性
    similarities = []
    for i in range(len(df)):
        for j in range(i+1, len(df)):
            sim = cosine_similarity(df['vec_desc'].iloc[i], df['vec_desc'].iloc[j])
            similarities.append((i, j, sim))

    # 输出相似性结果
    sim_list = []
    for i, j, sim in similarities:
        if df['target_desc'].values[i] != df['target_desc'].values[j]:
            sim_list.append([df['target_desc'].values[i], df['name'].values[i], df['pid'].values[i],
                             df['target_desc'].values[j], df['name'].values[j], df['pid'].values[j],
                             np.mean(sim)])
    sim_df = pd.DataFrame(sim_list, columns=['desc_1', 'name_1', 'pid_1', 'desc_2', 'name_2', 'pid_2', 'sim'])
    sim_df.sort_values(by='sim', ascending=False, inplace=True)
    # sim_df.boxplot(column='sim')
    sim_df = sim_df.loc[sim_df['sim'] > 0.27]
    sim_df.to_csv(os.path.join(workpath, 'data/pid_desc_sim.csv'), index=False, encoding='utf-8-sig')
    print(sim_df)


def get_para_df():
    """获取eol系统的para表
    """
    para_df = pd.read_sql(
        sql=f"select * from ods.eol_notccpparainfo",
        con=mysql_engine_prod
    )
    print(para_df)
    return para_df


if __name__ == '__main__':
    workpath = '/data1/pythonProjects/pyetl'
    para_df = get_para_df()

    df = pd.read_excel(os.path.join(workpath, 'data/PID统计_all.xlsx'), sheet_name='PID')
    df = df.loc[~df['target_desc'].isna()]

    # pid 添加eol表中的name 英文变量名
    para_pid_df = pd.merge(
        left=df, right=para_df,
        left_on='pid', right_on='PID', how='left'
    )
    para_pid_columns = ['pid', 'target_desc', 'name']
    para_pid_df = para_pid_df.loc[:, para_pid_columns]

    para_pid_df['seg_desc'] = para_pid_df['target_desc'].apply(lambda x: ' '.join(jieba.cut(x)))

    # w2v
    para_pid_df.loc[:, 'vec_desc'] = w2v(para_pid_df)

    # cos similarities
    get_cos_similarities(para_pid_df)

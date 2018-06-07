#!/home/admin/anaconda3/envs/TF/bin/ python3.5
# -*- coding: utf-8 -*-  
'''
Created on 2018年6月7日

@author: Zhukun Luo
Jiangxi university of finance and economics
'''
#数据的序列化与反序列化
#str()对不可变类型（例如元组）进行序列化->pickle or 流式传输的字符串，然后通过反序列化（python中的ast模块，使用literal_eval()解析元组字符串）
import ast
def map(key,val):
    #解析复合键
    key=ast.literal_eval(key)
    #以字符串写新的键
    return (str(key),val)
#当键值变得复杂，考虑使用更紧凑的数据结构，可以减少流量，结构化数据的常见表示形式：Base64编码的JSON字符串，仅适用ASCLL字符，易使用标准库序列化和反序列
import json
import base64
def serialize(data):
    '''
    返回数据（键，值）的Base64编码的JSON格式
    '''
    return base64.b64encode(json.dumps(data))
def deserialize(data):
    '''
    解码Base64编码的JSON数据
    '''
    return json.loads(base64.b64decode(data))

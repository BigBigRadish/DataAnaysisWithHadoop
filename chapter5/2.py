#!/home/admin/anaconda3/envs/TF/bin/ python3.5
# -*- coding: utf-8 -*- 
'''
Created on 2018年6月7日

@author: Zhukun Luo
Jiangxi university of finance and economics
'''
import csv
from pyspark import SparkConf,SparkContext
from io import  StringIO
from _datetime import datetime
APP_NAME="my 5.1.2 app demo"
#键空间模式
#键空间常用变换方式
def split(line):
    '''
    使用csv模块分割行的函数
    '''
    reader=csv.reader(StringIO(line))
    return next(reader)
def parse_date(str):
    return datetime.strptime(str)
def main(sc):
    orders=sc.textFile("order.csv").map(split)#将订单加载到一个RDD，解析csv
    orders= orders.map(lambda r:((r[0],r[1],r[2]),r[3:]))#键分配（orderid，customerid，date），product
    orders=orders.map(lambda(k,v):((k[0],parse_date(k[2])),len(v)))#计算订单大小，并将键拆分为orderid和date
    orders=orders.map(lambda(k,v):((v,k[1]),k[0]))#交换键值，按时间排序
    orders=orders.sortByKey(ascending=False)#根据键将订单排序
    orders=orders.map(lambda (k,v):(v,k))#再次交换键值，以便使用订单ID为键
    print (orders.take(10))    
if __name__ == '__main__':
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
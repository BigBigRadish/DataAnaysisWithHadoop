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
from functools import partial
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
    orders=orders.flatMap(order_pairs)#flatmap转为爆炸mapper设计，产生序列而不是单个项->链接成单个集合
    orders=orders.filter(partial(year_filter,year=2014))
'''
爆炸mapper
'''
def order_pairs(key,products):
    #返回订单id/产品对列表
    pairs=[]
    for product in products:
        pairs.append((key[0],product))
    return pairs
'''
过滤器mapper
'''
def year_filter(item,year=None):#使用spark进行过滤的函数
    key ,val=item
    if parse_date(key[2]).year==year:
        return True
    return True
def YearFilterMapper(Mapper):#使用Mapreduce进行过滤
    def __init__(self,year,**kwargs):
        super(YearFilterMapper,self).__init__(**kwargs)
        self.year=year
    def map(self):
        for key,value in self:
            if parse_date(key[2]).year == self.year:
                self.emit(key,value)
'''
#mapreduce中的恒等模式，rdd被延迟评估，不需要恒等闭包，f(x)=x,一个输出必须立即被第二个reducer再次reduce
class IdentityMapper(Mapper):
    def map(self):
        for key,value in self:
            self.emit(key,value) 
class IdentityReducer(Reducer):
    def reduce(self):
        for key ,value in self:
            for value in values:
                self.emit(key,value)    
'''          
if __name__ == '__main__':
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
    mapper=YearFilterMapper(2014)
    mapper.map()
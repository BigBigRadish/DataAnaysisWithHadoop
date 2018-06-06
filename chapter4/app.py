#!/home/admin/anaconda3/envs/TF/bin/ python3.5
# -*- coding: utf-8 -*-  
'''
Created on 2018年6月6日

@author: Zhukun Luo
Jiangxi university of finance and economics
'''
import csv 
import matplotlib.pyplot as plt

from io import StringIO
from datetime import datetime
from collections import  namedtuple
from operator import  add,itemgetter
from pyspark import SparkConf,SparkContext
from matplotlib.lines import lineStyles

APP_NAME=" Flight Delay Analysis"
DATE_FMT="%Y-%m-%d"
TIME_FMT="%H%M"

fields=('date','airline','flightnum','origin','dest','dep','dep_delay','arv','arv_delay','airtime','distance')
Flight=namedtuple('Flight',fields)

def parse(row):
    row[0]=datetime.strptime(row[0],DATE_FMT).date()
    row[5]=datetime.strptime(row[5],TIME_FMT).time()
    row[6]=float(row[6])
    row[7]=datetime.strptime(row[7],TIME_FMT).time()
    row[8]=float(row[8])
    row[9]=float(row[9])
    row[10]=float(row[10])
    return Flight(*row[:11])
def split(line):
    '''
    使用csv模块分割行的函数
    '''
    reader=csv.reader(StringIO(line))
    return next(reader)
def plot(delays):
    '''
    显示航空公司总延误状况柱状图
    '''
    airlines=[d[0] for d in delays]
    minutes=[d[1] for d in delays]
    index=list(airlines)
    fig,axe=plt.subplots()
    bars=axe.barh(index,minutes)
    #左右侧添加总分钟数
    for idx,air,min in zip(index,airlines,minutes):
        if min>0:
            bars[int(idx)].set_color("#d9230f")
            axe.annotate("%0.0f min" % min,xy=(min+1,idx+0.5),va="center" )
        else:
            bars[int(idx)].set_color("#469408")
            axe.annotate("%0.0f min" % min,xy=(10,idx+0.5),va="center" )
    #设置tick
    ticks=plt.yticks([idx+0.5 for idx in index],airlines)
    xt=plt.xticks()[0]
    plt.xticks(xt,[' ']*len(xt))
    # 最小化图表垃圾
    plt.grid(axis='x', color='white', lineStyles='-')
    plt.title('total mimutes delayed per airlines')
    plt.show()
def main(sc):
    #加载航空公司查找字典
    airlines=dict(sc.textFile('airlines.csv').map(split).collect())
    #将查找字典广播到集群
    airline_lookup=sc.broadcast(airlines)
    #读取csv数据到一个RDD
    flights=sc.textFile('flights.csv').map(split).map(parse)    
    #映射总延误时间到航空公司（使用广播变量进行关联）
    delays=flights.map(lambda f:(airline_lookup.value[f.airline],add(f.dep_delay,f.arvdelay)))  
    #航空公司总延误时间
    delays=delays.reduceByKey(add).collect()
    delays=sorted(delays,key=itemgetter(1))
    
    #驱动程序提供输出
    for d in delays:
        print('%0.0f minutes delayed \t%s' % (d[1],d[0]))
    #显示延误时间柱状图
    plot(delays)
if __name__ == '__main__':
     # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
    
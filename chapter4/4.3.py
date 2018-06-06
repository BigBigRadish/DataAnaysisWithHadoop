# -*- coding: utf-8 -*-  
'''
@author:Zhukun Luo
Jiangxi university of finance and economics
'''
#spark应用程序，使用spark-submit执行
##导入
from pyspark import SparkConf,SparkContext
##共享变量和数据
APP_NAME=" my spark Application"
##闭包函数
##主要功能
def main(sc):
    '''
    描述RDD转换和动作
    '''
    pass
if __name__ == '__main__':
    # 配置spark
    conf=SparkConf().SetAppName(APP_NAME)
    conf=conf.setMaster("local[*]")
    sc=SparkContext(conf=conf)
    #执行
    main(sc)
    
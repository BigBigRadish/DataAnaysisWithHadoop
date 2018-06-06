#!/usr/bin/env python
# -*- coding: utf-8 -*-  
'''
@author:Zhukun Luo
Jiangxi university of finance and economics
'''
import sys
from itertools import groupby#内存安全的迭代器帮助函数
from operator import itemgetter#操作符函数
SEP="\t"
class Reducer(object):
    def __init__(self,stream,sep=SEP):
        self.stream=stream
        self.sep=sep
    def emit(self,key,value):
        sys.stdout.write("{}{}{}\n".format(key, self.sep,value)) 
    def reduce(self):
        for current,group in groupby(self,itemgetter(0)):
            total =0
            count =0
            for item in group:
                total += item[1]
                count +=1
                self.emit(current, float(total)/float(count))   
    def __iter__(self):
        for line in self.stream:
            try:
                parts=line.split(self.sep)
                yield parts[0],float(parts[1])
            except:
                continue
if __name__ == '__main__':
    reducer =Reducer(sys.stdin)
    reducer.reduce()
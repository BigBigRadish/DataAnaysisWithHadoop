#!/home/admin/anaconda3/envs/TF/bin/ python3.5
# -*- coding: utf-8 -*- 
'''
Created on 2018年6月7日

@author: Zhukun Luo
Jiangxi university of finance and economics
'''
#5.1.3 pair and stripe
#向量，矩阵和数据框虽然紧凑，但scale太大；pair和stripe用来表示矩阵，都是基于键的计算
#建立单词共现矩阵
from itertools import combinations
from chapter3.mapper import  Mapper
from chapter3.reducer import Reducer
class WordPairsMapper(Mapper):
    def map(self):
        for docid,document in self:
            tokens=list(self.tokenize(document))
            for pair in combinations(sorted(tokens),2):#字典排序
                self.emit(pair,1)
#stripe 减少中间对的数量和网络通信，在mapper中为每个条目构造关联数组
from collections import Counter
class WordStripeMapper(Mapper):
    def map(self):
        for docid,document in self:
            tokens=list(self.tokenize(document))
            for i,item in enumerate(tokens):
                #为每个条目创建stripe
                stripe=Counter()
                for j,token in enumerate(tokens):
                    #不计算该条目与本身共现
                    if i !=j:
                        stripe[token]+=1
                #发射条目和stripe
                self.emit(item, stripe)
class StripeSumReducer(Reducer):
    def reduce(self):
        for key ,values in self:
            stripe=Counter()
            #将所有计数器相加
            for value in values:
                for token,count in value.iteritems():
                    #为每一个令牌分别累加stripe
                    stripe[stripe]+=count
            self.emit(key, stripe)

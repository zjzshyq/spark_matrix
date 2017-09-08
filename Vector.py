# -*- coding: utf-8 -*-
"""
Created on Thu Sep 07 21:14:52 2017

@author: ligong

@description:这是Vector的实现
"""
import numpy as np
from sklearn import preprocessing
from scipy.sparse import coo_matrix

MAX_LENGTH = 100000000
class Vector(object):
    def __init__(self,length):
        """
        初始化
        """
        global MAX_LENGTH
        if length > MAX_LENGTH or length <= 0 or not isinstance(length,int):
            raise Exception('Vector Length Error!')
        self.vector = None
        self.length = length
        
    def ones(self):
        self.vector = np.ones((1,self.length))
    
    def zeros(self):
        self.vector = np.zeros((1,self.length))
        
    def normal(self,mu=0,sigma=1):
        """
        正太
        """
        self.vector = np.random.normal(mu,sigma,(1,self.length))
    
    def normalize(self,norm='l2'):
        """
        正则化
        """
        self.vector = preprocessing.normalize(self.vector, norm=norm)
        
    def to_coo(self):
        """
        转化为稀疏形式
        """
        if isinstance(self.vector,coo_matrix):
            return
        row = [0 for i in xrange(self.length)]
        col = [i for i in xrange(self.length)]
        data = [self.vector[0][i] for i in xrange(self.length)]
        self.vector = coo_matrix((data, (row, col)),shape=(1, self.length))
       
    
    def multiply(self,vector):
        """
        向量乘法
        """
        if isinstance(vector,int) or isinstance(vector,float):
            result = Vector(self.length)
            result.vector = vector*self.vector
            return result
        if not isinstance(vector,Vector):
            raise Exception('Vector Type Error!')
        if vector.length != self.length:
            raise Exception('Vector Length:(%s,%s) Not Match!' % (self.length,vector.length))
        
        
        
        if isinstance(self.vector,np.ndarray) and isinstance(vector.vector,np.ndarray):
            return sum(sum((self.vector*vector.vector)))
        
        self.to_coo()
        vector.to_coo()
        return (self.vector*vector.vector.T).data[0]
       
    def __mul__(self,value):
        """
        数字乘法
        """
        assert isinstance(value,int) or isinstance(value,float)
        return self.multiply(value)

    def __str__(self):
        return str(self.vector)

    def __add__(self,vector):
        """
        矩阵加法
        """
        result = Vector(self.length)
        if isinstance(vector,int) or isinstance(vector,float):
            result.vector = vector+self.vector
            return result
        if not isinstance(vector,Vector):
            raise Exception('Vector Type Error!')
        if vector.length != self.length:
            raise Exception('Vector Length:(%s,%s) Not Match!' % (self.length,vector.length))

        result = Vector(self.length)

        if isinstance(self.vector,np.ndarray) and isinstance(vector.vector,np.ndarray):
            result.vector = self.vector+vector.vector
            return result
        self.to_coo()
        vector.to_coo()
        result = Vector(self.length)
        result.vector = self.vector+vector.vector
        return result

    def __sub__(self,vector):
        """
        减法
        """
        result = Vector(self.length)
        if isinstance(vector,int) or isinstance(vector,float):
            result.vector = self.vector - vector
            return result
        if not isinstance(vector,Vector):
            raise Exception('Vector Type Error!')
        if vector.length != self.length:
            raise Exception('Vector Length:(%s,%s) Not Match!' % (self.length,vector.length))

        result = Vector(self.length)

        if isinstance(self.vector,np.ndarray) and isinstance(vector.vector,np.ndarray):
            result.vector = self.vector-vector.vector
            return result
        self.to_coo()
        vector.to_coo()
        result = Vector(self.length)
        result.vector = self.vector-vector.vector
        return result

        
if __name__ == '__main__':
    v = Vector(100)
    v.ones()
    vv = Vector(100)
    vv.ones()
    vv.to_coo()
    print type(v + 5)
        

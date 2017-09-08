# -*- coding: utf-8 -*-
"""
Created on Thu Sep 07 14:48:18 2017

@author: ligong

@description:这是来做矩阵相关的一些值
包括：
L2
"""
from MatrixInRDD import MatrixInRDD
from BlockMatrixInRDD import BlockMatrixInRDD
from operator import add
import math

def trace(matrix,partitions=100):
    """
    获得trace
    """
    assert isinstance(matrix,MatrixInRDD)
    assert matrix.cols == matrix.rows
    matrix.change_format(MatrixInRDD.PAIR)
    r = matrix.rdd.map(lambda _:('TRACE',_[1] if _[0][1] == _[0][0] else 0)).reduceByKey(add,partitions).collect()
    return sum(map(lambda _:_[1],r))

def transpose(matrix):
    """
    转置
    """
    assert isinstance(matrix,MatrixInRDD)
    #处理block矩阵的问题
    if isinstance(matrix,BlockMatrixInRDD):
        matrix.deblocking()

    rdd = matrix.rdd
    
    #三种不同的格式处理函数
    def pair_format(kv):
        return ((kv[0][1],kv[0][0]),kv[1])
        
    def row_col_format(kv):
        return (kv[1][0],(kv[0],kv[1][1]))
    if matrix.format == MatrixInRDD.PAIR:
        trans_func = pair_format
    elif matrix.format in [MatrixInRDD.ROW,MatrixInRDD.COL]:
        trans_func = row_col_format
    else:
        raise Exception('Data Format Error!')
    
    #生成新矩阵
    new_matrix = MatrixInRDD(matrix.cols,matrix.rows)
    new_matrix.set_format(matrix.format)
    new_matrix.set_rdd(rdd.map(lambda _:trans_func(_)))
    return new_matrix
    
    
def norm(matrix,ntype='l2',partitions=100):
    """
    求norm
    """
    assert isinstance(matrix,MatrixInRDD)

    #处理block矩阵的问题
    if isinstance(matrix,BlockMatrixInRDD):
        matrix.deblocking()
    
    def denorm_value(v):
        if ntype == 'l1':
            return v
        elif ntype == 'l2':
            return math.sqrt(v)
        elif ntype.startswith('l'):
            try:
                return v**(1.0/float(ntype[1:]))
            except:
                raise Exception('Norm Type Error!')
        
    def norm_value(v):
        if ntype == 'l1':
            return abs(v)
        elif ntype == 'l2':
            return v*v
        elif ntype.startswith('l'):
            try:
                return v**float(ntype[1:])
            except:
                raise Exception('Norm Type Error!')
    
    #三种不同的格式处理函数
    def pair_format(kv):
        return norm_value(kv[1])
        
    def row_col_format(kv):
        return norm_value(kv[1][1])
        
    if matrix.format == MatrixInRDD.PAIR:
        trans_func = pair_format
    elif matrix.format in [MatrixInRDD.ROW,MatrixInRDD.COL]:
        trans_func = row_col_format
    else:
        raise Exception('Data Format Error!')
    r = matrix.rdd.map(lambda _:('NORM',trans_func(_))).reduceByKey(add,partitions).collect()
    s = 0
    return denorm_value(sum(map(lambda _:_[1],r)))
    
    
    


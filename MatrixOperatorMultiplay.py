# -*- coding: utf-8 -*-
"""
Created on Fri Sep 08 11:06:30 2017

@author: ligong

@description:这是矩阵的乘法
"""
from operator import add
from MatrixInRDD import MatrixInRDD
from BlockMatrixInRDD import BlockMatrixInRDD
from RowMatrixInRDD import RowMatrixInRDD
from Vector import Vector
MIN_BLOCK_SIZE = 100

def matrix_matrix_multiply(block_matrix_obj1,block_matrix_obj2,partitions=100):
    """
    矩阵与矩阵乘积
    """
    #类型检查和大小检查
    assert isinstance(block_matrix_obj1,BlockMatrixInRDD)
    assert isinstance(block_matrix_obj2,BlockMatrixInRDD)
    assert block_matrix_obj1.block_size == block_matrix_obj2.block_size
    assert block_matrix_obj1.cols == block_matrix_obj2.rows
        
    #获得右边矩阵的块-列
    COLS = block_matrix_obj2.cols/block_matrix_obj2.block_size
    block_size =  block_matrix_obj1.block_size
    #获得左边矩阵的块-行
    ROWS = block_matrix_obj1.cols/block_matrix_obj1.block_size
    def left(kv):
        """
        生成一个个键值对
        """
        keys = []
        for i in xrange(COLS):
            keys.append((kv[0][0],i,kv[0][1]))
        return map(lambda _:(_,kv[1]),keys)
        
    def right(kv):
        """
        生成一个个键值对
        """
        keys = []
        for i in xrange(ROWS):
            keys.append((i,kv[0][1],kv[0][0]))
        return map(lambda _:(_,kv[1]),keys)

    def dot(items):
        return items[0].dot(items[1])
            
    def to_pair(kv):
        (row,col) = kv[0]
        kv = dict(kv[1].todok())
        result = []
        for (k,v) in kv.iteritems():
            result.append(((row*block_size+k[0],col*block_size+k[1]),v))
        return result
            
    #分块化
    if not block_matrix_obj1.blocked:
        block_matrix_obj1.blocking()
    if not block_matrix_obj2.blocked:
        block_matrix_obj2.blocking()
            
    rdd = block_matrix_obj1.rdd.flatMap(lambda _:left(_)).join(block_matrix_obj2.rdd.flatMap(lambda _:right(_)),
                partitions).mapValues(lambda _:_[0].dot(_[1])).map(lambda _:((_[0][0],_[0][1]),_[1])).\
                reduceByKey(lambda x,y:x+y,partitions).flatMap(lambda _:to_pair(_))
        
    #生成对象
    bmirdd = BlockMatrixInRDD(block_matrix_obj1.rows,block_matrix_obj2.cols,block_size)
    bmirdd.set_rdd(rdd)
    return bmirdd
    
def matrix_vector_multiply(matrix,vector,spark_context,partitions=100):
    """
    矩阵与向量的乘积，vector广播
    """
    #类型检查和大小检查
    assert isinstance(matrix,RowMatrixInRDD)
    assert isinstance(vector,Vector)
    assert matrix.cols == vector.length
    #print matrix.rdd.take(10)
    matrix.change_format(MatrixInRDD.ROW) 
    #先转化为稀疏，然后广播
    b_vector = spark_context.broadcast(vector.vector)
    #print b_vector.value[0] 
    def dot(item):
        cid,value = item[0],item[1]
        return value*b_vector.value[0][cid]
    #print matrix.rdd.take(10)
    
    result = matrix.rdd.mapValues(lambda _:dot(_)).reduceByKey(add,partitions).collect()
    #print result
    answer = Vector(matrix.rows)
    answer.zeros()
    for (rid,v) in result:
        answer.vector[0][rid] = v
    return answer
    
def multiply(obj1,obj2,spark_context,partitions=100):
    global MIN_BLOCK_SIZE
    if isinstance(obj1,MatrixInRDD) and isinstance(obj2,Vector):
        #生成对象
        bobj1 = RowMatrixInRDD(obj1.rows,obj1.cols)
        bobj1.set_rdd(obj1.rdd)
        bobj1.change_format(MatrixInRDD.ROW)
        return matrix_vector_multiply(bobj1,obj2,partitions)
    if isinstance(obj1,MatrixInRDD) and isinstance(obj2,MatrixInRDD):
        #生成对象
        bobj1 = BlockMatrixInRDD(obj1.rows,obj1.cols,MIN_BLOCK_SIZE)
        bobj1.set_rdd(obj1.rdd)
        bobj2 = BlockMatrixInRDD(obj2.rows,obj2.cols,MIN_BLOCK_SIZE)
        bobj2.set_rdd(obj2.rdd)
        return matrix_matrix_multiply(bobj1,bobj2,partitions)
    if isinstance(obj1,Vector) and isinstance(obj2,Vector):
        #生成对象
        return obj1.multiply(obj2)

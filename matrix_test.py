# -*- coding: utf-8 -*-
"""
Created on Fri Sep 08 13:32:31 2017

@author: ligong

@description:这是矩阵相关的测试程序
"""
from MatrixOperatorMultiplay import *
from MatrixOperatorOther import *

def merge_test():
    bmirdd = MatrixInRDD(100,100)
    bmirdd.load(sc,'hdfs:/tmp/b.txt',MatrixInRDD.PAIR,'int',10)
    result = transpose(bmirdd)
    print trace(multiply(bmirdd,result,None))

def transpose_test():
    bmirdd = MatrixInRDD(100,100)
    bmirdd.load(sc,'hdfs:/tmp/b.txt',MatrixInRDD.PAIR,'int',10)
    result = transpose(bmirdd)
    print result.rdd.collect()

def norm_test():
    bmirdd = MatrixInRDD(100,100)
    bmirdd.load(sc,'hdfs:/tmp/b.txt',MatrixInRDD.PAIR,'int',10)
    result = norm(bmirdd,'l2',10)
    print result

def matrix_matrix_multiply_test():
    bmirdd = BlockMatrixInRDD(100,100,10)
    bmirdd.load(sc,'hdfs:/tmp/b.txt',MatrixInRDD.PAIR,'int',10)
    result = matrix_matrix_multiply(bmirdd,bmirdd,10)
    
    print result.rdd.collect()

def matrix_vector_multiply_test(sc):
    bmirdd = RowMatrixInRDD(100,100)
    bmirdd.load(sc,'hdfs:/tmp/b.txt',MatrixInRDD.PAIR,'int',10)
    v = Vector(100)
    v.ones()
    result = matrix_vector_multiply(bmirdd,v,sc,10)
    print result.vector

def matrix_add_test():
    bmirdd = MatrixInRDD(100,100)
    bmirdd.load(sc,'hdfs:/tmp/b.txt',MatrixInRDD.PAIR,'int',10)
    
    result = bmirdd - bmirdd
    result.filter()
    print result.rdd.collect()

if __name__ == '__main__':
    import pyspark as ps
    conf = ps.SparkConf().setAppName("CF").setMaster("spark://spark31:7077")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.cores.max","100") 
    conf.set("spark.driver.maxResultSize","5g")
    sc = ps.SparkContext(conf=conf)
    #matrix_matrix_multiply_test()
    #matrix_vector_multiply_test(sc)
    #transpose_test()
    #norm_test()
    #matrix_add_test()
    merge_test()

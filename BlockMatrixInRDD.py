# -*- coding: utf-8 -*-
"""
Created on Wed Sep 06 11:52:27 2017

@author: ligong

@description:这是实现分块矩阵法
"""
from scipy.sparse import coo_matrix
from MatrixInRDD import MatrixInRDD
MIN_BLOCK_SIZE = 100
class BlockMatrixInRDD(MatrixInRDD):
    def __init__(self,rows,cols,block_size=None):
        """
        初始化
        rows,cols矩阵的行和列
        blocked:标记有没有分块
        """
        global MIN_BLOCK_SIZE
        super(BlockMatrixInRDD,self).__init__(rows,cols)
        self.block_size = block_size or MIN_BLOCK_SIZE
        self.blocked = False
        self.format = MatrixInRDD.PAIR
        
    def set_block_size(self,block_size):
        """
        设置block_size
        """
        self.block_size = block_size
    
        
    def blocking(self,partitions=100):
        """
        加载数据
        """
        if self.blocked:
            return
        self.change_format(MatrixInRDD.PAIR)
        block_size = self.block_size
        def to_block(kv):
            """
            生成块
            """
            (rid,cid),v = kv[0],kv[1]
            key = (rid/block_size,cid/block_size)
            value = [(rid % block_size,cid % block_size,v)]
            return (key,value)
        
        #累加所有的数据
        def merge(items):
            row,col,data = [],[],[]
            for item in items:
                row.append(item[0])
                col.append(item[1])
                data.append(item[2])
            return coo_matrix((data, (row, col)),shape=(block_size, block_size))
        
        self.rdd = self.rdd.map(lambda _:to_block(_)).reduceByKey(lambda x,y:x+y,partitions).mapValues(lambda _:merge(_))
        self.blocked = True
     
    def deblocking(self):
        """
        重新恢复成点对的格式
        """
        if not self.blocked:
            return
        block_size = self.block_size
        def to_pair(kv):
            (row,col) = kv[0]
            kv = dict(kv[1].todok())
            result = []
            for (k,v) in kv.iteritems():
                result.append(((row*block_size+k[0],col*block_size+k[1]),v))
            return result
        self.rdd = self.rdd.flatMap(lambda _:to_pair(_))
    
    def change_format(self,dformat):
        self.deblocking()
        super(BlockMatrixInRDD,self).change_format(dformat)
    
    
if __name__ == '__main__':
    import pyspark as ps
    conf = ps.SparkConf().setAppName("CF").setMaster("spark://spark31:7077")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.cores.max","100") 
    conf.set("spark.driver.maxResultSize","5g")
    sc = ps.SparkContext(conf=conf)
    
    bmirdd = BlockMatrixInRDD(100,100,10)
    bmirdd.load(sc,'hdfs:/tmp/b.txt','pair','int',10)
    print bmirdd.rdd.take(10)
    bmirdd.multiply(bmirdd)
    bmirdd.retransform()
    print bmirdd.rdd.collect()

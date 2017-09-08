# -*- coding: utf-8 -*-
"""
Created on Wed Sep 06 11:54:23 2017

@author: ligong

@description:这是matrix的类
"""
from operator import add
class MatrixInRDD(object):
    PAIR = 1
    ROW = 2
    COL = 3
    def __init__(self,rows,cols):
        """
        初始化
        rows,cols矩阵的行和列
        """
        self.rows,self.cols = rows,cols
    
    def set_rdd(self,rdd):
        """
        设置rdd
        """
        self.rdd = rdd
        
    def set_format(self,dformat):
        """
        设置rdd
        """
        assert dformat in [MatrixInRDD.ROW,MatrixInRDD.COL,MatrixInRDD.PAIR]
        self.format = dformat
        
    def load(self,sc,input_file,dformat,dtype='int',partitions=100):
        """
        dformat:数据格式
        row,pair
        """
        dtype = dtype.lower()
        data_f = lambda _: int(_) if dtype == 'int' else float(_)
        
        def extract_data(item):
            """
            提取数据
            row_idx;col_idx;v -->  ((row_idx,col_idx),v)
            """
            row_idx,col_idx,v = item.split(';')
            return ((int(row_idx),int(col_idx)),data_f(v))
        
        def row_line(line):
            """
            加载数据，数据格式：
            row_idx,col_idx;v,col_idx;v,col_idx;v,col_idx;v,col_idx;v
            """
            items = line.strip().split(',')
            row = int(items[0])
            result = []
            for item in items[1:]:
                col,value = item.split(';')
                result.append((row,(int(col),data_f(value))))

            return result
            
        def col_line(line):
            """
            加载数据，数据格式：
            col_idx,row_idx;v,row_idx;v,row_idx;v,row_idx;v,row_idx;v
            """
            items = line.strip().split(',')
            col = int(items[0])
            result = []
            for item in items[1:]:
                row,value = item.split(';')
                result.append((col,(int(row),data_f(value))))

            return result
 
        def pair_line(line):
            """
            加载数据，数据格式：
            每一行表示矩阵的一行
            row_idx;col_idx;v,row_idx;col_idx;v,row_idx;col_idx;v,row_idx;col_idx;v\n
            """
            items = line.strip().split(',')
            return map(lambda _:extract_data(_),items)
        
        #数据格式
        self.format = dformat
        if self.format == MatrixInRDD.COL:
            trans_fun = col_line
        elif self.format == MatrixInRDD.ROW:
            trans_fun = row_line
        elif self.format == MatrixInRDD.PAIR:
            trans_fun = pair_line
        else:
            raise Exception("Data Format Error!")
        self.rdd = sc.textFile(input_file,partitions).flatMap(lambda _:trans_fun(_))
        
    def change_format(self,dformat):
        """
        格式转换
        """
        if self.format == dformat:
            return
        dispatcher_dict = {(MatrixInRDD.PAIR,MatrixInRDD.COL):self.__pair_to_col__,
                           (MatrixInRDD.PAIR,MatrixInRDD.ROW):self.__pair_to_row__,
                           (MatrixInRDD.COL,MatrixInRDD.PAIR):self.__col_to_pair__,
                           (MatrixInRDD.ROW,MatrixInRDD.PAIR):self.__row_to_pair__,
                           (MatrixInRDD.COL,MatrixInRDD.ROW):self.__col_to_row__,
                           (MatrixInRDD.ROW,MatrixInRDD.COL):self.__row_to_col__}
        transfunc = dispatcher_dict.get((self.format,dformat),None)
        if transfunc is None:
            raise Exception('Data Format Error!')
        transfunc()
           
    
    def __row_to_pair__(self):
        """
        row到pair
        """
        self.rdd = self.rdd.map(lambda _:((_[0],_[1][0]),_[1][1]))
        self.format = MatrixInRDD.PAIR
    
    def __col_to_pair__(self):
        """
        col到pair
        """
        self.rdd = self.rdd.map(lambda _:((_[1][0],_[0]),_[1][1]))
        self.format = MatrixInRDD.PAIR
        
    def __pair_to_row__(self):
        """
        col到pair
        """
        self.rdd = self.rdd.map(lambda _:(_[0][0],(_[0][1],_[1])))
        self.format = MatrixInRDD.ROW
    

    def __pair_to_col__(self):
        """
        pair到col
        """
        self.rdd = self.rdd.map(lambda _:(_[0][1],(_[0][0],_[1])))
        self.format = MatrixInRDD.COL
        
    def __col_to_row__(self):
        """
        col到row
        """
        self.rdd = self.rdd.map(lambda _:(_[1][0],(_[0],_[1][1])))
        self.format = MatrixInRDD.ROW

    def __row_to_col__(self):
        """
        row到col
        """
        self.rdd = self.rdd.map(lambda _:(_[1][0],(_[0],_[1][1])))
        self.format = MatrixInRDD.COL
       
    def __add__(self,matrix,partitions=100):
        """
        矩阵加法，不支持加某个标量
        """ 
        assert isinstance(matrix,MatrixInRDD)
        assert matrix.cols == self.cols and matrix.rows == self.rows
        matrix.change_format(self.format)
        result = MatrixInRDD(self.rows,self.cols)
        result.format = self.format
        result.set_rdd(self.rdd.union(matrix.rdd).reduceByKey(add,partitions))
        return result

    def __sub__(self,matrix,partitions=100):
        """
        矩阵减法，不支持加某个标量
        """
        assert isinstance(matrix,MatrixInRDD)
        assert matrix.cols == self.cols and matrix.rows == self.rows
        matrix.change_format(self.format)
        result = MatrixInRDD(self.rows,self.cols)
        result.format = self.format
        def pair_value(value):
            return -value
        def row_col_value(value):
            return (value[0],-value[1])
        trans_fun = pair_value if self.format == MatrixInRDD.PAIR else row_col_value
        result.set_rdd(self.rdd.union(matrix.rdd.mapValues(lambda _:trans_fun(_))).reduceByKey(add,partitions))
        return result

    def filter(self):
        """
        把矩阵中的0值过滤掉
        """
        def pair_value(kv):
            return kv[1] != 0
        def row_col_value(kv):
            return kv[1][1] != 0
        trans_fun = pair_value if self.format == MatrixInRDD.PAIR else row_col_value
        self.rdd = self.rdd.filter(lambda _:trans_fun(_))

if __name__ == '__main__':
    import pyspark as ps
    conf = ps.SparkConf().setAppName("CF").setMaster('local')#setMaster("spark://spark31:7077")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.cores.max","100") 
    conf.set("spark.driver.maxResultSize","5g")
    sc = ps.SparkContext(conf=conf)
    
    mirdd = MatrixInRDD(10000,10000)
    mirdd.load(sc,'file:///home/CF/python/pair_matrix.txt','pair','int',10)
    print mirdd.rdd.take(10)

    
        

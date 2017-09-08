# -*- coding: utf-8 -*-
"""
Created on Fri Sep 08 10:40:48 2017

@author: ligong

@description:这是ROWMatrixInRDD
"""
from MatrixInRDD import MatrixInRDD
class RowMatrixInRDD(MatrixInRDD):
    def __init__(self,rows,cols):
        """
        初始化
        rows,cols矩阵的行和列
        """
        super(RowMatrixInRDD,self).__init__(rows,cols)

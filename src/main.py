from pyspark import SparkContext, SparkConf
from pyspark.sql.context import SQLContext
import os
from task1 import loadDataAndPrintNumRows
from task2 import task2
from task3 import task3

def main():
    conf = SparkConf().setAppName("TDT4305 Assignment 1").setMaster("local")
    sc = SparkContext(conf=conf)
    #sc.addPyFile("../TDT4305-BigData/graphframes-0.8.1-spark3.0-s_2.12.jar")
    sqlContext = SQLContext(sc)
    # Task 1 - Load data and print number of rows
    posts, comments, users, badges = loadDataAndPrintNumRows(sc)

    # Task 2
    task2(posts, comments, users, badges)

    # Task 3
    task3(posts, comments, users, badges, sqlContext, sc)


if __name__ == "__main__":
    main()


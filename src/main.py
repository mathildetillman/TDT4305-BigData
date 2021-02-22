from pyspark import SparkContext, SparkConf

from task1 import loadDataAndPrintNumRows
from task2 import task2

def main():
    conf = SparkConf().setAppName("TDT4305 Assignment 1").setMaster("local")
    sc = SparkContext(conf=conf)

    # Task 1 - Load data and print number of rows
    posts, comments, users, badges = loadDataAndPrintNumRows(sc)

    # Task 2
    task2(posts, comments)


if __name__ == "__main__":
    main()


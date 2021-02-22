from pyspark import SparkContext, SparkConf

from task1 import loadDataAndPrintNumRows

def main():
    conf = SparkConf().setAppName("TDT4305 Assignment 1").setMaster("local")
    sc = SparkContext(conf=conf)

    # Task 1 - Load data and print number of rows
    posts, comments, users, badges = loadDataAndPrintNumRows(sc)


if __name__ == "__main__":
    main()


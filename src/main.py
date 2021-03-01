from pyspark import SparkContext, SparkConf

from task1 import task1
from task2 import task2

def main():
    conf = SparkConf().setAppName("TDT4305 Assignment 1").setMaster("local")
    sc = SparkContext(conf=conf)

    # Task 1
    posts, comments, users, badges = task1(sc)

    # Task 2
    task2(posts, comments, users, badges)


if __name__ == "__main__":
    main()


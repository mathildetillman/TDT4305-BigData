from pyspark import SparkContext, SparkConf
import base64
import sys

# command:
# spark-submit main.py C:\sparkData 14

INPUT_DATA_PATH = sys.argv[1]
POST_ID = sys.argv[2]

def main():
    conf = SparkConf().setAppName("TDT4305 Assignment 2").setMaster("local")
    sc = SparkContext(conf=conf)

    # Load the data into an RDD
    posts = sc.textFile(INPUT_DATA_PATH + '/posts.csv.gz')

    # Split on "\t"
    posts = posts.map(lambda line: line.split("\t"))

    # Extract post with id from input, select body and decode
    post = posts.filter(lambda x: x[0] == POST_ID).map(lambda x: base64.b64decode(x[5]))

    # Print post
    print(post.first(()))


if __name__ == "__main__":
    main()


from itertools import islice
import graphframes
from operator import add
from pyspark.sql.types import (IntegerType, ShortType, StringType, StructType, StructField, TimestampType)
import sys

INPUT_DATA_PATH = sys.argv[1]

def task3(posts, comments, users, badges, sqlContext, sc):
    posts = posts.map(lambda x: x.split("\t"))
    
    posts = posts.mapPartitionsWithIndex(
        lambda idx, it: islice(it, 1, None) if idx == 0 else it
    )

    
    comments = comments.mapPartitionsWithIndex(
        lambda idx, it: islice(it, 1, None) if idx == 0 else it
    )

    users = users.map(lambda x: x.split("\t"))
    
    users = users.mapPartitionsWithIndex(
        lambda idx, it: islice(it, 1, None) if idx == 0 else it
    )

    # Task 3.1

    v = createNodes(posts, users, sqlContext)
    e = createEdges(posts, comments, sqlContext, sc, 1)
    gf = graphframes.GraphFrame(v, e)

    print("Task 3.1")
    gf.edges.show()

    print("Task 3.2")
    df = convertEdgesToDF(createEdges(posts, comments, sqlContext, sc, 0))
    df.show()

    print("Task 3.3")
    print(f"Top ten users who wrote the most comments: {getMostComments(comments, sqlContext)}")



def createNodes(posts, users, sqlContext):
    
    # Filter out community posts
    users = users.filter(lambda x: x[0] != -1)
    
    # Only get Ids and display names
    users = users.map(lambda x: (x[0], x[3]))

    # Get ownerId of posts 
    posts = posts.map(lambda x: (x[6], 0))
    
    # Join on Ids to get only users who have posts
    combo = users.join(posts)
    
    # Extract only name and id
    combo = combo.map(lambda x: (x[0], x[1][0]))
    
    # Remove duplicates
    combo = combo.distinct()

    return sqlContext.createDataFrame(combo, ["id", "displayname"])

def createEdges(posts, comments, sqlContext, sc, type):

    comments = comments.map(lambda x: x.split("\t"))

    # (postId commented on by userID)
    filteredComments = comments.map(lambda x: (x[0], x[4]))

    # (postId posted by ownerID)
    filteredPosts = posts.map(lambda x: (x[0],x[6]))

    # Join the two RDDs in the format (postId, (commenterId, postOwnerId))
    combo = filteredComments.join(filteredPosts)

    # Extract only (commenterId and postOwnerId)
    combo = combo.map(lambda x: (x[1]) )
    withWeights = addWeight(combo, sc)
    if (type == 1):
        return sqlContext.createDataFrame(withWeights, ["src", "dst", "weight"])
    else:
        return withWeights
def addWeight(combo, sc):
    # Create nested tuple with 1 as the second item to allow for reduceByKey to work with addition
    combo = combo.map(lambda x: (x, 1))
    
    # Reduce by key to get the weights for each tuple
    combo = list(combo.reduceByKey(add).collect())
    
    # Get the list back as an RDD
    rdd = sc.parallelize(combo)
    
    # Spread the nested tuple to create a triple that is (commenterId, postOwnerId, weight)
    combo = rdd.map(lambda x: (*x[0], x[1]))
    return combo

def convertEdgesToDF(rdd):
    columns = ["CommenterId", "PostOwnerId", "Weight"]
    return rdd.toDF(columns)

def getMostComments(comments, sqlContext):
    schema = StructType([
        StructField("PostId", IntegerType()),
        StructField("Score", IntegerType()),
        StructField("Text", StringType()),
        StructField("CreationDate", TimestampType()),
        StructField("UserId", IntegerType()),
    ])
    comments = sqlContext.read.option("delimiter", "\t").csv( INPUT_DATA_PATH + "/comments.csv.gz", schema=schema, header=True)
    unique = comments.groupBy("UserId").count().sort("count", ascending=False)
    #uList = sorted(list(unique))
    #print(uList[0])
    print(unique.take(10))
    print("lmao")

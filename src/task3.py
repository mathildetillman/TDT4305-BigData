from itertools import islice
from operator import add
from pyspark.sql.types import (DataType, IntegerType, StringType, StructType, StructField, TimestampType)
import sys

INPUT_DATA_PATH = sys.argv[1]

def task3(posts, comments, users, sqlContext, sc):
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
    print("Task 3.1")
    print(f"Graph with weights: {createEdges(posts, comments, sqlContext, sc, 0).take(3)}")

    # Task 3.2
    print("Task 3.2")
    df = convertEdgesToDF(createEdges(posts, comments, sqlContext, sc, 0))
    df.show()

    # Task 3.3
    print("Task 3.3")
    print(f"Top ten users who wrote the most comments:")
    getMostComments(sqlContext)

    # Task 3.4
    print("Task 3.4")
    print("UserIds of people who have the most comments on their posts:")
    getUsersWithMostCommentsOnPosts(sqlContext)


    # Task 3.5
    print("Task 3.5")
    saveToCSV(posts, comments, sqlContext, sc)
    print("Saved to edges.csv")


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

def getMostComments(sqlContext):
    schema = StructType([
        StructField("PostId", IntegerType()),
        StructField("Score", IntegerType()),
        StructField("Text", StringType()),
        StructField("CreationDate", TimestampType()),
        StructField("UserId", IntegerType()),
    ])
    comments = sqlContext.read.option("delimiter", "\t").csv( INPUT_DATA_PATH + "/comments.csv.gz", schema=schema, header=True)
    unique = comments.groupBy("UserId").count().sort("count", ascending=False)
    unique.show(10)


def getUsersWithMostCommentsOnPosts(sqlContext):

    postsSchema = StructType([
        StructField("Id", IntegerType()),
        StructField("PostTypeId", IntegerType()),
        StructField("CreationDate", TimestampType()),
        StructField("Score", IntegerType()),
        StructField("ViewCount", IntegerType()),
        StructField("Body", StringType()),
        StructField("OwnerUserId", IntegerType()),
        StructField("LastActivityDate", TimestampType()),
        StructField("Title", StringType()),
        StructField("Tags", StringType()),
        StructField("AnswerCount", IntegerType()),
        StructField("CommentCount", IntegerType()),
        StructField("FavoriteCount", IntegerType()),
        StructField("ClosedDate", TimestampType()),
    ])
    posts = sqlContext.read.option("delimiter", "\t").csv( INPUT_DATA_PATH + "/posts.csv.gz", schema=postsSchema, header=True)

    usersSchema = StructType([
        StructField("Id", IntegerType()),
        StructField("Reputation", IntegerType()),
        StructField("CreationDate", TimestampType()),
        StructField("DisplayName", StringType()),
        StructField("LastAccessDate", TimestampType()),
        StructField("AboutMe", StringType()),
        StructField("Views", IntegerType()),
        StructField("UpVotes", IntegerType()),
        StructField("DownVotes", IntegerType()),
    ])
    
    users = sqlContext.read.option("delimiter", "\t").csv( INPUT_DATA_PATH + "/users.csv.gz", schema=usersSchema, header=True)

    combined = users.join(posts, users["Id"] == posts["OwnerUserId"])
    combined.select("DisplayName", "CommentCount").groupBy("DisplayName").sum().sort("sum(CommentCount)", ascending=False).show(10)

def saveToCSV(posts, comments, sqlContext, sc):
    df = createEdges(posts, comments, sqlContext, sc, 1)
    df.coalesce(1).write.csv(INPUT_DATA_PATH + "edges.csv", header=True, sep="\t")
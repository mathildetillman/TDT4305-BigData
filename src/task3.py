from itertools import islice
import graphframes
from operator import add
def task3(posts, comments, users, badges, sqlContext, sc):
    posts = posts.map(lambda x: x.split("\t"))
    
    posts = posts.mapPartitionsWithIndex(
        lambda idx, it: islice(it, 1, None) if idx == 0 else it
    )

    comments = comments.map(lambda x: x.split("\t"))
    
    comments = comments.mapPartitionsWithIndex(
        lambda idx, it: islice(it, 1, None) if idx == 0 else it
    )

    users = users.map(lambda x: x.split("\t"))
    
    users = users.mapPartitionsWithIndex(
        lambda idx, it: islice(it, 1, None) if idx == 0 else it
    )

    # Task 3.1

    v = createNodes(posts, users, sqlContext)
    e = createEdges(posts, comments, sqlContext, sc)
    gf = graphframes.GraphFrame(v, e)

    print("Task 3.1")
    gf.edges.show()



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

def createEdges(posts, comments, sqlContext, sc):


    # (postId commented on by userID)
    filteredComments = comments.map(lambda x: (x[0], x[4]))

    # (postId posted by ownerID)
    filteredPosts = posts.map(lambda x: (x[0],x[6]))

    # Join the two RDDs in the format (postId, (commenterId, postOwnerId))
    combo = filteredComments.join(filteredPosts)

    # Extract only (commenterId and postOwnerId)
    combo = combo.map(lambda x: (x[1]) )
    withWeights = addWeight(combo, sc)
    return sqlContext.createDataFrame(withWeights, ["src", "dst", "weight"])

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

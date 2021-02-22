import base64
from itertools import islice

def getAverageLength(rdd, col):
    # Remove header
    rdd = rdd.mapPartitionsWithIndex(
        lambda idx, it: islice(it, 1, None) if idx == 0 else it
    )

    # Find number of rows
    numRows = rdd.count()

    # Select correct column and decode base64
    rdd = rdd.map(lambda line: len((base64.b64decode(line[col]))))

    # Find average length
    return (rdd.reduce(lambda a, b: a + b)) / numRows

# TASK 2
def task2(posts, comments):

    # 2.1
    # Find the average length of the questions, answers, and comments

    # Split on \t
    posts = posts.map(lambda line: line.split("\t"))
    
    comments = comments.map(lambda line: line.split("\t"))

    # Split posts into questions and answers
    questions = posts.filter(lambda line: line[1] == "1")
    answers = posts.filter(lambda line: line[1] == "2")

    # Find average length
    print(f"Average length of comments: {getAverageLength(comments, 2)}")
    print(f"Average length of questions: {getAverageLength(questions, 5)}")
    print(f"Average length of answers: {getAverageLength(answers, 5)}")



    # 2.3
    # Find the users who wrote the greatest number of answers and questions
    
    print(f"User with most questions: {greatestNumberofQuestionsAndAnswers(posts, '1')[0]} with {greatestNumberofQuestionsAndAnswers(posts, '1')[1]} questions")
    print(f"User with most answers: {greatestNumberofQuestionsAndAnswers(posts, '2')[0]} with {greatestNumberofQuestionsAndAnswers(posts, '2')[1]} answers")

def greatestNumberofQuestionsAndAnswers(rdd, type):
    
    # Remove headers
    rdd = rdd.mapPartitionsWithIndex(
        lambda idx, it: islice(it, 1, None) if idx == 0 else it
    )

    # Filter out posts by community
    posts = rdd.filter(lambda line: line[6] != -1)

    # Only take questions or answers
    filteredPosts = posts.filter(lambda x: x[1] == type)

    # Filter out posts where the ownerId is null
    ownerIds = filteredPosts.filter(lambda x: x[6] != "NULL")
    # Create tuple of ownerId and 0    
    ownerIds = ownerIds.map(lambda x: (x[6], 0))

    # Count by ownerId and sort the list
    filteredPostsPerUser = sorted(ownerIds.countByKey().items(), key=takeSecond)

    # Return the last value as that is the one with the most posts
    return filteredPostsPerUser[-1]


def takeSecond(tuple):
    return tuple[1]
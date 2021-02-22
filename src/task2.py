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




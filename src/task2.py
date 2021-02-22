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
def task2(posts, comments, users):

    # 2.1
    # Find the average length of the questions, answers, and comments

    # Split on \t
    posts = posts.map(lambda line: line.split("\t"))
    comments = comments.map(lambda line: line.split("\t"))
    users = users.map(lambda line: line.split("\t"))

    # Split posts into questions and answers
    questions = posts.filter(lambda line: line[1] == "1")
    answers = posts.filter(lambda line: line[1] == "2")

    # Find average length
    print("Task 2.1")
    print(f"Average length of comments: {getAverageLength(comments, 2)}")
    print(f"Average length of questions: {getAverageLength(questions, 5)}")
    print(f"Average length of answers: {getAverageLength(answers, 5)}")

    # 2.2
    # Find the dates when the first and the last questions were asked and who posted them

    # Select column for dateTime and userId
    questionsDate = questions.map(lambda line: (line[2], line[6]))

    # Find first and last question
    firstQuestion = questionsDate.min()
    lastQuestion = questionsDate.max()

    # Select column for userId and displayName
    userNames = users.map(lambda line: (line[0], line[3]))

    # Find users who posted first and last question
    userFirst = userNames.filter(lambda user: user[0] == firstQuestion[1] )
    userLast = userNames.filter(lambda user: user[0] == lastQuestion[1])

    print("Task 2.2")
    print(f"The first question was asked by {userFirst.first()[1]} on {firstQuestion[0]}")
    print(f"The last question was asked by {userLast.first()[1]} on {lastQuestion[0]}")






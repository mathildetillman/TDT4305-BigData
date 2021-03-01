import base64
from itertools import islice
import math


# Split on "\t" and remove header of RDD
def splitAndRemoveHeader(rdd):
    rdd = rdd.map(lambda line: line.split("\t"))
    return rdd.mapPartitionsWithIndex(
        lambda idx, it: islice(it, 1, None) if idx == 0 else it
    )


# 2.1
def getAverageLength(rdd, col):
    # Find number of rows
    numRows = rdd.count()

    # Select correct column and decode base64
    rdd = rdd.map(lambda line: len((base64.b64decode(line[col]))))

    # Find average length
    return (rdd.reduce(lambda a, b: a + b)) / numRows


# 2.2
def firstAndLastQuestion(users, questions):
    # Create tuples for dateTime and userId
    questionsDate = questions.map(lambda line: (line[2], line[6]))

    # Find first and last question
    firstQuestion = questionsDate.min()
    lastQuestion = questionsDate.max()

    # Create tuples for userId and displayName
    userNames = users.map(lambda line: (line[0], line[3]))

    # Find users who posted first and last question
    userFirst = userNames.filter(lambda user: user[0] == firstQuestion[1] )
    userLast = userNames.filter(lambda user: user[0] == lastQuestion[1])

    return userFirst, userLast, firstQuestion, lastQuestion


# 2.3
def greatestNumberofQuestionsAndAnswers(posts, postType):
    # Filter out posts where userId is -1 or "NULL"
    posts = posts.filter(lambda line: (line[6] != -1) and (line[6] != "NULL"))

    # Select questions or answers based on function argument
    posts = posts.filter(lambda x: x[1] == postType)

    # Create tuple of ownerId and 0    
    ownerIds = posts.map(lambda x: (x[6], 0))

    # Count by ownerId and sort the list by amount
    filteredPostsPerUser = sorted(ownerIds.countByKey().items(), key=lambda x: x[1])

    # Return the last value as that is the one with the most posts
    return filteredPostsPerUser[-1]


# 2.4
def lessThanThreeBadges(badges):
    # Create tuples of UserId and 0    
    userIds = badges.map(lambda x: (x[0], 0))

    # Create tuples with UserId and amount of badges
    badgesPerUser = sorted(userIds.countByKey().items(), key=lambda x: x[1])

    # Filter the tuples on badge amount
    filteredBadgesPerUser = list(filter(lambda x: x[1] < 3, badgesPerUser))

    return len(filteredBadgesPerUser)


# 2.5
def pearsonsR(users):
    # Make rdd for upvotes and find average
    users_upvotes = users.map(lambda x: int(x[7]))
    average_upvotes = users_upvotes.sum()/users_upvotes.count()

    # Make rdd for downvotesand find average
    users_downvotes = users.map(lambda x: int(x[8]))
    average_downvotes = users_downvotes.sum() / users_downvotes.count()

    # Find Pearson numerator
    numerator = users.map(lambda x: (int(x[7]) - average_upvotes) * (int(x[8]) - average_downvotes)).sum()

    # Find Pearson denominator
    denominator = (math.sqrt(users.map(lambda x: (int(x[7]) - average_upvotes) ** 2  ).sum())) * (math.sqrt(users.map(lambda x: (int(x[8]) - average_downvotes) ** 2).sum()))

    # Return Pearsons r
    return numerator/denominator


# 2.6
def entropy(comments):
    # Find num of rows
    num_rows = comments.count()

    # Make tuple of (id, commentcount/numRows)
    px = comments.map(lambda x: (x[4], 1)).reduceByKey(lambda a, b: a + b).map(lambda x: (x[0], x[1]/num_rows))

    # Return entropy
    return -(px.map(lambda x: x[1] * math.log2(x[1])).sum())


# TASK 2
def task2(posts, comments, users, badges):

    # Split on "\t" and remove header of RDD
    posts = splitAndRemoveHeader(posts)
    comments = splitAndRemoveHeader(comments)
    users = splitAndRemoveHeader(users)
    badges = splitAndRemoveHeader(badges)

    # Split posts into questions and answers
    questions = posts.filter(lambda line: line[1] == "1")
    answers = posts.filter(lambda line: line[1] == "2")

    # 2.1
    # Find the average length of the questions, answers, and comments
    print("Task 2.1")
    print(f"Average length of comments: {getAverageLength(comments, 2)}")
    print(f"Average length of questions: {getAverageLength(questions, 5)}")
    print(f"Average length of answers: {getAverageLength(answers, 5)}")
    print("")

    # 2.2
    # Find the dates when the first and the last questions were asked and who posted them
    userFirst, userLast, firstQuestion, lastQuestion = firstAndLastQuestion(users, questions)

    print("Task 2.2")
    print(f"The first question was asked by {userFirst.first()[1]} on {firstQuestion[0]}")
    print(f"The last question was asked by {userLast.first()[1]} on {lastQuestion[0]}")
    print("")

    # 2.3
    # Find the users who wrote the greatest number of answers and questions
    userMostQuestions = greatestNumberofQuestionsAndAnswers(posts, '1')
    userMostAnswers = greatestNumberofQuestionsAndAnswers(posts, '2')

    print("Task 2.3")
    print(f"Id of user who wrote the greatest number of questions: {userMostQuestions[0]}. Number of questions: {userMostQuestions[1]}")
    print(f"Id of user who wrote the greatest number of answers: {userMostAnswers[0]}. Number of answers: {userMostAnswers[1]}")
    print("")

    # 2.4
    # Calculate the number of users who received less than three badges
    print("Task 2.4")
    print(f"Number of users who received less than 3 badges: {lessThanThreeBadges(badges)}")
    print("")

    # 2.5
    # Calculate Pearson's correlation coefficient between number of upvotes and downvotes cast by a user
    print("Task 2.5")
    print(f"Pearson's correlation coefficient between number of upvotes and downvotes cast by a user: {pearsonsR(users)}")
    print("")

    # 2.6
    # Calculate the entropy of id of users who wrote one or more comment
    print("Task 2.6")
    print(f"Entropy of id of users who wrote one or more comment : {entropy(comments)}")
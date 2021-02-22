import base64
from itertools import islice

# TASK 2
# 2.1 Find the average length of the questions, answers, and comments in character
def task2(sc, posts, comments, users, badges):

    commentsCount = comments.count()
    # Remove header, split on \t, and decode with base64
    comments = comments.mapPartitionsWithIndex(
      lambda idx, it: islice(it, 1, None) if idx == 0 else it
    )
    comments = comments.map(lambda line: line.split("\t"))
    commentsLength = comments.map(lambda line: len((base64.b64decode(line[2]))))

    avgLength = (commentsLength.reduce(lambda a, b : a + b))/commentsCount
    print(avgLength)

import sys

INPUT_DATA_PATH = sys.argv[1]

# TASK 1
def loadDataAndPrintNumRows(sc):
    # 1.1 - 1.4
    # Load the data into RDDs
    posts = sc.textFile(INPUT_DATA_PATH + '/posts.csv.gz')
    comments = sc.textFile(INPUT_DATA_PATH + '/comments.csv.gz')
    users = sc.textFile(INPUT_DATA_PATH +'/users.csv.gz')
    badges = sc.textFile(INPUT_DATA_PATH +'/badges.csv.gz')

    # 1.5
    # Print number of rows in each RDD
    print("Task 1")
    print(f"Number of rows in posts: {posts.count()}")
    print(f"Number of rows in comments: {comments.count()}")
    print(f"Number of rows in users: {users.count()}")
    print(f"Number of rows in badges: {badges.count()}")

    return posts,comments, users, badges

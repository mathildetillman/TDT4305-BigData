import os

#os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages graphframes:graphframes:0.8.1-spark3.0-s_2.12 pyspark-shell"

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession.builder.master("local[*]").appName("HelloWorldApp").getOrCreate()
sc = spark.sparkContext
sc.addPyFile('https://github.com/graphframes/graphframes/archive/b3b97cf1dac9e7be7806a938c8de406e5c09f431.zip')
sqlContext = SQLContext(sc)

## the rest of this code (down below) comes from: https://graphframes.github.io/graphframes/docs/_site/quick-start.html#getting-started-with-apache-spark-and-spark-packages

# Create a Vertex DataFrame with unique ID column "id"
v = sqlContext.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
], ["id", "name", "age"])

# Create an Edge DataFrame with "src" and "dst" columns
e = sqlContext.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
], ["src", "dst", "relationship"])

# Create a GraphFrame
from graphframes import *
g = GraphFrame(v, e)

# Query: Get in-degree of each vertex.
g.inDegrees.show()

# Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

# Run PageRank algorithm, and show results.
results = g.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").show()
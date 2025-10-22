from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from pysparky_vector import vector_search


spark = SparkSession.builder.appName("VectorDBMock").getOrCreate()


data = [
    ("A", [0.1, 0.2, 0.3]),
    ("B", [0.4, 0.5, 0.6]),
    ("C", [0.2, 0.1, 0.0])
]
df = spark.createDataFrame(data, ["id", "vector"])

vector_search(df, [0.2, 0.1, 0.2], metric="cosine", top_k=1).show()
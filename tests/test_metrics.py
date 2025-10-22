from pyspark.sql import functions as F
from pysparky_vector.metrics.similarity import cosine_sim, dot_expr
from pysparky_vector.metrics.distance import dist_expr, manhattan_expr


def make_df(spark):
    data = [
        ("A", [1.0, 0.0]),
        ("B", [0.0, 1.0]),
        ("C", [1.0, 1.0]),
    ]
    return spark.createDataFrame(data, ["id", "vector"])


def test_cosine_similarity(spark):
    df = make_df(spark)
    query = F.array(F.lit(1.0), F.lit(0.0))
    df = df.withColumn("cos", cosine_sim(F.col("vector"), query))
    results = {r["id"]: round(r["cos"], 2) for r in df.collect()}
    assert results["A"] == 1.0  # identical
    assert results["B"] == 0.0  # orthogonal
    assert 0.7 < results["C"] < 0.8  # 1/sqrt(2)


def test_euclidean_distance(spark):
    df = make_df(spark)
    query = F.array(F.lit(1.0), F.lit(0.0))
    df = df.withColumn("dist", dist_expr(F.col("vector"), query))
    results = {r["id"]: round(r["dist"], 2) for r in df.collect()}
    assert results["A"] == 0.0
    assert results["B"] == 1.41
    assert results["C"] == 1.0


def test_manhattan_distance(spark):
    df = make_df(spark)
    query = F.array(F.lit(1.0), F.lit(0.0))
    df = df.withColumn("manhattan", manhattan_expr(F.col("vector"), query))
    results = {r["id"]: r["manhattan"] for r in df.collect()}
    assert results["A"] == 0.0
    assert results["B"] == 2.0
    assert results["C"] == 1.0


def test_dot_expr(spark):
    df = make_df(spark)
    query = F.array(F.lit(1.0), F.lit(0.0))
    df = df.withColumn("dot", dot_expr(F.col("vector"), query))
    results = {r["id"]: r["dot"] for r in df.collect()}
    assert results["A"] == 1.0
    assert results["B"] == 0.0
    assert results["C"] == 1.0
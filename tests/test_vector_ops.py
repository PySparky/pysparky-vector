from pysparky_vector.core.vector_ops import vector_distance, vector_search
from pysparky_vector.metrics.similarity import cosine_sim
from pysparky_vector.metrics.distance import dist_expr


def make_df(spark):
    data = [
        ("A", [1.0, 0.0]),
        ("B", [0.0, 1.0]),
        ("C", [1.0, 1.0]),
    ]
    return spark.createDataFrame(data, ["id", "vector"])


def test_vector_distance_cosine(spark):
    df = make_df(spark)
    result = vector_distance(df, [1.0, 0.0], cosine_sim, alias="score", desc_order=True)
    rows = result.collect()
    assert rows[0]["id"] == "A"   # highest cosine sim first
    assert "score" in result.columns


def test_vector_distance_euclidean(spark):
    df = make_df(spark)
    result = vector_distance(df, [1.0, 0.0], dist_expr, alias="dist", desc_order=False)
    rows = result.collect()
    assert rows[0]["id"] == "A"   # smallest distance first


def test_vector_search_registry(spark):
    df = make_df(spark)
    result = vector_search(df, [1.0, 0.0], metric="cosine", top_k=2)
    rows = result.collect()
    assert len(rows) == 2
    assert "cosine_score" in result.columns
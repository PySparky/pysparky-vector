from pyspark.sql import functions as F
from ..metrics.registry import METRICS

def vector_distance(df, query_vector, metric_fn, vector_col="vector", top_k=None, alias=None, desc_order=False):
    query_array = F.array([F.lit(x) for x in query_vector])
    metric_col = metric_fn(F.col(vector_col), query_array)
    alias = alias or metric_fn.__name__
    df = df.withColumn(alias, metric_col)
    order_col = F.desc(alias) if desc_order else F.col(alias)
    df = df.orderBy(order_col)
    if top_k:
        df = df.limit(top_k)
    return df


def vector_search(df, query_vector, metric="euclidean", vector_col="vector", top_k=None, alias=None):
    """User-friendly wrapper that looks up the metric from registry."""
    config = METRICS.get(metric.lower())
    if not config:
        raise ValueError(f"Unknown metric: {metric}")
    fn, desc = config["fn"], config["desc_order"]
    alias = alias or f"{metric}_score"
    return vector_distance(df, query_vector, fn, vector_col, top_k, alias, desc)
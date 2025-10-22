from pyspark.sql import functions as F

def cosine_sim(col_vector, query_array):
    dot = F.aggregate(
        F.zip_with(col_vector, query_array, lambda x, y: x * y),
        F.lit(0.0),
        lambda acc, x: acc + x
    )
    norm1 = F.sqrt(F.aggregate(F.transform(col_vector, lambda x: x * x), F.lit(0.0), lambda acc, x: acc + x))
    norm2 = F.sqrt(F.aggregate(F.transform(query_array, lambda x: x * x), F.lit(0.0), lambda acc, x: acc + x))
    return dot / (norm1 * norm2)


def dot_expr(col_vector, query_array):
    return F.aggregate(
        F.zip_with(col_vector, query_array, lambda x, y: x * y),
        F.lit(0.0),
        lambda acc, x: acc + x
    )
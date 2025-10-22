from pyspark.sql import functions as F

def dist_expr(col_vector, query_array):
    return F.sqrt(
        F.aggregate(
            F.zip_with(col_vector, query_array, lambda x, y: (x - y) ** 2),
            F.lit(0.0),
            lambda acc, x: acc + x
        )
    )

def manhattan_expr(col_vector, query_array):
    return F.aggregate(
        F.zip_with(col_vector, query_array, lambda x, y: F.abs(x - y)),
        F.lit(0.0),
        lambda acc, x: acc + x
    )
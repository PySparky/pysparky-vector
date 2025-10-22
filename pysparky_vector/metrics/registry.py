from .similarity import cosine_sim, dot_expr
from .distance import dist_expr, manhattan_expr

METRICS = {
    "cosine": {"fn": cosine_sim, "desc_order": True},
    "dot": {"fn": dot_expr, "desc_order": True},
    "euclidean": {"fn": dist_expr, "desc_order": False},
    "manhattan": {"fn": manhattan_expr, "desc_order": False},
}
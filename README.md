# 🧮 pyspark-vector

A lightweight, Spark-native library for computing **vector similarity and distance** directly in **PySpark DataFrames** — no UDFs, no Pandas overhead.

It lets you perform operations like cosine similarity, Euclidean distance, or dot product directly using Spark SQL column expressions, just like a mini vector database.

---

## 🚀 Features

✅ Native Spark column expressions (`zip_with`, `aggregate`, `transform`)  
✅ Fast — no Python UDFs or Arrow overhead  
✅ Extensible metric registry (`cosine`, `euclidean`, `dot`, `manhattan`)  
✅ Functional API (`vector_distance`) and friendly wrapper (`vector_search`)  
✅ Easy to integrate with DataFrames, SQL, or Dataiku flows  
✅ Fully unit-tested with `pytest`

---

## 📦 Installation

Clone the repo and install in **editable** mode (recommended for development):

```bash
git clone https://github.com/yourname/pyspark-vector.git
cd pyspark-vector
pip install -e .[dev]
```

### Requirements
- Python ≥ 3.9  
- PySpark ≥ 3.3.0  
- pytest (for development)

---

## 🧱 Project Structure

```
pyspark_vector/
├── core/
│   └── vector_ops.py          # main logic (vector_distance, vector_search)
├── metrics/
│   ├── similarity.py          # cosine, dot
│   ├── distance.py            # euclidean, manhattan
│   └── registry.py            # metric registry
└── tests/
    ├── conftest.py
    ├── test_metrics.py
    └── test_vector_ops.py
```

---

## 🧠 Quick Start

```python
from pyspark.sql import SparkSession
from pyspark_vector import vector_search

spark = SparkSession.builder.appName("VectorDemo").getOrCreate()

data = [
    ("A", [0.1, 0.2, 0.3]),
    ("B", [0.4, 0.5, 0.6]),
    ("C", [0.2, 0.1, 0.0])
]
df = spark.createDataFrame(data, ["id", "vector"])

query = [0.2, 0.1, 0.2]

# Cosine similarity (higher = closer)
df_cos = vector_search(df, query, metric="cosine", top_k=2)
df_cos.show(truncate=False)

# Euclidean distance (lower = closer)
df_euc = vector_search(df, query, metric="euclidean")
df_euc.show(truncate=False)
```

---

## ⚙️ API Overview

### `vector_distance(df, query_vector, metric_fn, vector_col="vector", top_k=None, alias=None, desc_order=False)`
Compute a metric using any Spark column expression builder.

**Arguments:**

| Parameter | Description |
|------------|--------------|
| `df` | Spark DataFrame containing a vector column |
| `query_vector` | Python list or tuple (e.g. `[0.2, 0.1, 0.2]`) |
| `metric_fn` | Function that builds a Spark column expression |
| `vector_col` | Name of the array column (default `"vector"`) |
| `top_k` | Optional integer — return only top K results |
| `alias` | Column name for result |
| `desc_order` | True if higher value = closer (e.g. cosine) |

---

### `vector_search(df, query_vector, metric="euclidean", vector_col="vector", top_k=None, alias=None)`
User-friendly wrapper that looks up the metric from the internal registry.

**Supported metrics:**

| Metric | Description | Order |
|---------|--------------|-------|
| `euclidean` | L2 distance | ascending |
| `manhattan` | L1 distance | ascending |
| `cosine` | Cosine similarity | descending |
| `dot` | Inner product | descending |

---

## 🧪 Testing

Run all unit tests:

```bash
pytest -v
```

Example output:
```
tests/test_metrics.py::test_cosine_similarity PASSED
tests/test_vector_ops.py::test_vector_search_registry PASSED
```

---

## 📘 Development Tips

| Command | Description |
|----------|--------------|
| `pip install -e .[dev]` | Editable install for live code updates |
| `pytest -v` | Run tests |
| `black pyspark_vector` | Format code |
| `isort pyspark_vector` | Sort imports |

To verify import path:
```python
import pyspark_vector
print(pyspark_vector.__file__)
```

---

## 🪶 Example: Add a New Metric

1. Create `pyspark_vector/metrics/my_metric.py`
2. Implement a new Spark expression:
   ```python
   def chebyshev_expr(col_vector, query_array):
       from pyspark.sql import functions as F
       return F.aggregate(
           F.zip_with(col_vector, query_array, lambda x, y: F.abs(x - y)),
           F.lit(0.0),
           lambda acc, x: F.greatest(acc, x)
       )
   ```
3. Register it in `metrics/registry.py`:
   ```python
   from .my_metric import chebyshev_expr
   METRICS["chebyshev"] = {"fn": chebyshev_expr, "desc_order": False}
   ```
4. Use it:
   ```python
   vector_search(df, query, metric="chebyshev").show()
   ```

---

## 🪪 License
MIT License © 2025 Cenz Wong

---

## ❤️ Acknowledgements
Inspired by the performance and expressiveness of Spark SQL, this project aims to bring **vector-DB-like semantics** into the **PySpark ecosystem**, making it easier to run large-scale similarity searches without leaving Spark.

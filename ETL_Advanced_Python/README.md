# 📚 Advanced Python for ETL with Pandas

A comprehensive, self-contained reference library of advanced Python topics essential for building **ETL (Extract, Transform, Load)** pipelines with Pandas. Each file contains:

✅ Explanation of the concept  
✅ Must-remember points  
✅ Code examples (ETL-focused)  
✅ ETL use cases table  
✅ Common pitfalls with fixes

---

## 📁 Contents

### 🐍 Core Python (Advanced)

| File                                                                         | Topic                                                       |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------- |
| [01_generators_iterators.md](01_generators_iterators.md)                     | Generators, Iterators, `yield`, `itertools`                 |
| [02_decorators.md](02_decorators.md)                                         | Decorators, `@wraps`, `@retry`, `@timer`, `@lru_cache`      |
| [03_context_managers.md](03_context_managers.md)                             | `with`, `contextmanager`, `ExitStack`                       |
| [04_comprehensions.md](04_comprehensions.md)                                 | List, Dict, Set comprehensions, Generator expressions       |
| [05_functional_programming.md](05_functional_programming.md)                 | `lambda`, `map`, `filter`, `reduce`, `partial`, composition |
| [06_exception_handling.md](06_exception_handling.md)                         | Try/except, custom exceptions, retry, `traceback`           |
| [07_file_io.md](07_file_io.md)                                               | CSV, JSON, `pathlib`, `shutil`, Parquet basics              |
| [08_regular_expressions.md](08_regular_expressions.md)                       | `re` module, validation, extraction, Pandas `.str` regex    |
| [09_multithreading_multiprocessing.md](09_multithreading_multiprocessing.md) | `ThreadPoolExecutor`, `ProcessPoolExecutor`, `Queue`, GIL   |
| [10_logging.md](10_logging.md)                                               | `logging`, Handlers, JSON logging, ETL step logging         |
| [11_type_hints_dataclasses.md](11_type_hints_dataclasses.md)                 | Type hints, `@dataclass`, `frozen=True`, `asdict()`         |
| [12_database_connectivity.md](12_database_connectivity.md)                   | `psycopg2`, `SQLAlchemy`, `pd.read_sql`, upsert pattern     |

---

### 🐼 Pandas (Advanced ETL)

| File                                                                           | Topic                                                                   |
| ------------------------------------------------------------------------------ | ----------------------------------------------------------------------- |
| [13_pandas_advanced_groupby_agg.md](13_pandas_advanced_groupby_agg.md)         | `groupby`, `agg`, named aggregations, `transform`, `pd.Grouper`         |
| [14_pandas_merge_join.md](14_pandas_merge_join.md)                             | `merge`, `join`, `concat`, star schema joins, `indicator`               |
| [15_pandas_apply_map_transform.md](15_pandas_apply_map_transform.md)           | `apply`, `map`, `transform`, `np.where`, `np.select`                    |
| [16_pandas_pivot_melt_reshape.md](16_pandas_pivot_melt_reshape.md)             | `pivot_table`, `melt`, `stack`, `unstack`, `crosstab`                   |
| [17_pandas_datetime_handling.md](17_pandas_datetime_handling.md)               | `pd.to_datetime`, `.dt` accessor, timezones, `resample`, date dimension |
| [18_pandas_string_operations.md](18_pandas_string_operations.md)               | `.str` accessor, validation, extraction, fixed-width parsing            |
| [19_pandas_performance_optimization.md](19_pandas_performance_optimization.md) | Memory optimization, vectorization, chunking, `query()`                 |
| [20_pandas_io_read_write.md](20_pandas_io_read_write.md)                       | CSV, Parquet, JSON, Excel, SQL read/write, format auto-detection        |

---

## 🏗️ ETL Pipeline Quick Reference

```
EXTRACT          TRANSFORM                 LOAD
─────────        ─────────────────────     ─────────────
File I/O  ──→   Generators/Iterators  ──→  DB Connectivity
DB Query  ──→   Comprehensions        ──→  Pandas to_sql()
API Call  ──→   Functional Prog.      ──→  Parquet/CSV Write
          ──→   Pandas: groupby       ──→  ExcelWriter
          ──→   Pandas: merge/join
          ──→   Pandas: apply/map
          ──→   Datetime handling
          ──→   String operations

CROSS-CUTTING:
Decorators (logging, retry, timing)
Exception Handling (quarantine, retry)
Logging (audit trail, monitoring)
Multithreading/Multiprocessing (parallel extraction)
Type Hints + Dataclasses (schema definition)
Regular Expressions (validation, parsing)
Performance Optimization (memory, speed)
```

---

## 🚦 Recommended Learning Order

1. Generators & Iterators → **lazy processing foundation**
2. Exception Handling → **build robust pipelines early**
3. Logging → **observability from day one**
4. File I/O → **read your sources**
5. Pandas I/O → **DataFrame-based reading**
6. Pandas GroupBy/Agg → **aggregate and summarize**
7. Pandas Merge/Join → **combine multiple sources**
8. Pandas Apply/Map → **custom transformations**
9. Pandas Datetime → **temporal data**
10. Pandas Strings → **text cleaning**
11. Pandas Performance → **scale up**
12. Decorators → **add cross-cutting concerns**
13. Context Managers → **resource safety**
14. Database Connectivity → **production DB operations**
15. Multithreading/Multiprocessing → **parallelism**
16. Everything else → **polish and best practices**

# 🔄 Generators & Iterators in Python (ETL Context)

---

## 📖 Explanation

**Iterators** are objects that implement the `__iter__()` and `__next__()` methods. They allow you to traverse a sequence one element at a time without loading everything into memory.

**Generators** are a simpler way to create iterators using `yield`. They are **lazy** — they produce values only when requested, making them ideal for processing **large datasets** in ETL pipelines without memory overflow.

---

## 🧠 Must-Remember Points

- Generators use `yield` instead of `return` — they pause execution and resume from where they left off.
- A generator is an iterator, but not all iterators are generators.
- Use `next()` to get the next value from an iterator.
- `StopIteration` is raised when the iterator is exhausted.
- `yield from` delegates to another generator (Python 3.3+).
- Generators are **single-use** — once exhausted, they cannot be restarted.
- Use `itertools` for advanced iterator operations (very common in ETL).
- Generator expressions: `(x*2 for x in range(100))` vs list comprehensions `[x*2 for x in range(100)]`.

---

## 💻 Code Examples

### 1️⃣ Basic Iterator (Custom Class)

```python
class CountUp:
    def __init__(self, limit):
        self.limit = limit
        self.current = 0

    def __iter__(self):
        return self  # The object itself is the iterator

    def __next__(self):
        if self.current >= self.limit:
            raise StopIteration
        self.current += 1
        return self.current

counter = CountUp(5)
for val in counter:
    print(val)  # 1, 2, 3, 4, 5
```

---

### 2️⃣ Basic Generator with `yield`

```python
def count_up(limit):
    current = 1
    while current <= limit:
        yield current
        current += 1

gen = count_up(5)
print(next(gen))  # 1
print(next(gen))  # 2

for val in count_up(3):
    print(val)  # 1, 2, 3
```

---

### 3️⃣ ETL: Reading a Large CSV File in Chunks (Generator)

```python
import csv

def read_large_csv(filepath, chunk_size=1000):
    """Generator that yields chunks of rows from a large CSV file."""
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
        if chunk:  # yield the last partial chunk
            yield chunk

# Usage in ETL pipeline
for chunk in read_large_csv("sales_data.csv", chunk_size=500):
    # Process each chunk (transform, load, etc.)
    print(f"Processing {len(chunk)} rows...")
    # e.g., insert into database, apply transformations
```

---

### 4️⃣ ETL: Generator Pipeline (Chain of Generators)

```python
def extract_rows(filepath):
    """Extract: yields rows one by one from file."""
    with open(filepath, 'r') as f:
        for line in f:
            yield line.strip()

def transform_rows(rows):
    """Transform: filter and clean each row."""
    for row in rows:
        if row and not row.startswith('#'):  # Skip empty lines and comments
            yield row.upper()  # Example transformation

def load_rows(rows, output_file):
    """Load: write transformed rows to a file."""
    with open(output_file, 'w') as f:
        for row in rows:
            f.write(row + '\n')

# Build the pipeline
raw = extract_rows("input.txt")
transformed = transform_rows(raw)
load_rows(transformed, "output.txt")  # Nothing runs until here!
```

---

### 5️⃣ `yield from` — Delegating to Sub-generators

```python
def read_multiple_files(filepaths):
    for path in filepaths:
        with open(path, 'r') as f:
            yield from f  # delegates to file's iterator

files = ["file1.txt", "file2.txt"]
for line in read_multiple_files(files):
    print(line.strip())
```

---

### 6️⃣ Generator Expression

```python
# List comprehension - loads all in memory
squares_list = [x**2 for x in range(1_000_000)]

# Generator expression - lazy, memory efficient
squares_gen = (x**2 for x in range(1_000_000))

# Use sum directly with generator — no intermediate list
total = sum(x**2 for x in range(1_000_000))
print(total)
```

---

### 7️⃣ `itertools` — Essential for ETL

```python
import itertools

# islice: take first N items from an iterator
import csv
with open("huge.csv") as f:
    reader = csv.reader(f)
    first_10 = list(itertools.islice(reader, 10))

# chain: combine multiple iterables
gen1 = (x for x in [1, 2, 3])
gen2 = (x for x in [4, 5, 6])
combined = itertools.chain(gen1, gen2)
print(list(combined))  # [1, 2, 3, 4, 5, 6]

# batched (Python 3.12+): group items into batches
data = range(10)
for batch in itertools.batched(data, 3):
    print(list(batch))  # [0, 1, 2], [3, 4, 5], [6, 7, 8], [9]

# For Python < 3.12, manual batching:
def batched(iterable, n):
    it = iter(iterable)
    while True:
        batch = list(itertools.islice(it, n))
        if not batch:
            break
        yield batch
```

---

## 🏭 ETL Use Cases

| Use Case                            | Generator Benefit                   |
| ----------------------------------- | ----------------------------------- |
| Reading huge CSV/JSON files         | Avoids loading entire file into RAM |
| Streaming data from APIs            | Process records as they arrive      |
| Batch inserts into database         | Process N rows at a time            |
| Multi-step transformation pipelines | Lazy evaluation — chain generators  |
| Log file processing                 | Line-by-line processing             |
| Data validation while reading       | Validate each record on the fly     |

---

## ⚠️ Common Pitfalls

```python
# WRONG: Generators are single-use!
gen = (x for x in range(5))
list1 = list(gen)   # [0, 1, 2, 3, 4]
list2 = list(gen)   # [] — already exhausted!

# RIGHT: Re-create the generator each time
def make_gen():
    return (x for x in range(5))

list1 = list(make_gen())
list2 = list(make_gen())
```

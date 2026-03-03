# 🔄 Generators & Iterators in Python (ETL Context)

---

## 🤔 What Is This and Why Should You Care?

Imagine you have a file with **10 million rows of sales data**. If you try to load the entire file into memory at once, your program will likely crash or become extremely slow.

**Iterators and Generators** solve this problem by processing data **one piece at a time** — like reading a book page by page instead of memorizing the whole thing first.

> 💡 **Real-world analogy**: Think of an iterator like a Netflix streaming service. Instead of downloading the entire movie (which would take a long time and fill your hard drive), Netflix streams it to you frame by frame. You get to watch immediately, and it doesn't overwhelm your device.

---

## 🧱 Part 1: Iterators — The Foundation

### What is an Iterator?

An **iterator** is any object that:

1. Knows how to move to the **next item** in a sequence
2. Knows when to **stop** (raises `StopIteration`)

In Python, iterators must have two special methods:

- `__iter__()` — returns the iterator object itself
- `__next__()` — returns the next value, or raises `StopIteration` when done

### What is an Iterable?

An **iterable** is anything you can loop over — like a `list`, `tuple`, `string`, or file. When you use a `for` loop, Python automatically calls these special methods behind the scenes.

```python
# This is what Python does behind the scenes when you write "for x in [1, 2, 3]"
my_list = [1, 2, 3]

# Step 1: Get the iterator from the iterable
iterator = iter(my_list)        # Calls my_list.__iter__()

# Step 2: Keep calling next() until StopIteration
print(next(iterator))   # 1  — Calls iterator.__next__()
print(next(iterator))   # 2
print(next(iterator))   # 3
print(next(iterator))   # 💥 Raises StopIteration — Python's signal to stop the loop
```

---

## 🧱 Part 2: Custom Iterator (Class-based)

You can build your own iterator by implementing the two special methods.

```python
class CountUp:
    """
    A custom iterator that counts from 1 up to 'limit'.
    Think of it as a for-loop counter you built from scratch.
    """

    def __init__(self, limit):
        self.limit = limit     # The maximum number to count to
        self.current = 0       # Track where we are (starts at 0, before first value)

    def __iter__(self):
        # This method must return the iterator object.
        # Since this class IS the iterator, we return 'self'.
        return self

    def __next__(self):
        # This method is called each time the loop asks "what's next?"
        if self.current >= self.limit:
            # Signal that there are no more values to return
            raise StopIteration

        # Move forward by 1
        self.current += 1

        # Return the current value
        return self.current


# Using our custom iterator just like any built-in iterable
counter = CountUp(5)
for val in counter:
    print(val)
# Output: 1  2  3  4  5

# You can also use it manually
counter2 = CountUp(3)
print(next(counter2))   # 1
print(next(counter2))   # 2
print(next(counter2))   # 3
# print(next(counter2)) # Would raise StopIteration
```

---

## ⚡ Part 3: Generators — A Simpler, More Powerful Way

Writing a full class with `__iter__` and `__next__` is verbose. Python gives us **generators** as a shortcut using the `yield` keyword.

### How `yield` works

When Python sees `yield` in a function, it:

1. Returns the value after `yield` to the caller
2. **Pauses** the function (saves its state: local variables, where it was)
3. When `next()` is called again, **resumes** from exactly where it paused

> 💡 **Analogy**: Think of `yield` like hitting a "save and pause" button in a video game. The game saves your position and progress, you go do something else, and when you return, you continue from exactly where you left off.

```python
def count_up(limit):
    """
    A generator function that counts from 1 to 'limit'.
    Notice: this is MUCH simpler than the class-based version above!
    """
    current = 1
    while current <= limit:
        yield current   # Pause here, return 'current', then resume next time
        current += 1    # This runs AFTER the caller processes the yielded value


# Creating a generator object (calling the function does NOT run any code yet!)
gen = count_up(5)

# Pulling values one by one
print(next(gen))   # 1   — function runs until first 'yield', pauses
print(next(gen))   # 2   — resumes, runs until next 'yield', pauses
print(next(gen))   # 3

# Or use in a for loop (most common)
for val in count_up(5):
    print(val)          # 1, 2, 3, 4, 5


# KEY DIFFERENCE: Generator vs Normal Function
# Normal function runs completely and returns a single value
def normal_func():
    return [1, 2, 3]     # Builds the ENTIRE list in memory

result = normal_func()   # [1, 2, 3] — all in memory NOW

# Generator function pauses after each yield — no list in memory
def gen_func():
    yield 1              # Returns 1, pauses
    yield 2              # Returns 2, pauses
    yield 3              # Returns 3, pauses (then StopIteration)

gen = gen_func()         # Nothing computed yet!
print(next(gen))         # 1 — computed on demand
print(next(gen))         # 2 — computed on demand
```

---

## 📦 ETL Code Examples

### Example 1: The Classic ETL Problem — Large CSV Files

```python
import csv

def read_large_csv(filepath, chunk_size=1000):
    """
    Generator that reads a large CSV file in chunks.

    WHY CHUNKS?
    If your file has 50 million rows, loading it all at once would require
    gigabytes of RAM. Instead, we process 1000 rows at a time.

    HOW IT WORKS:
    - We open the file and start reading row by row
    - We accumulate rows into a list called 'chunk'
    - Once the chunk reaches 'chunk_size', we yield it (hand it off to the caller)
    - The caller processes that chunk, then we continue filling the next chunk
    """
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)  # Reads each row as a dictionary
        chunk = []                   # Temporary storage for current chunk

        for row in reader:
            chunk.append(row)        # Add this row to the current chunk

            if len(chunk) == chunk_size:
                yield chunk          # 🔁 Hand off the chunk, then pause
                chunk = []           # Reset for the next chunk

        # Don't forget the last partial chunk (e.g., file has 1050 rows:
        # chunk_size=1000 yields once, then this handles the remaining 50)
        if chunk:
            yield chunk


# ---- HOW TO USE ----
# Imagine "sales_data.csv" has 5 million rows
for batch_number, chunk in enumerate(read_large_csv("sales_data.csv", chunk_size=500)):
    print(f"📦 Processing batch {batch_number + 1}: {len(chunk)} rows")
    # Do your ETL work here — transform, validate, load to database, etc.
    # Only 500 rows live in memory at any time!
```

---

### Example 2: Building an ETL Pipeline with Generator Chaining

One of the most powerful patterns: chain generators together so each stage processes one record at a time.

```python
# ════ STEP 1: EXTRACT ════
def extract_rows(filepath):
    """
    Generator: Opens a text file and yields one line at a time.
    Memory used: Only ONE line at a time — no matter the file size!
    """
    with open(filepath, 'r') as f:
        for line in f:
            yield line.strip()    # strip() removes trailing newline characters


# ════ STEP 2: TRANSFORM ════
def transform_rows(rows):
    """
    Generator: Takes rows from 'extract_rows', cleans them, yields cleaned rows.

    Note: This function receives a generator as input and returns a generator!
    No data is actually processed until someone pulls from the final output.
    """
    for row in rows:               # 'rows' is the upstream generator
        # Skip empty lines and comment lines
        if not row or row.startswith('#'):
            continue               # Skip bad rows — don't yield them

        # Apply transformations
        transformed = row.upper()  # Example: convert to uppercase
        yield transformed          # Pass the cleaned row downstream


# ════ STEP 3: LOAD ════
def load_rows(rows, output_file):
    """
    Consumer function: Reads from the pipeline and writes to output.
    This is where all the computation actually happens!
    """
    count = 0
    with open(output_file, 'w') as f:
        for row in rows:           # Pulling from the pipeline triggers everything
            f.write(row + '\n')
            count += 1
    print(f"✅ Loaded {count} rows to {output_file}")


# ════ WIRE THE PIPELINE ════
# Notice: Nothing runs yet! We're just connecting pipes.
raw_data      = extract_rows("input.txt")         # Pipe 1
clean_data    = transform_rows(raw_data)           # Pipe 2
# Output runs the whole pipeline lazily:
load_rows(clean_data, "output.txt")               # This triggers everything
```

---

### Example 3: `yield from` — Delegating to Another Generator

```python
def read_multiple_files(filepaths):
    """
    Generator that yields lines from multiple files as if they were one.

    'yield from' is a shortcut for:
       for line in f:
           yield line

    It delegates to another iterable (the file object here).
    """
    for path in filepaths:
        print(f"  📄 Now reading: {path}")
        with open(path, 'r') as f:
            yield from f            # Yield every line from this file


files = ["january_sales.txt", "february_sales.txt", "march_sales.txt"]
for line in read_multiple_files(files):
    print(line.strip())             # Seamlessly reads all 3 files
```

---

### Example 4: Generator Expressions — One-liners

```python
# ---- List comprehension: Builds ENTIRE list in memory ----
squares_list = [x**2 for x in range(1_000_000)]   # 8MB of memory used immediately!

# ---- Generator expression: Lazy, computes on demand ----
squares_gen = (x**2 for x in range(1_000_000))    # Almost no memory used yet!
# Syntax note: same as list comprehension but with () instead of []

# Usage: Use with sum(), max(), min(), for loops, etc.
total = sum(x**2 for x in range(1_000_000))        # Computes and discards each x**2
print(f"Sum of squares: {total}")

# ---- Practical ETL example ----
import csv

with open("transactions.csv") as f:
    reader = csv.DictReader(f)
    # This generator expression processes ONE row at a time:
    total_revenue = sum(
        float(row["amount"])               # Convert to float
        for row in reader                   # For each row in the file
        if row["status"] == "COMPLETED"    # Only include completed transactions
    )
print(f"Total Revenue: ${total_revenue:,.2f}")
# Memory used: Just ONE row at a time, regardless of file size!
```

---

### Example 5: `itertools` — The Iterator Toolbox

```python
import itertools

# ---- islice: Take only the first N items ----
# Perfect for previewing/sampling large files during development
import csv
with open("huge_dataset.csv") as f:
    reader = csv.reader(f)
    first_10_rows = list(itertools.islice(reader, 10))
    print("Sample rows:", first_10_rows)

# ---- chain: Combine multiple iterables seamlessly ----
january_data = (row for row in ["jan_row_1", "jan_row_2"])
february_data = (row for row in ["feb_row_1", "feb_row_2"])
march_data    = (row for row in ["mar_row_1", "mar_row_2"])

all_data = itertools.chain(january_data, february_data, march_data)
for row in all_data:
    print(row)   # jan_row_1, jan_row_2, feb_row_1, ... seamlessly combined

# ---- Manual batching (if Python < 3.12) ----
def batched(iterable, batch_size):
    """Group an iterable into batches of size n."""
    it = iter(iterable)               # Get the iterator
    while True:
        batch = list(itertools.islice(it, batch_size))   # Grab next n items
        if not batch:
            break                     # No more items — stop
        yield batch                   # Yield the batch

data = range(17)   # 17 items
for batch in batched(data, 5):
    print(list(batch))
# [0, 1, 2, 3, 4]
# [5, 6, 7, 8, 9]
# [10, 11, 12, 13, 14]
# [15, 16]           ← Last partial batch still yielded
```

---

## 🏭 ETL Use Cases Summary

| Use Case                           | Why Generators Help                                        |
| ---------------------------------- | ---------------------------------------------------------- |
| **Reading huge CSV/JSON files**    | Avoids OOM errors — processes one chunk at a time          |
| **Streaming API responses**        | Start processing records as they arrive, don't wait        |
| **Batch inserts to database**      | Insert N rows at a time instead of all at once             |
| **Multi-step transform pipelines** | Chain generators — lazy evaluation all the way through     |
| **Log file analysis**              | Line-by-line without loading the entire file               |
| **Data validation**                | Validate each record on the fly, report errors immediately |

---

## 🧠 Must-Remember Points (Summary)

| Concept                   | What to Remember                                                          |
| ------------------------- | ------------------------------------------------------------------------- |
| `yield` vs `return`       | `yield` pauses and hands off a value; function resumes on next call       |
| Generator = Iterator      | All generators are iterators, but not all iterators are generators        |
| `StopIteration`           | Automatically raised when a generator/iterator is exhausted               |
| Generators are single-use | Once exhausted, cannot restart — create a new one                         |
| `yield from`              | Delegate to another iterable (Python 3.3+)                                |
| Generator expression      | `(expr for item in iterable)` — lazy version of list comprehension        |
| `itertools`               | Built-in module with powerful iterator tools (`islice`, `chain`, `cycle`) |

---

## ⚠️ Common Mistakes Beginners Make

```python
# ❌ MISTAKE 1: Generators are single-use!
gen = (x for x in range(5))
list1 = list(gen)        # [0, 1, 2, 3, 4] — generator exhausted!
list2 = list(gen)        # [] — empty! Nothing left to generate

# ✅ FIX: Create a new generator each time, or wrap it in a function
def make_gen():
    return (x for x in range(5))

list1 = list(make_gen())   # [0, 1, 2, 3, 4]
list2 = list(make_gen())   # [0, 1, 2, 3, 4] — fresh generator!


# ❌ MISTAKE 2: Trying to use len() on a generator (it has no length!)
gen = (x for x in range(5))
print(len(gen))   # TypeError: object of type 'generator' has no len()

# ✅ FIX: Convert to list if you need length (but this loads all into memory)
data = list(gen)
print(len(data))   # 5


# ❌ MISTAKE 3: Generator expression with side-effects before exhaustion
import csv
gen = (row["amount"] for row in csv.DictReader(open("file.csv")))
# File is opened but NOT read yet! Reading happens as you consume the generator.
# The file handle stays open until the generator is exhausted or GC'd.

# ✅ FIX: Use a proper function with 'with' statement for file generators
def read_amounts(filepath):
    with open(filepath) as f:
        for row in csv.DictReader(f):
            yield row["amount"]    # File closes properly after generator exhausts
```

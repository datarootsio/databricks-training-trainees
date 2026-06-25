# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 1 — Python Basics: SOLUTION
# MAGIC
# MAGIC This notebook contains the complete solutions for the Python 101 exercise.
# MAGIC Each part corresponds to a TODO in the exercise notebook.
# MAGIC
# MAGIC **Dataset context:** Olist Brazilian e-commerce — orders, reviews, products, and sellers.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A: Data Types
# MAGIC
# MAGIC **Task:** Create a dictionary mapping Brazilian state abbreviations to order counts.

# COMMAND ----------

# WHY: Dictionaries are ideal for key-value lookups — here state code → order count
orders_by_state = {
    "SP": 41746,
    "RJ": 12852,
    "MG": 11635,
    "RS": 5466,
    "PR": 5045,
    "SC": 3637,
    "BA": 3380,
    "DF": 2140,
    "ES": 2033,
    "GO": 2020,
}
print(f"Number of states tracked: {len(orders_by_state)}")
print(f"State with most orders: SP ({orders_by_state['SP']} orders)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part A — Bonus: Exploring data types further
# MAGIC
# MAGIC Python's built-in `type()` and `isinstance()` are useful for inspecting values at runtime.

# COMMAND ----------

# Bonus: verify types and explore dict methods
print(f"Type of orders_by_state: {type(orders_by_state)}")
print(f"Keys (states): {list(orders_by_state.keys())}")
print(f"Total orders across all tracked states: {sum(orders_by_state.values()):,}")

# NOTE: dict.get() is safer than dict[key] — it returns None instead of raising KeyError
print(f"Orders in AM (using .get): {orders_by_state.get('AM', 'Not in dict')}")
print(f"Orders in SP (using .get): {orders_by_state.get('SP', 'Not in dict')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B: Functions
# MAGIC
# MAGIC **Task:** Write a function `categorize_review_score(score)` that maps review scores 1–5
# MAGIC to sentiment labels: `"negative"` (1–2), `"neutral"` (3), or `"positive"` (4–5).

# COMMAND ----------

# WHY: Using elif chains keeps the logic readable and mutually exclusive
def categorize_review_score(score: int) -> str:
    """
    Categorize a review score (1-5) into sentiment buckets.

    Args:
        score: Integer review score between 1 and 5
    Returns:
        Sentiment string: 'negative', 'neutral', or 'positive'
    """
    if score <= 2:
        return "negative"
    elif score == 3:
        return "neutral"
    else:  # score >= 4
        return "positive"

# Test the function
for s in [1, 2, 3, 4, 5]:
    print(f"Score {s} → {categorize_review_score(s)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part B — Bonus: Applying the function to a list of scores

# COMMAND ----------

# Bonus: use a list comprehension to categorize a batch of scores
sample_scores = [5, 3, 1, 4, 2, 5, 3, 1, 4, 5]
categories = [categorize_review_score(s) for s in sample_scores]
print(f"Scores:     {sample_scores}")
print(f"Categories: {categories}")

# WHY: Summarising with Counter is more concise than a manual loop
from collections import Counter
distribution = Counter(categories)
print(f"\nSentiment distribution: {dict(distribution)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C: Loops and Conditionals
# MAGIC
# MAGIC **Task:** Loop over a dict of `state → order_count` and print only the states
# MAGIC that have more than 1,000 orders.

# COMMAND ----------

orders_by_state = {
    "SP": 41746, "RJ": 12852, "MG": 11635, "RS": 5466, "PR": 5045,
    "SC": 3637, "BA": 3380, "DF": 2140, "ES": 2033, "GO": 2020,
    "AM": 748, "AC": 81, "RR": 46,
}

# WHY: .items() unpacks key-value pairs — much more readable than states[key]
print("States with more than 1000 orders:")
for state, count in orders_by_state.items():
    if count > 1000:
        print(f"  {state}: {count:,} orders")
# NOTE: The {:,} format adds thousands separators for readability

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part C — Bonus: Sorting within the loop

# COMMAND ----------

# Bonus: print states in descending order of volume (not insertion order)
# WHY: sorted() on .items() with a key function avoids importing extra libraries
print("States with more than 1000 orders (sorted by volume):")
for state, count in sorted(orders_by_state.items(), key=lambda x: x[1], reverse=True):
    if count > 1000:
        print(f"  {state}: {count:,} orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D: List Comprehensions and Built-ins
# MAGIC
# MAGIC **Task:** Given `review_scores = [4, 2, 5, 1, 3, 5, 4, 2]`, compute the average,
# MAGIC maximum, minimum, and a descending-sorted copy of the list.

# COMMAND ----------

review_scores = [4, 2, 5, 1, 3, 5, 4, 2]

# WHY: sum()/len() is the standard Pythonic way to compute mean without importing statistics
average_score = sum(review_scores) / len(review_scores)
max_score = max(review_scores)
min_score = min(review_scores)
# WHY: sorted() with reverse=True returns a new list; list.sort() mutates in place
scores_desc = sorted(review_scores, reverse=True)

print(f"Average score: {average_score:.2f}")
print(f"Max score:     {max_score}")
print(f"Min score:     {min_score}")
print(f"Scores (descending): {scores_desc}")

# Bonus: count 5-star reviews using a list comprehension
five_star_count = len([s for s in review_scores if s == 5])
print(f"Number of 5-star reviews: {five_star_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part D — Bonus: Using the `statistics` module and generator expressions

# COMMAND ----------

import statistics

# NOTE: The statistics module provides median, stdev, and mode — useful for EDA
print(f"Median score:  {statistics.median(review_scores)}")
print(f"Mode score:    {statistics.mode(review_scores)}")
print(f"Std deviation: {statistics.stdev(review_scores):.4f}")

# WHY: Generator expressions (without [ ]) are memory-efficient for large datasets
positive_scores_sum = sum(s for s in review_scores if s >= 4)
print(f"Sum of positive scores (>=4): {positive_scores_sum}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part E: Dictionaries
# MAGIC
# MAGIC **Task:** Write a function that takes a dict of `state → revenue` and returns
# MAGIC the top-3 states as a sorted list of `(state, revenue)` tuples.

# COMMAND ----------

# WHY: sorted() with key=lambda lets us sort by value (revenue) without external libraries
def top_states_by_revenue(state_revenue: dict, n: int = 3) -> list:
    """
    Return the top-n states by revenue as a sorted list of (state, revenue) tuples.

    Args:
        state_revenue: dict mapping state code → total revenue
        n: number of top states to return (default: 3)
    Returns:
        List of (state, revenue) tuples, sorted by revenue descending
    """
    # WHY: .items() gives (key, value) pairs; we sort by value (index 1), descending
    sorted_states = sorted(state_revenue.items(), key=lambda x: x[1], reverse=True)
    return sorted_states[:n]

# Test with sample data
state_revenue = {
    "SP": 5_854_321.50,
    "RJ": 1_923_456.75,
    "MG": 1_654_321.00,
    "RS": 876_543.25,
    "PR": 754_321.80,
    "BA": 432_100.00,
}

top3 = top_states_by_revenue(state_revenue, n=3)
print("Top 3 states by revenue:")
for rank, (state, revenue) in enumerate(top3, start=1):
    # NOTE: enumerate(start=1) gives 1-based index — cleaner for rankings
    print(f"  #{rank} {state}: R$ {revenue:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part E — Bonus: Make the function more flexible

# COMMAND ----------

# Bonus: extend the function to also accept a filter threshold
def top_states_by_revenue_filtered(state_revenue: dict, n: int = 3, min_revenue: float = 0.0) -> list:
    """
    Return top-n states by revenue, excluding states below a minimum revenue threshold.

    Args:
        state_revenue: dict mapping state code → total revenue
        n: number of top states to return
        min_revenue: exclude states with revenue below this value
    Returns:
        List of (state, revenue) tuples, sorted by revenue descending
    """
    # WHY: filter first, then sort — avoids sorting entries that will be discarded anyway
    filtered = {state: rev for state, rev in state_revenue.items() if rev >= min_revenue}
    sorted_states = sorted(filtered.items(), key=lambda x: x[1], reverse=True)
    return sorted_states[:n]

# Test: top 5 states with at least R$ 500,000 revenue
top5_filtered = top_states_by_revenue_filtered(state_revenue, n=5, min_revenue=500_000)
print("Top 5 states with revenue >= R$ 500,000:")
for rank, (state, revenue) in enumerate(top5_filtered, start=1):
    print(f"  #{rank} {state}: R$ {revenue:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC | Part | Topic | Key takeaway |
# MAGIC |------|-------|-------------|
# MAGIC | A | Data types | Use `dict` for key-value lookups; `list` for ordered sequences |
# MAGIC | B | Functions | Type hints + docstrings make code self-documenting |
# MAGIC | C | Loops | `.items()` unpacks dicts; `{:,}` formats numbers with commas |
# MAGIC | D | Built-ins | `sum()`, `max()`, `min()`, `sorted()` cover most numeric needs without imports |
# MAGIC | E | Dicts + sorting | `sorted(.items(), key=lambda x: x[1])` is the idiomatic way to sort by value |

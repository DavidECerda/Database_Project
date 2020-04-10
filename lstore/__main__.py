from db import Database
from query import Query
from time import process_time
from random import choice, randrange

from mergejob import MergeJob
import threading
import time
import statistics

# Student Id and 4 grades
db = Database()
db.open()
grades_table = db.create_table('Grades', 5, 0) # Type: Table
query = Query(grades_table)
keys = []

# Measuring Insert Performance
insert_time_0 = process_time()
for i in range(0, 10000):
    query.insert(906659671 + i, randrange(0,100), randrange(0,100), randrange(0,100), randrange(0,100))
    keys.append(906659671 + i)
insert_time_1 = process_time()

print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

# Measuring update Performance
update_cols = [
    [randrange(0, 100), None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

update_time_0 = process_time()
for i in range(0, 10000):
    query.update(choice(keys), *(choice(update_cols)))
update_time_1 = process_time()
print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)

select_times_before = []
agg_times_before = []

for _ in range(1):
    print('')
    # Measuring Select Performance
    select_time_0 = process_time()
    for i in range(0, 10000):
        query.select(choice(keys), 0, [1, 1, 1, 1, 1])
    select_time_1 = process_time()
    elapsed = select_time_1 - select_time_0
    select_times_before.append(elapsed)
    print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)

    # Measuring Aggregate Performance
    agg_time_0 = process_time()
    for i in range(0, 10000, 100):
        sum_col = randrange(0, 5)
        # print(sum_col)
        result = query.sum(906659671+i, 906659671+100, sum_col)
    agg_time_1 = process_time()
    elapsed = agg_time_1 - agg_time_0
    agg_times_before.append(elapsed)
    print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)
    print('')

print("Creating Indices")
grades_table.indices.create_index(1)
grades_table.indices.create_index(2)
grades_table.indices.create_index(3)
grades_table.indices.create_index(4)
print("Created Indices")
select_times_after = []
agg_times_after = []

for _ in range(1):
    print('')
    # Measuring Select Performance
    select_time_0 = process_time()
    for i in range(0, 10000):
        query.select(choice(keys), 0, [1, 1, 1, 1, 1])
    select_time_1 = process_time()
    elapsed = select_time_1 - select_time_0
    select_times_after.append(elapsed)
    print("Selecting 10k records took:  \t\t\t", elapsed)

    # Measuring Aggregate Performance
    agg_time_0 = process_time()
    for i in range(0, 10000, 100):
        sum_col = randrange(0, 5)
        # print(sum_col)
        result = query.sum(906659671+i, 906659671+100, sum_col)
    agg_time_1 = process_time()
    elapsed = agg_time_1 - agg_time_0
    agg_times_after.append(elapsed)
    print("Aggregate 10k of 100 record batch took:\t", elapsed)
    print('')

print('Mean select before', statistics.mean(select_times_before))
print('Mean select after', statistics.mean(select_times_after))
print('Mean aggr before', statistics.mean(agg_times_before))
print('Mean aggr after', statistics.mean(agg_times_after))


# Measuring Delete Performance
delete_time_0 = process_time()
for i in range(0, 1000):
    query.delete(906659671 + i)
delete_time_1 = process_time()
print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)

stop = True
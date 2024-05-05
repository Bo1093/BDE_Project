import time
import os
import sys
from pyspark.sql import SparkSession
from itertools import combinations
from operator import add
import copy
import math

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

sc = spark.sparkContext

start_time = time.time()
frequent_itemsets_rdd = ""
rdd_size = 0
case_number = 2
support = 3


def read_data():
    global frequent_itemsets_rdd
    global rdd_size

    if case_number == 1:
        #user_id ka case
        frequent_itemsets_rdd = sc.textFile("small1.csv")\
            .filter(lambda x: x != "user_id,business_id")\
            .map(lambda x: x.split(","))\
            .groupByKey()\
            .map(lambda x: (set(x[1])))
    else:
        #business_id ka case
        frequent_itemsets_rdd = sc.textFile("small1.csv") \
            .filter(lambda x: x != "user_id,business_id") \
            .map(lambda x: x.split(",")) \
            .map(lambda x: [x[1], x[0]]) \
            .groupByKey() \
            .map(lambda x: (set(x[1])))

    rdd_size = frequent_itemsets_rdd.count()


def get_largest_set_size(list_of_sets):
    return max(map(len, list_of_sets))


def get_subsets(super_set, set_size):
    return [set(i) for i in combinations(super_set, set_size)]


def count_occurrences(subset, partition_data, partition_support):
    count = 0
    for set_ in partition_data:
        if subset.issubset(set_):
            count += 1
        if count >= partition_support:
            return count
    return count


def get_qualifying_subsets(subsets, partition_data, partition_support):
    qualifying_subsets = []
    for subset in subsets:
        if count_occurrences(subset, partition_data, partition_support) >= partition_support:
            qualifying_subsets.append(subset)
    return qualifying_subsets


def get_union(sets_):
    if sets_ is not None and len(sets_) > 0:
        return set.union(*sets_)
    return set()


def get_candidates_with_apriori(partition):
    global support

    partition_data = copy.deepcopy(list(partition))
    partition_support_threshold = math.ceil(support * len(partition_data) / rdd_size)
    largest_set_size = get_largest_set_size(partition_data)
    candidates = []

    union_qualifying_sets = get_union(partition_data)
    for i in range(1, largest_set_size + 1):
        all_subsets = get_subsets(union_qualifying_sets, i)
        qualifying_subsets = get_qualifying_subsets(all_subsets, partition_data, partition_support_threshold)
        if len(qualifying_subsets) <= 0:
            break
        candidates.extend(qualifying_subsets)
        union_qualifying_sets = get_union(qualifying_subsets)
    return candidates


def get_counts_over_whole_dataset(partition, list_of_candidates):
    global support
    partition_data = copy.deepcopy(list(partition))
    qualifying_sets = []
    for candidate in list_of_candidates:
        qualifying_sets.append([tuple(candidate), count_occurrences(candidate, partition_data, support)])
    return qualifying_sets


def write_output(candidates, frequent_itemsets):
    formatted_result = "Candidates:\n"
    for i in candidates:
        for sets_ in i[1]:
            formatted_result += "(" + ", ".join("'" + ele + "'" for ele in sets_) + "),"
        formatted_result = formatted_result[:-1] + "\n\n"

    formatted_result += "Frequent Itemsets:\n"
    for i in frequent_itemsets:
        for sets_ in i[1]:
            formatted_result += "(" + ", ".join("'" + ele + "'" for ele in sets_) + "),"
        formatted_result = formatted_result[:-1] + "\n\n"

    with open("task1_output_n_c1_sup4.txt", mode="w") as op_file:
        op_file.write(formatted_result)


def son_algorithm():
    global frequent_itemsets_rdd
    global support

    candidates = frequent_itemsets_rdd \
        .mapPartitions(get_candidates_with_apriori) \
        .map(lambda x: (len(x), sorted(tuple(x)))) \
        .groupByKey() \
        .map(lambda x: (x[0], map(tuple, x[1]))) \
        .map(lambda x: (x[0], sorted(list(set(x[1]))))) \
        .sortByKey()

    list_of_candidates = candidates \
        .map(lambda x: x[1]) \
        .flatMap(lambda x: x) \
        .map(lambda x: set(x)) \
        .collect()

    frequent_itemsets = frequent_itemsets_rdd \
        .mapPartitions(lambda x: get_counts_over_whole_dataset(x, list_of_candidates)) \
        .reduceByKey(add) \
        .filter(lambda x: x[1] >= support) \
        .map(lambda x: (len(x[0]), sorted(x[0])))\
        .groupByKey()\
        .map(lambda x: (x[0], sorted(list(x[1]))))\
        .sortByKey()

    write_output(candidates.collect(), frequent_itemsets.collect())
    print("Duration: ", time.time() - start_time)
    print("Hello World ")


read_data()
son_algorithm()

# list_of_sets = [{1, 2, 3}, {4, 5}, {6, 7, 8, 9}]
# largest_size = get_largest_set_size(list_of_sets)
# print("Largest set size:", largest_size)

# super_set = {1, 2, 3,4}
# set_size = 2
# subsets = get_subsets(super_set, set_size)
# print("Subsets of size", set_size, ":", subsets)

# subset = {2, 1}
# partition_data = [{1, 2, 3}, {4, 5}, {1, 2}]
# partition_support = 2
# count = count_occurrences(subset, partition_data, partition_support)
# print("Count of subset occurrence:", count)

# count = 0: Initialize the count to zero.
# for set_ in partition_data: Iterate over each set in the partition data.
# 1st iteration: set_ = {1, 2, 3}
# Check if the subset {1, 2} is a subset of {1, 2, 3}.
# Since it is a subset, increment count by 1.
# Count = 1
# 2nd iteration: set_ = {4, 5}
# Check if the subset {1, 2} is a subset of {4, 5}.
# It's not, so no action taken, count remains 1.
# 3rd iteration: set_ = {1, 2}
# Check if the subset {1, 2} is a subset of {1, 2}.
# Since it's a subset, increment count by 1.
# Count = 2
# if count >= partition_support: return count: Check if the count is greater than or equal to the partition support threshold.
# Since count (2) is equal to the support threshold (2), return count.

# subsets = [{1, 2}, {1, 3}, {2, 3}]
# partition_data = [{1, 2, 3}, {4, 5}, {1, 2}, {2,1}]
# partition_support = 3
# qualifying_subsets = get_qualifying_subsets(subsets, partition_data, partition_support)
# print("Qualifying subsets:", qualifying_subsets)

# qualifying_subsets = []: Initialize an empty list to store qualifying subsets.
# for subset in subsets: Iterate over each subset in the given list.
# 1st iteration: subset = {1, 2}
# Call count_occurrences({1, 2}, [{1, 2, 3}, {4, 5}, {1, 2}], 2)
# The count of {1, 2} in the partition data is 2, which is equal to the partition support threshold (2).
# So, {1, 2} qualifies and is appended to qualifying_subsets.
# 2nd iteration: subset = {1, 3}
# Call count_occurrences({1, 3}, [{1, 2, 3}, {4, 5}, {1, 2}], 2)
# The count of {1, 3} in the partition data is 1, which is less than the partition support threshold (2).
# So, {1, 3} does not qualify and is not appended to qualifying_subsets.
# 3rd iteration: subset = {2, 3}
# Call count_occurrences({2, 3}, [{1, 2, 3}, {4, 5}, {1, 2}], 2)
# The count of {2, 3} in the partition data is 1, which is less than the partition support threshold (2).
# So, {2, 3} does not qualify and is not appended to qualifying_subsets.
# return qualifying_subsets: Return the list of qualifying subsets.


# sets_ = [{1, 2}, {2, 3}, {3, 4}]
# union_set = get_union(sets_)
# print("Union set:", union_set)

# partition = [{1, 2, 3}, {4, 5}, {1, 2}, {2,7}]
# candidates = get_candidates_with_apriori(partition)
# print("Candidates:", candidates)

# list_of_candidates = [{1, 2}, {1, 3}]
# partition = [{1, 2, 3}, {4, 5}, {1, 2}]
#
# counts = get_counts_over_whole_dataset(partition, list_of_candidates)
# print("Counts over whole dataset:", counts)
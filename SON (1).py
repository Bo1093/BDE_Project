import time
import os
import sys
from pyspark.sql import SparkSession
from itertools import combinations
from operator import add
import copy
import math
import firebase_admin
from firebase_admin import credentials, storage

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
case_number = 1
support = 5


def read_data():
    global frequent_itemsets_rdd
    global rdd_size

    if case_number == 1:
        #user_id ka case
        frequent_itemsets_rdd = sc.textFile("small5.csv")\
            .filter(lambda x: x != "user_id,business_id")\
            .map(lambda x: x.split(","))\
            .groupByKey()\
            .map(lambda x: (set(x[1])))
    else:
        #business_id ka case
        frequent_itemsets_rdd = sc.textFile("small5.csv") \
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


cred_json={
"type": "service_account",
"project_id": "ieee-3db33",
"private_key_id": "3425a51e45a7e544dd526f4ced115a254ff879a2",
"private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDhI6b51iQ5+4nA\nytdaPGr9a8HchyDWQ04nFoQMPiMe7zDe2zhJIbAv3mw1CoSb+evKq0wi6XLX6jPJ\nJ6Mg8Ptkprlh2SJP5PU9PRhkbKr2lWfEbrBK4Hm9f704i0/3dKOOWXbIWlOgLWYC\nUd5/Hivx8p0NkFTNl9xJaNXws3bVkCBTiEigLYGb/uQ0CHTk0gKZVfudgr0qangi\nj5h8mfcURslD/29Ieh50hBOcbxE/o14AkdeJI5AyysqTkAo/nhL4CA7Sp69tciG1\nRJHLoySx4M14ik9gDzZ8Wet1nIxXF1m7wmH2m64fOYeN7IiFRDZgvKYFbYP+DYOQ\nx5fcoPznAgMBAAECggEAAkRjaktJV6YiNk4GEFNVJ++x/h1hVGh7vNqTDe3Hh8Kg\nOZZJdZZXwluk5k7tq5FWZb+SqmJtKTmMCVFNjAHIgTRJgszfdbfeBfuekCBTd9zZ\nBhFqAj2Qt+k1h7qOUgC+XisCvPo3JxgvZRtnT5cHU84VOxBq+AStD57pgPprfT8j\nDtKqpEUlcUacy2A5T9Uy8qXxvGbjyv+ApqllLummgZUWQbrxYG+YM4GUpRgmdB9U\nUvfY9p80tOD//EBSCqF4Oyd+bUqffZD3BxVraX2LZ8qW9rlM+DrPewhDdBB0m4U7\nkjTS5qwOTlm6u0FUfGznqwj344Us4ia3FKRX/D677QKBgQD1dyLQKM9WvBDbUJ5o\n/Jy0582eQoW83EmgdauYJlmpw5Ry4OxzPEX9Oa+ulmOdL5+hoFQDTMRbbvB7zBkN\naiff1ZOoGHwbCuqSedCdjFKNh8WLBtZcY7cjoBV4qkJ/tNxQ19ENJiV3KRmy8QcT\nX7ma0PMd/FmKSxLjWnjAQuFjnQKBgQDqzTLYUjYPaqM62DFx/gzVtrAU7f2vDBZd\nNbcVAXPauOa7WT8F83WY7hvnkvgcKrAcLdFZDWwt6Jait1YUEv5I5bsTCmB0YTly\nwVYIvEXAfLbwnZMhoN2yMR++d6uH8bCWZM2nvU/6ra2UA9N8METNIY0wUgPBiaoC\nnoZ1DkglUwKBgQCbGGCdx/th4UiBWooM6fgV8hUgdwXLlCDNSyxV4X1r35DvmSCt\nmxrZ6lYP6SQd0FZ7qDMNNrcm0o0Om6IEsNtq+abnYjkgWSBn1qIyudP7axstQe+1\nxqeT0fVfHa0QxfUi+4oyVbT8erKrNtHystwybu3+N1FYKSFRF/wN9vQ0nQKBgAKj\nC08inTjPGcYvZ17AW6SKyK9zfMXafOXPFJ9HxOVP7kdsWSjX8xokkmunWuH2GMQ4\nP4GghPZ/BjINnQncrL5k1hUAqNSlwt9nDHBMrPvcarGJE33tMJAvvQuGjIaaUEFg\nIG8h0SQfjzN6V4WthRhIqC1CvogN47rjzN7DqkvBAoGAFL92rRbFjmXWak/65uB4\nc6rdv6dza9aWEo9e26AE/fzsJxTe0f6ued4TG/bWFdzJIMltLouDRZ9FhmP+sRyw\n7UPexKUfi9AanBnh+0eMOnCezKjuiwgGkwGwQ6adddFSB1U00Umf9xmVBczHug9I\nOOKxF8KF0qAKWDn6Gveevmw=\n-----END PRIVATE KEY-----\n",
"client_email": "firebase-adminsdk-v76aj@ieee-3db33.iam.gserviceaccount.com",
"client_id": "110580532928577646626",
"auth_uri": "https://accounts.google.com/o/oauth2/auth",
"token_uri": "https://oauth2.googleapis.com/token",
"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
"client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-v76aj%40ieee-3db33.iam.gserviceaccount.com",
"universe_domain": "googleapis.com"
}

cred = credentials.Certificate(cred_json)
firebase_admin.initialize_app(cred, {
'storageBucket': 'ieee-3db33.appspot.com'
})

def upload_file(local_file, storage_path):
    bucket = storage.bucket()
    blob = bucket.blob(storage_path)
    blob.upload_from_filename(local_file)

    # Make the file public
    blob.make_public()

    # Get the public URL
    public_url = blob.public_url
    print(f"File {local_file} uploaded to {storage_path}. Public URL: {public_url}")
    return public_url

public_url = upload_file("task1_output_n_c1_sup4.txt", "videos/task1_output_n_c1_sup4.txt")

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
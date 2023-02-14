import os
import itertools
import pandas
import numpy as np

my_path = os.path.abspath(os.path.dirname(__file__))

# Read datasets into DataFrames
dataset1 = pandas.read_csv(os.path.join(my_path, "../data/dataset1.csv"))
dataset2 = pandas.read_csv(os.path.join(my_path, "../data/dataset2.csv")).astype({"tier": str})

# Join data of the data sets.
joined_data = pandas.merge(dataset1, dataset2, on='counter_party')
group_fields = ["legal_entity", "counter_party", "tier"]
all_data = []

# Create and append DataFrames to all_data list for all combinations of group_fields
for x in reversed(range(1,len(group_fields) + 1)):  
    for y in list(itertools.combinations(group_fields, x)):
        grouped_data = joined_data.groupby(list(y)).agg(
            max_rating=("rating", "max"),
            sum_arap=("value", lambda x: x[joined_data["status"] == "ARAP"].sum()),
            sum_accr=("value", lambda x: x[joined_data["status"] == "ACCR"].sum())
        ).reset_index()
        all_data.append(grouped_data)

# Output all data to CSV file.
output_data = pandas.concat(all_data)
output_data.round(decimals=0).astype(object).replace(np.nan, "Total") \
    .rename(columns={
        "max_rating": "max(rating by counter_party)", 
        "sum_arap": "sum(value where status=ARAP)", 
        "sum_accr": "sum(value where status=ACCR)"
    }).to_csv(os.path.join(my_path, "../output/pandas_output.csv"), index=False)
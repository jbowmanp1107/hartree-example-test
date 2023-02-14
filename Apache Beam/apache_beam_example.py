import os
import itertools
import apache_beam as beam
from apache_beam.dataframe.io import read_csv, to_csv
from apache_beam.dataframe.convert import to_pcollection, to_dataframe
from apache_beam import dataframe
import typing


class OutputTypes(typing.NamedTuple):
    legal_entity: str
    counter_party: str
    tier: str
    max_rating: int
    sum_arap: int
    sum_accr: int

def fill_missing_fields(record):
    fields = list(record)
    if not hasattr(record, "legal_entity"):
        fields.insert(0, "Total")
    if not hasattr(record, "counter_party"):
        fields.insert(1, "Total")
    if not hasattr(record, "tier"):
        fields.insert(2, "Total")
    return fields

with beam.Pipeline() as p:
    my_path = os.path.abspath(os.path.dirname(__file__))

    # Read datasets into DefferedDataFrame
    dataset1 = p | "Read dataset1" >> read_csv(os.path.join(my_path, "../data/dataset1.csv"))
    dataset2 = p | "Read dataset2" >> read_csv(os.path.join(my_path, "../data/dataset2.csv"))

    # Join data of the datasets
    with dataframe.allow_non_parallel_operations():
        joined_data = dataset1.merge(dataset2, on="counter_party")

    # Convert DefferedDataFrame into PCollection
    joined_data = to_pcollection(joined_data)
    group_fields = ["legal_entity", "counter_party", "tier"]
    all_data = []

    # Create and append PCollections to all_data list for all combinations of group_fields
    for x in reversed(range(1,len(group_fields) + 1)):  
        for y in list(itertools.combinations(group_fields, x)):
            grouped_data = joined_data | "aggregating " + str(y)  >> beam.GroupBy(*y) \
                .aggregate_field("rating", max, "max_rating") \
                .aggregate_field(lambda v: v.value if v.status == "ARAP" else 0, sum, "sum_arap") \
                .aggregate_field(lambda v: v.value if v.status == "ACCR" else 0, sum, "sum_accr") \
            | "filling " + str(y) >> beam.Map(fill_missing_fields)
            all_data.append(grouped_data)

    # Output all data to a CSV file
    output_data = all_data | 'Flatten' >> beam.Flatten().with_output_types(OutputTypes)
    to_csv(to_dataframe(output_data).fillna("Total"), os.path.join(my_path, "../output/beam_output.csv"), index=False, line_terminator='\n', header=["legal_entity", "counter_party", "tier", "max(rating by counter_party)", "sum(value where status=ARAP)", "sum(value where status=ACCR)"])
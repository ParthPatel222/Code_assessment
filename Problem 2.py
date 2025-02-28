import csv
import hashlib
import pandas as pd
from faker import Faker
import dask.dataframe as dd

# Generate Sample Data using Faker to test
def generate_csv(filename, num_records=1_000_000):
    fake = Faker()
    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["first_name", "last_name", "address", "date_of_birth"])
        for _ in range(num_records):
            writer.writerow([fake.first_name(), fake.last_name(), fake.address(), fake.date_of_birth()])

# Anonymization Function
def hash_value(value):
    return hashlib.sha256(value.encode()).hexdigest()

# Anonymize Using Pandas (For Medium Files)
def anonymize_pandas(input_file, output_file):
    chunk_size = 100_000  # Process in chunks because file is large (1M records)
    for chunk in pd.read_csv(input_file, chunksize=chunk_size):
        chunk["first_name"] = chunk["first_name"].apply(hash_value)
        chunk["last_name"] = chunk["last_name"].apply(hash_value)
        chunk["address"] = chunk["address"].apply(hash_value)
        chunk.to_csv(output_file, mode="a", index=False, header=False)

# Anonymize Using Dask (For Large Files)
def anonymize_dask(input_file, output_file):
    df = dd.read_csv(input_file)
    df["first_name"] = df["first_name"].apply(hash_value, meta=("x", "str"))
    df["last_name"] = df["last_name"].apply(hash_value, meta=("x", "str"))
    df["address"] = df["address"].apply(hash_value, meta=("x", "str"))
    df.to_csv(output_file, single_file=True, index=False)

# Run process
if __name__ == "__main__":
    generate_csv("large_dataset.csv", num_records=1_000_000)  # Generate sample data
    anonymize_pandas("large_dataset.csv", "anonymized_dataset.csv")  # Process with Pandas
    # anonymize_dask("large_dataset.csv", "anonymized_large_dataset.csv")  # Process with Dask
    print("Anonymization complete!")

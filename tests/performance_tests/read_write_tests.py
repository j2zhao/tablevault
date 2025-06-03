import os
import time
import json
import pickle
import csv
import pandas as pd
import yaml

# For Parquet and Feather, we use pandas built-in support.
PARQUET_ENGINE = None
try:
    import pyarrow  # noqa: F401

    PARQUET_ENGINE = "pyarrow"
except ImportError:
    try:
        import fastparquet  # noqa: F401

        PARQUET_ENGINE = "fastparquet"
    except ImportError:
        print("No Parquet engine installed; skipping Parquet and Feather tests.")


# ---------------------------
# Helper functions
# ---------------------------
def time_function(func, *args, **kwargs):
    """Utility to time a function call."""
    start = time.time()
    result = func(*args, **kwargs)
    end = time.time()
    return result, end - start


def remove_file(fname):
    try:
        os.remove(fname)
    except OSError:
        pass


def get_file_size(fname):
    """Return file size in bytes."""
    try:
        return os.path.getsize(fname)
    except OSError:
        return None


def write_and_read_json(data_dict, filename="test.json"):
    # Write JSON
    with open(filename, "w") as f:
        _, write_time = time_function(json.dump, data_dict, f)
    file_size = get_file_size(filename)
    # Read JSON
    with open(filename, "r") as f:
        _, read_time = time_function(json.load, f)
    remove_file(filename)
    return write_time, read_time, file_size


def write_and_read_pickle(data_dict, filename="test.pkl"):
    with open(filename, "wb") as f:
        _, write_time = time_function(
            pickle.dump, data_dict, f, protocol=pickle.HIGHEST_PROTOCOL
        )
    file_size = get_file_size(filename)
    with open(filename, "rb") as f:
        _, read_time = time_function(pickle.load, f)
    remove_file(filename)
    return write_time, read_time, file_size


def write_and_read_yaml(data_dict, filename="test.yaml"):
    with open(filename, "w") as f:
        _, write_time = time_function(yaml.dump, data_dict, f)
    file_size = get_file_size(filename)
    with open(filename, "r") as f:
        _, read_time = time_function(yaml.safe_load, f)
    remove_file(filename)
    return write_time, read_time, file_size


def write_and_read_csv_csvmodule(data_list, filename="test.csv"):
    # Write CSV using csv module
    with open(filename, "w", newline="") as f:
        fieldnames = ["number", "text"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        start = time.time()
        for row in data_list:
            writer.writerow(row)
        write_time = time.time() - start
    file_size = get_file_size(filename)
    # Read CSV using csv module
    with open(filename, "r", newline="") as f:
        start = time.time()
        reader = csv.DictReader(f)
        list(reader)
        read_time = time.time() - start
    remove_file(filename)
    return write_time, read_time, file_size


def write_and_read_csv_pandas(df, filename="test_pandas.csv"):
    _, write_time = time_function(df.to_csv, filename, index=False)
    file_size = get_file_size(filename)
    _, read_time = time_function(pd.read_csv, filename)
    remove_file(filename)
    return write_time, read_time, file_size


def write_and_read_parquet(df, filename="test.parquet"):
    if PARQUET_ENGINE is None:
        return None, None, None
    _, write_time = time_function(
        df.to_parquet, filename, engine=PARQUET_ENGINE, index=False
    )
    file_size = get_file_size(filename)
    _, read_time = time_function(pd.read_parquet, filename, engine=PARQUET_ENGINE)
    remove_file(filename)
    return write_time, read_time, file_size


def write_and_read_hdf(df, filename="test.h5"):
    # HDF5 requires a key name; here we use "data"
    _, write_time = time_function(df.to_hdf, filename, key="data", mode="w")
    file_size = get_file_size(filename)
    _, read_time = time_function(pd.read_hdf, filename, key="data")
    remove_file(filename)
    return write_time, read_time, file_size


def write_and_read_feather(df, filename="test.feather"):
    if PARQUET_ENGINE is None:
        return None, None, None
    _, write_time = time_function(df.to_feather, filename)
    file_size = get_file_size(filename)
    _, read_time = time_function(pd.read_feather, filename)
    remove_file(filename)
    return write_time, read_time, file_size


# ---------------------------
# Main Experiment Loop
# ---------------------------
# List of different sizes (number of rows/items) to test with
test_sizes = [1_000, 10_000, 100_000]

# We'll record results in a list of dictionaries
results = []

for N in test_sizes:
    print(f"\n--- Testing with N = {N} ---")

    # Create data for JSON/Pickle/YAML (a dictionary)
    data_dict = {
        "numbers": list(range(N)),
        "text": ["This is a sample string. " * 5 for _ in range(N)],
    }

    # Create data for tabular formats (a pandas DataFrame)
    df = pd.DataFrame(
        {
            "number": range(N),
            "text": ["This is a sample string. " * 5 for _ in range(N)],
        }
    )
    # Also create list-of-dicts for the csv module
    data_list = [
        {"number": i, "text": "This is a sample string. " * 5} for i in range(N)
    ]

    # Run tests for each format
    json_w, json_r, json_size = write_and_read_json(data_dict)
    pickle_w, pickle_r, pickle_size = write_and_read_pickle(data_dict)
    yaml_w, yaml_r, yaml_size = write_and_read_yaml(data_dict)
    csv_w, csv_r, csv_size = write_and_read_csv_csvmodule(data_list)
    pandas_csv_w, pandas_csv_r, pandas_csv_size = write_and_read_csv_pandas(df)
    parquet_w, parquet_r, parquet_size = (
        write_and_read_parquet(df) if PARQUET_ENGINE else (None, None, None)
    )
    hdf_w, hdf_r, hdf_size = write_and_read_hdf(df)
    feather_w, feather_r, feather_size = (
        write_and_read_feather(df) if PARQUET_ENGINE else (None, None, None)
    )

    # Store the results in a dict
    result = {
        "N": N,
        "json_write_ms": json_w * 1000,
        "json_read_ms": json_r * 1000,
        "json_file_size_bytes": json_size,
        "pickle_write_ms": pickle_w * 1000,
        "pickle_read_ms": pickle_r * 1000,
        "pickle_file_size_bytes": pickle_size,
        "yaml_write_ms": yaml_w * 1000,
        "yaml_read_ms": yaml_r * 1000,
        "yaml_file_size_bytes": yaml_size,
        "csv_module_write_ms": csv_w * 1000,
        "csv_module_read_ms": csv_r * 1000,
        "csv_module_file_size_bytes": csv_size,
        "pandas_csv_write_ms": pandas_csv_w * 1000,
        "pandas_csv_read_ms": pandas_csv_r * 1000,
        "pandas_csv_file_size_bytes": pandas_csv_size,
        "parquet_write_ms": parquet_w * 1000 if parquet_w is not None else None,
        "parquet_read_ms": parquet_r * 1000 if parquet_r is not None else None,
        "parquet_file_size_bytes": parquet_size,
        "hdf_write_ms": hdf_w * 1000,
        "hdf_read_ms": hdf_r * 1000,
        "hdf_file_size_bytes": hdf_size,
        "feather_write_ms": feather_w * 1000 if feather_w is not None else None,
        "feather_read_ms": feather_r * 1000 if feather_r is not None else None,
        "feather_file_size_bytes": feather_size,
    }
    results.append(result)

    # Print the results for this size
    print(
        f"JSON:           write = {result['json_write_ms']:.2f} ms, read = {result['json_read_ms']:.2f} ms, file size = {result['json_file_size_bytes']} bytes"
    )
    print(
        f"Pickle:         write = {result['pickle_write_ms']:.2f} ms, read = {result['pickle_read_ms']:.2f} ms, file size = {result['pickle_file_size_bytes']} bytes"
    )
    print(
        f"YAML:           write = {result['yaml_write_ms']:.2f} ms, read = {result['yaml_read_ms']:.2f} ms, file size = {result['yaml_file_size_bytes']} bytes"
    )
    print(
        f"CSV (csv mod):  write = {result['csv_module_write_ms']:.2f} ms, read = {result['csv_module_read_ms']:.2f} ms, file size = {result['csv_module_file_size_bytes']} bytes"
    )
    print(
        f"Pandas CSV:     write = {result['pandas_csv_write_ms']:.2f} ms, read = {result['pandas_csv_read_ms']:.2f} ms, file size = {result['pandas_csv_file_size_bytes']} bytes"
    )
    if parquet_w is not None:
        print(
            f"Parquet ({PARQUET_ENGINE}): write = {result['parquet_write_ms']:.2f} ms, read = {result['parquet_read_ms']:.2f} ms, file size = {result['parquet_file_size_bytes']} bytes"
        )
    else:
        print("Parquet:        skipped (no engine installed)")
    print(
        f"HDF5:           write = {result['hdf_write_ms']:.2f} ms, read = {result['hdf_read_ms']:.2f} ms, file size = {result['hdf_file_size_bytes']} bytes"
    )
    if feather_w is not None:
        print(
            f"Feather:        write = {result['feather_write_ms']:.2f} ms, read = {result['feather_read_ms']:.2f} ms, file size = {result['feather_file_size_bytes']} bytes"
        )
    else:
        print("Feather:        skipped (no engine installed)")

# Optionally, print a summary table at the end
print("\n--- Summary of Results ---")
for res in results:
    print(res)

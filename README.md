Benchmarks I/O operations on Pandas Dataframes.

Currently tests these file formats:
- Python pickle
- CSV
- HDF5
- Parquet
- Feather
- DuckDB

Supports some compression formats, depending on the file format.

### Requirements

- duckdb
- fastparquet
- pandas
- pyarrow
- zstandard

`requirements.txt` coming soon.

### Usage

1. (Optional) Generate a dataframe of random data to benchmark:

    ```py
    python -m dfio.gen --help
    ```
    
    Or bring your own data, in uncompressed Python pickle format.
    
2. Run benchmarks:

    ```py
    python -m dfio.benchmark --help
    ```
    
    This writes a file, by default `./dfio-benchmark.json`, with benchmark
    results.  Multiple runs are appended to the same file.
    
3. Show results:

    ```py
    python -m dfio.analyze --help
    ```



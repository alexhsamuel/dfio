import argparse
import datetime
import numpy as np
import os
import logging
from   pathlib import Path
import socket
import time

import dfio.db
import dfio.methods
from   . import gen

#-------------------------------------------------------------------------------

class NotSupported(RuntimeError):

    pass


def _benchmark(fn, *, burn=1, samples=5):
    for _ in range(burn):
        fn()

    times = []
    for _ in range(samples):
        t0 = time.perf_counter()
        fn()
        t1 = time.perf_counter()
        times.append(t1 - t0)

    return times


def _get_data_size(df):
    def get(name):
        dtype = df.dtypes[name]
        return (
            df[name].str.len().sum() if dtype.kind == "O"
            else len(df) * dtype.itemsize
        )

    return int(sum( get(n) for n in df.dtypes.keys() ))


def _build_results(operation, method, schema, df, path, times):
    return {
        "operation"     : operation,
        "method"        : method.to_jso(),
        "method_name"   : str(method),
        "schema"        : schema,
        "cols"          : len(df.dtypes),
        "length"        : len(df),
        "data_size"     : _get_data_size(df),
        "file_size"     : os.stat(path).st_size,
        "dir"           : str(path.parent),
        "timestamp"     : datetime.datetime.utcnow().isoformat(),
        "hostname"      : socket.gethostname(),
        "time"          : {
            "min"       : float(np.min(times)),
            "spread"    : float(np.max(times) - np.min(times)),
            "mean"      : float(np.mean(times)),
            "std"       : float(np.std(times)),
        },
    }


def benchmark_write(method, schema, length, dir):
    dir = Path(dir)
    if not dir.is_dir():
        raise NotADirectoryError(dir)

    size, df = gen.get_dataframe(schema, length)
    path = dir / f"{schema}-{length}"
    try:
        times = _benchmark(lambda: method.write(df, path))
        return _build_results("write", method, schema, df, path, times)
    finally:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


def benchmark_decompress(method, schema, length, dir):
    if not hasattr(method, "decompress"):
        raise NotSupported(f"decompress for {method}")

    dir = Path(dir)
    if not dir.is_dir():
        raise NotADirectoryError(dir)

    raw_size, df = gen.get_dataframe(schema, length)
    path = dir / f"{schema}-{length}"
    method.write(df, path)
    try:
        times = _benchmark(lambda: method.decompress(path))
        return _build_results("decompress", method, schema, df, path, times)
    finally:
        os.unlink(path)


def benchmark_read(method, schema, length, dir):
    dir = Path(dir)
    if not dir.is_dir():
        raise NotADirectoryError(dir)

    raw_size, df = gen.get_dataframe(schema, length)
    path = dir / f"{schema}-{length}"
    method.write(df, path)
    try:
        times = _benchmark(lambda: method.read(path))
        return _build_results("read", method, schema, df, path, times)
    finally:
        os.unlink(path)


#-------------------------------------------------------------------------------

def benchmark(operation, method, schema, length, dir=".", *, path=dfio.db.DEFAULT_PATH):
    fn = globals()[f"benchmark_{operation}"]
    try:
        rec = fn(method, schema, length, dir)
    except NotSupported:
        logging.info(f"not supported: {operation} {method} {schema} {length}")
    except Exception:
        logging.error(f"failed: {operation} {method} {schema} {length}", exc_info=True)
    else:
        dfio.db.append(rec, path=path)


#-------------------------------------------------------------------------------

ALL_OPERATIONS = (
    "write",
    "decompress",
    "read",
)

ALL_SCHEMAS = [
    "bars",
]

def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-path", metavar="DB-PATH", default="./dfio-benchmark.json",
        help="benchmark results output path [def: ./dfio-benchmark.json]")
    parser.add_argument(
        "-o", "--operation", metavar="OP", default=list(ALL_OPERATIONS),
        help="select operation OP [def: all]")
    parser.add_argument(
        "-s", "--schema", metavar="NAME", default=list(ALL_SCHEMAS),
        help="select table schema with NAME")
    parser.add_argument(
        "-l", "--length", metavar="LEN", type=int, nargs="+", default=[100000],
        help="benchmark tables of length LEN")
    parser.add_argument(
        "-m", "--method", metavar="CLASS", dest="method_class", nargs="+", default=None,
        help="select method CLASS")
    args = parser.parse_args()

    methods = dfio.methods.ALL_METHODS
    if args.method_class is not None:
        methods = ( m for m in methods if m.__class__.__name__ in args.method_class )

    runs = (
        (method, operation, schema, length)
        for method in methods
        for operation in args.operation
        for schema in args.schema
        for length in args.length
    )

    for run in runs:
        logging.info(" ".join( str(x) for x in run ))
        method, operation, schema, length = run

        benchmark(operation, method, schema, length, path=args.db_path)


if __name__ == "__main__":
    main()


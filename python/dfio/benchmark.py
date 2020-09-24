import argparse
import datetime
import itertools
import numpy as np
import logging
from   pathlib import Path
import pickle
import socket
import tempfile
import time

import dfio.db
import dfio.methods
from   . import gen

#-------------------------------------------------------------------------------

def _benchmark(fn, *, burn=1, samples=3):
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


def _build_results(operation, method, df, path, times):
    return {
        "operation"     : operation,
        "method"        : method.to_jso(),
        "method_name"   : str(method),
        "cols"          : len(df.dtypes),
        "length"        : len(df),
        "data_size"     : _get_data_size(df),
        "file_size"     : method.get_file_size(path),
        "dir"           : str(path.parent),
        "timestamp"     : datetime.datetime.utcnow().isoformat(),
        "hostname"      : socket.gethostname(),
        "time"          : {
            "count"     : len(times),
            "min"       : float(np.min(times)),
            "spread"    : float(np.max(times) - np.min(times)),
            "mean"      : float(np.mean(times)),
            "std"       : float(np.std(times)),
        },
    }


def benchmark_write(method, df, dir, *, samples=3):
    path = Path(tempfile.mktemp(dir=dir))
    try:
        times = _benchmark(lambda: method.write(df, path), samples=samples)
        return _build_results("write", method, df, path, times)
    finally:
        method.clean_up(path)


def benchmark_read(method, df, dir, *, samples=3):
    path = Path(tempfile.mktemp(dir=dir))
    method.write(df, path)
    try:
        times = _benchmark(lambda: method.read(path), samples=samples)
        return _build_results("read", method, df, path, times)
    finally:
        method.clean_up(path)


#-------------------------------------------------------------------------------

ALL_OPERATIONS = (
    "write",
    "read",
)

ALL_SCHEMAS = [
    "bars",
]

def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m", "--method", metavar="CLASS", dest="method_class", default=None,
        help="select method CLASS [def: all]")
    parser.add_argument(
        "-o", "--operation", metavar="OP", default=None,
        help="select operation OP [def: all]")
    parser.add_argument(
        "-s", "--schema", metavar="NAME", default="bars",
        help="generate table with schema NAME")
    parser.add_argument(
        "-l", "--length", metavar="LEN", type=int, default=100000,
        help="generate table of length LEN [def: 100000]")
    parser.add_argument(
        "--data", metavar="PATH", type=Path, default=None,
        help="use pickled dataframe in PATH (ignores -ls)")
    parser.add_argument(
        "--dir", metavar="DIR", type=Path, default=Path("."),
        help="benchmark reads/writes from DIR [def: .]")
    parser.add_argument(
        "--samples", metavar="NUM", type=int, default=3,
        help="time NUM samples per operation [def: 3]")
    parser.add_argument(
        "--db-path", metavar="DB-PATH", default="./dfio-benchmark.json",
        help="benchmark results output path [def: ./dfio-benchmark.json]")
    args = parser.parse_args()

    methods = dfio.methods.ALL_METHODS
    if args.method_class is not None:
        methods = [
            m for m in methods
            if m.__class__.__name__ in args.method_class
        ]
    operations = ALL_OPERATIONS if args.operation is None else [args.operation]

    if not args.dir.is_dir():
        parser.error(f"not a directory: {args.dir}")

    meta = {}

    # Load or generate the benchmark data.
    if args.data is not None:
        with open(args.data, "rb") as file:
            df = pickle.load(file)
        meta.update(data=args.data.name)
    else:
        df = gen.get_dataframe(args.schema, args.length)
        meta.update(schema=args.schema, length=args.length)

    for method, operation in itertools.product(methods, operations):
        logging.info(f"{method} {operation}")

        fn = globals()[f"benchmark_{operation}"]
        try:
            rec = fn(method, df, args.dir, samples=args.samples)
        except Exception:
            logging.error(f"failed: {operation} {method}", exc_info=True)
        else:
            rec.update(meta)
            dfio.db.append(rec, path=args.db_path)


if __name__ == "__main__":
    main()


import argparse
import datetime
import numpy as np
import os
import logging
from   pathlib import Path
import socket
import time

import dfio.db
import dfio.clui
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


def _build_results(operation, method, codename, df, path, times):
    return {
        "operation"     : operation,
        "method"        : method.to_jso(),
        "method_name"   : str(method),
        "codename"      : codename,
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


def benchmark_write(method, codename, length, dir):
    dir = Path(dir)
    if not dir.is_dir():
        raise NotADirectoryError(dir)

    size, df = gen.get_dataframe(codename, length)
    path = dir / f"{codename}-{length}"
    try:
        times = _benchmark(lambda: method.write(df, path))
        return _build_results("write", method, codename, df, path, times)
    finally:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass



def benchmark_read(method, codename, length, dir):
    dir = Path(dir)
    if not dir.is_dir():
        raise NotADirectoryError(dir)

    raw_size, df = gen.get_dataframe(codename, length)
    path = dir / f"{codename}-{length}"
    method.write(df, path)
    try:
        times = _benchmark(lambda: method.read(path))
        return _build_results("read", method, codename, df, path, times)
    finally:
        os.unlink(path)
        

#-------------------------------------------------------------------------------

def benchmark(operation, method, codename, length, dir=".", *, path=dfio.db.DEFAULT_PATH):
    fn = globals()[f"benchmark_{operation}"]
    try:
        rec = fn(method, codename, length, dir)
    except Exception:
        logging.error(f"failed: {operation} {method} {codename} {length}", exc_info=True)
    else:
        dfio.db.append(rec, path=path)


#-------------------------------------------------------------------------------

ALL_METHODS = [
    dfio.methods.Pickle()
] + [
    dfio.methods.Pickle(comp=(c, l))
    for c in dfio.methods.FILE_COMPRESSIONS
    for l in (1, 5, 9)
] + [
    dfio.methods.PandasHDF5(format=f)
    for f in ("table", "fixed")
] + [
    dfio.methods.PandasHDF5(comp=(c, l), format=f)
    for f in ("table", "fixed")
    for c in dfio.methods.PandasHDF5.COMPLIBS
    for l in (1, 5, 9)
]

ALL_OPERATIONS = (
    "write",
    "read",
)

ALL_SCHEMAS = [
    "bars",
    "i",
    "tbif",
    "iiiiiiii",
    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
]

ALL_LENGTHS = [
       1000,
      10000,
    #  100000,
    # 1000000,
]

ALL_RUNS = [
    {
        "method": method.to_jso(),
        "operation": operation,
        "codename": schema,
        "length": length,
    }
    for method in ALL_METHODS
    for operation in ALL_OPERATIONS
    for schema in ALL_SCHEMAS
    for length in ALL_LENGTHS
]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-path", metavar="DB-PATH", default="./dfio-benchmark.json",
        help="benchmark results output path [def: ./dfio-benchmark.json]")
    dfio.clui.add_filter_args(parser)
    args = parser.parse_args()

    runs = dfio.clui.filter_by_args(args, ALL_RUNS)
    for run in runs:
        print(" ".join( f"{k}={v}" for k, v in run.items() ))

        method = run.pop("method")
        method = getattr(dfio.methods, method.pop("class"))(**method)

        benchmark(
            run["operation"],
            method,
            run["codename"], 
            run["length"],
            path=args.db_path
        )


if __name__ == "__main__":
    main()


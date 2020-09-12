import datetime
import numpy as np
import os
from   pathlib import Path
import socket
import time

from   . import db, gen

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
        return _build_results("write", method, codename, df, path, times),
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
        return _build_results("read", method, codename, df, path, times),
    finally:
        os.unlink(path)
        

#-------------------------------------------------------------------------------

def benchmark(operation, method, codename, length, dir=".", *, path=db.DEFAULT_PATH):
    fn = globals()[f"benchmark_{operation}"]
    rec = fn(method, codename, length, dir)
    db.append(rec, path=path)



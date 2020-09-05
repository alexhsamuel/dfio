import datetime
import numpy as np
import os
from   pathlib import Path
import socket
import time

from   . import gen

#-------------------------------------------------------------------------------

def benchmark(fn, *, burn=1, samples=3):
    for _ in range(burn):
        fn()

    times = []
    for _ in range(samples):
        t0 = time.perf_counter()
        fn()
        t1 = time.perf_counter()
        times.append(t1 - t0)

    return times


def benchmark_write(codename, length, method, out_dir):
    out_dir = Path(out_dir)
    if not out_dir.is_dir():
        raise NotADirectoryError(out_dir)

    size, df = gen.get_dataframe(codename, length)
    path = out_dir / f"{codename}-{length}"
    try:
        times = benchmark(lambda: method.write(df, path))
    finally:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    return {
        "method"    : method.to_jso(),
        "codename"  : codename,
        "length"    : length,
        "metadata"  : {
            "timestamp" : datetime.datetime.utcnow().isoformat(),
            "hostname"  : socket.gethostname(),
        },
        "time"      : {
            "min"   : float(np.min(times)),
            "mean"  : float(np.mean(times)),
            "std"   : float(np.std(times)),
        },
    }



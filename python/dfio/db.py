import json
import os
from   pathlib import Path

DEFAULT_PATH = "./dfio-benchmark.json"

#-------------------------------------------------------------------------------

def append(rec, *, path=DEFAULT_PATH):
    path = Path(path)
    with open(path, "a") as file:
        file.write(json.dumps(rec))
        file.write("\n")


def load(*, path=DEFAULT_PATH):
    path = Path(path)
    with open(path, "r") as file:
        recs = [ json.loads(l.rstrip()) for l in file ]
    return recs



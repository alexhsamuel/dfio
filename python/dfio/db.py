import json
import os
from   pathlib import Path

DEFAULT_PATH = Path(__file__).parent / "db.json"

#-------------------------------------------------------------------------------

def append(rec, *, path=DEFAULT_PATH):
    path = Path(path)
    with open(path, "a") as file:
        file.write(json.dumps(rec))
        file.write("\n")


def load(*, path=DEFAULT_PATH):
    path = Path(path)
    with open(path, "r") as file:
        recs = [ json.loads(l) for l in file.rstrip() ]
    return recs



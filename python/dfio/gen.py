import argparse
from   functools import partial
import logging
import numpy as np
import pandas as pd
from   pathlib import Path
import pickle
import random

#-------------------------------------------------------------------------------

def _ints(value, shape):
    if callable(value):
        # It's a random function.
        return value(shape)
    else:
        # Assume it's a constant.
        result = np.empty(shape, dtype=int)
        result[:] = value
        return result


def cumsum(gen):
    return lambda n: gen(n).cumsum()


def boolean():
    return lambda s: np.random.randint(0, 2, s).astype(bool)


def normal(mu=0, sigma=1, digits=12):
    return lambda s: np.round(np.random.normal(mu, sigma, s), digits)


def uniform(lo=0, hi=1):
    return partial(np.random.uniform, lo, hi)


def uniform_int(lo, hi):
    return partial(np.random.randint, lo, hi)


def word(length, upper=False):
    """
    :param length:
      Fixed string length, or a random function to generate it.
    """
    letters = "abcdefghijklmnopqrstuvwxyz"
    if upper:
        letters = letters.upper()

    def gen(shape):
        lengths = _ints(length, shape)
        field_length = lengths.max()
        dtype = "U{}".format(field_length)

        result = np.empty(shape, dtype=dtype)
        flat = result.ravel()
        for i, l in enumerate(lengths):
            flat[i] = "".join( random.choice(letters) for _ in range(l) )
        return result

    return gen


def sample(choices):
    """
    Samples from `choices` with replacement.
    """
    return partial(np.random.choice, choices)


def dataframe(**kw_args):
    def gen(length):
        columns = { n: g(length) for n, g in kw_args.items() }
        return pd.DataFrame.from_dict(columns)

    return gen


#-------------------------------------------------------------------------------

SCHEMAS = {
    "bars": {
        "instr" : sample(uniform_int(1000000, 10000000)(5000)),
        "open"  : normal(digits=4),
        "high"  : normal(digits=4),
        "low"   : normal(digits=4),
        "close" : normal(digits=4),
        "volume": uniform_int(0, 100000),
    },
}

#-------------------------------------------------------------------------------

# b: bool
# f: float, normal(6)
# i: int, uniform(8)
# t: S8, tickers

def get_column(c):
    if c == "b":
        return boolean()
    elif c == "f":
        return normal(digits=6)
    elif c == "i":
        return uniform_int(0, 1000000000)
    elif c == "t":
        return word(8, upper=True)
    else:
        raise ValueError(f"Unknown column code: {c}")


def get_generator(schema):
    try:
        cols = SCHEMAS[schema]
    except KeyError:
        cols = { f"col{i:03d}": get_column(c) for i, c in enumerate(schema) }
    return dataframe(**cols)


#-------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--length", metavar="LEN", type=int, default=10000,
        help="generate dataframe with LEN rows [def: 10000]")
    parser.add_argument(
        "--schema", metavar="SCHEMA", default="bars",
        help="generate dataframe with SCHEMA [def: bars]")
    parser.add_argument(
        "path", metavar="PATH", type=Path,
        help="write generated dataframe to PATH")
    args = parser.parse_args()

    generator = get_generator(args.schema)
    df = generator(args.length)
    with open(args.path, "wb") as file:
        pickle.dump(df, file)


if __name__ == "__main__":
    main()


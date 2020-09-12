import dfio.benchmark
from   dfio import methods

METHODS = [
    methods.Pickle()
] + [
    methods.Pickle(comp=(c, l))
    for c in methods.FILE_COMPRESSIONS
    for l in (1, 5, 9)
] + [
    methods.PandasHDF5(format=f)
    for f in ("table", "fixed")
] + [
    methods.PandasHDF5(comp=(c, l), format=f)
    for f in ("table", "fixed")
    for c in methods.PandasHDF5.COMPLIBS
    for l in (1, 5, 9)
]

OPERATIONS = (
    "write",
    "read",
)

SCHEMAS = [
    "i",
    "tbif",
    "iiiiiiii",
    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
]

LENGTHS = [
       1000,
      10000,
     100000,
  # 1000000,
]

for method in METHODS:
    for operation in OPERATIONS:
        for schema in SCHEMAS:
            for length in LENGTHS:
                print(f"{method} {operation} {schema[:12]} {length}")
                dfio.benchmark.benchmark(operation, method, schema, length)


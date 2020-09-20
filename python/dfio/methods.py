import contextlib
import os
import pickle

from   dfio.lib.py import format_ctor

#-------------------------------------------------------------------------------

ALL_METHODS = []

@contextlib.contextmanager
def _zstd_open_write(path, level):
    import zstd
    compressor = zstd.ZstdCompressor(level=level)
    with open(path, "wb") as file, \
         compressor.stream_writer(file) as writer:
        yield writer


@contextlib.contextmanager
def _zstd_open_read(path):
    import zstd
    decompressor = zstd.ZstdDecompressor()
    with open(path, "rb") as file, \
         decompressor.stream_reader(file) as reader:
        yield reader


FILE_COMPRESSIONS = (
    "gzip",
    # "bzip2",  # Too slow; don't test this anymore.
    "zstd",
)


def open_comp(path, comp, mode):
    format, level = comp
    if format is None or level == None:
        return open(path, mode + "b")

    elif format == "gzip":
        import gzip
        return gzip.open(path, mode=mode, compresslevel=level)

    elif format == "bzip2":
        import bz2
        return bz2.open(path, mode=mode, compresslevel=level)

    elif format == "zstd":
        if mode == "w":
            return _zstd_open_write(path, level)
        elif mode == "r":
            return _zstd_open_read(path)
        else:
            raise ValueError(f"Bad mode for zstd: {mode}")

    else:
        raise ValueError(f"Unknown compression format: {format}")


#-------------------------------------------------------------------------------

class Pickle:

    def __init__(self, *, comp=(None, 0), protocol=pickle.HIGHEST_PROTOCOL):
        self.comp = comp
        self.protocol = protocol


    def __repr__(self):
        return format_ctor(self, comp=self.comp, protocol=self.protocol)


    def to_jso(self):
        return {
            "class"     : self.__class__.__name__,
            "comp"      : self.comp,
            "engine"    : self.protocol,
        }


    def write(self, df, path):
        with open_comp(path, self.comp, "w") as file:
            pickle.dump(df, file, protocol=self.protocol)


    def decompress(self, path):
        BLOCK_SIZE = 1024**2
        with open_comp(path, self.comp, "r") as file:
            while True:
                if len(file.read(BLOCK_SIZE)) < BLOCK_SIZE:
                    break


    def read(self, path):
        with open_comp(path, self.comp, "r") as file:
            return pickle.load(file)



ALL_METHODS.append(Pickle())
ALL_METHODS.extend(
    Pickle(comp=(c, l))
    for c in FILE_COMPRESSIONS
    for l in (1, 5, 9)
)

#-------------------------------------------------------------------------------

class PandasCSV:

    COMPRESSIONS = (
        None,
        "gzip",
        "bz2",
        "bz2",
        "xz",
    )

    def __init__(self, *, comp=None):
        self.comp = comp


    def __repr__(self):
        return format_ctor(self, comp=self.comp)


    def to_jso(self):
        return {
            "class"     : self.__class__.__name__,
            "comp"      : self.comp,
        }


    def write(self, df, path):
        df.to_csv(path, compression=self.comp)


    def read(self, path):
        import pandas as pd
        return pd.read_csv(path, compression=self.comp)



ALL_METHODS.append(PandasCSV())
ALL_METHODS.extend( PandasCSV(comp=c) for c in PandasCSV.COMPRESSIONS )

#-------------------------------------------------------------------------------

class PandasHDF5:

    COMPLIBS = (
        "zlib",
        "lzo",
        "bzip2",
        "blosc",
        "blosc:blosclz",
        "blosc:lz4",
        "blosc:lz4hc",
        "blosc:snappy",
        "blosc:zlib",
        "blosc:zstd",
    )

    def __init__(self, *, comp=("zlib", 0), engine="fixed"):
        self.comp = comp
        self.engine = engine


    def __repr__(self):
        return format_ctor(self, comp=self.comp, engine=self.engine)


    def to_jso(self):
        return {
            "class"     : self.__class__.__name__,
            "comp"      : list(self.comp),
            "engine"    : self.engine,
        }


    def write(self, df, path):
        complib, complevel = self.comp
        # FIXME
        if path.is_file():
            os.unlink(path)
        df.to_hdf(
            path, mode="w", key="dataframe",
            format=self.engine,
            complib=complib, complevel=complevel,
        )


    def read(self, path):
        import pandas as pd
        return pd.read_hdf(path, key="dataframe")



ALL_METHODS.extend(
    PandasHDF5(engine=f)
    for f in ("table", "fixed")
)
ALL_METHODS.extend(
    PandasHDF5(comp=(c, l), engine=f)
    for f in ("table", "fixed")
    for c in PandasHDF5.COMPLIBS
    for l in (1, 5, 9)
)

#-------------------------------------------------------------------------------

class Parquet:

    def __init__(self, *, comp=None, engine="pyarrow"):
        self.comp = comp
        self.engine = engine


    def __repr__(self):
        return format_ctor(self, comp=self.comp, engine=self.engine)


    def to_jso(self):
        return {
            "class"     : self.__class__.__name__,
            "comp"      : self.comp,
            "engine"    : self.engine,
        }


    def write(self, df, path):
        df.to_parquet(
            path,
            engine=self.engine,
            compression=self.comp,
        )


    def read(self, path):
        import pandas as pd
        return pd.read_parquet(path, engine=self.engine)



ALL_METHODS.extend(
    Parquet(comp=c, engine=e)
    for c in (None, "gzip", "snappy", "brotli", )  # zstd?
    for e in ("pyarrow", "fastparquet", )
)

#-------------------------------------------------------------------------------


import contextlib
import os
import pickle

from   dfio.lib.py import format_ctor

#-------------------------------------------------------------------------------

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
    "bzip2",
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
            "protocol"  : self.protocol,
        }


    def write(self, df, path):
        with open_comp(path, self.comp, "w") as file:
            pickle.dump(df, file, protocol=self.protocol)


    def read(self, path):
        with open_comp(path, self.comp, "r") as file:
            return pickle.load(file)



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

    def __init__(self, *, comp=("zlib", 0), format="fixed"):
        self.comp = comp
        self.format = format


    def __repr__(self):
        return format_ctor(self, comp=self.comp, format=self.format)


    def to_jso(self):
        return {
            "class"     : self.__class__.__name__,
            "comp"      : list(self.comp),
            "format"    : self.format,
        }


    def write(self, df, path):
        complib, complevel = self.comp
        # FIXME
        if path.is_file():
            os.unlink(path)
        df.to_hdf(
            path, mode="w", key="dataframe",
            format=self.format,
            complib=complib, complevel=complevel,
        )


    def read(self, path):
        import pandas as pd
        return pd.read_hdf(path, key="dataframe")



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




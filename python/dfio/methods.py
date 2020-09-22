import contextlib
import os
import pickle

from   dfio.lib.py import format_ctor

#-------------------------------------------------------------------------------

ALL_METHODS = []

def clean_up(path):
    with contextlib.suppress(FileNotFoundError):
        os.unlink(path)


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

class _Method:

    def get_file_size(self, path):
        return path.stat().st_size


    def clean_up(self, path):
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)


    def to_jso(self):
        return {
            "class"     : self.__class__.__name__,
        }



#-------------------------------------------------------------------------------

class Pickle(_Method):

    def __init__(self, *, comp=(None, 0), protocol=pickle.HIGHEST_PROTOCOL):
        self.comp = comp
        self.protocol = protocol


    def __repr__(self):
        return format_ctor(self, comp=self.comp, protocol=self.protocol)


    def to_jso(self):
        return {
            **super().to_jso(),
            "comp"      : self.comp,
            "engine"    : self.protocol,
        }


    def write(self, df, path):
        with open_comp(path, self.comp, "w") as file:
            pickle.dump(df, file, protocol=self.protocol)


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

class PandasCSV(_Method):

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
            **super().to_jso(),
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

class PandasHDF5(_Method):

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
            **super().to_jso(),
            "comp"      : list(self.comp),
            "engine"    : self.engine,
        }


    def write(self, df, path):
        complib, complevel = self.comp
        # FIXME
        clean_up(path)
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

class Parquet(_Method):

    def __init__(self, *, comp=None, engine="pyarrow"):
        self.comp = comp
        self.engine = engine


    def __repr__(self):
        return format_ctor(self, comp=self.comp, engine=self.engine)


    def to_jso(self):
        return {
            **super().to_jso(),
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

class Feather(_Method):

    COMPRESSIONS = (
        "uncompressed",
        "lz4",
        "zstd",
    )

    def __init__(self, comp="uncompressed"):
        self.comp = comp


    def __repr__(self):
        return format_ctor(self, comp=self.comp)


    def to_jso(self):
        return {
            **super().to_jso(),
            "comp"      : self.comp,
        }


    def write(self, df, path):
        import pyarrow.feather
        pyarrow.feather.write_feather(df, path, compression=self.comp)


    def read(self, path):
        import pyarrow.feather
        return pyarrow.feather.read_feather(path)

        

ALL_METHODS.extend( Feather(c) for c in Feather.COMPRESSIONS )

#-------------------------------------------------------------------------------

class DuckDB(_Method):

    def __repr__(self):
        return format_ctor(self)


    def get_file_size(self, path):
        size = path.stat().st_size
        wal_path = path.parent / (path.name + ".wal")
        with contextlib.suppress(FileNotFoundError):
            size += wal_path.stat().st_size
        return size


    def clean_up(self, path):
        super().clean_up(path)
        wal_path = path.parent / (path.name + ".wal")
        with contextlib.suppress(FileNotFoundError):
            os.unlink(wal_path)


    def write(self, df, path):
        import duckdb

        clean_up(path)
        with contextlib.closing(duckdb.connect(str(path))) as con:
            con.register("df_view", df)
            con.execute("CREATE TABLE df_table AS SELECT * FROM df_view")
            con.unregister("df_view")


    def read(self, path):
        import duckdb

        with contextlib.closing(duckdb.connect(str(path), read_only=True)) as con:
            con.execute("SELECT * FROM df_table")
            return con.fetchdf()



ALL_METHODS.append(DuckDB())


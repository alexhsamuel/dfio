import pandas as pd
import pickle

from   dfio.lib.py import format_ctor

#-------------------------------------------------------------------------------

def open_comp(path, comp, mode):
    format, level = comp
    if format is None:
        return open(path, "wb")

    elif format == "gzip":
        import gzip
        return gzip.open(path, mode=mode, compresslevel=level)

    elif format == "bzip2":
        import bz2
        return bz2.open(path, mode=mode, compresslevel=level)

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



class PandasHDF5:

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
        df.to_hdf(
            path, key="dataframe",
            format=self.format,
            complib=complib, complevel=complevel,
        )


    def load(self, path):
        return pd.load_hdf(path, key="dataframe")




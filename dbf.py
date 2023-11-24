import os.path as path
import os
import polars as pl
from dbfread import DBF


def read_as_df(filename: str, bytes: bytes, encoding: str = None):
    tmp_file = path.join(".tmp", path.basename(filename).split(".")[0] + ".dbf")

    with open(tmp_file, "wb") as f:
        f.write(bytes)

    try:
        dbf = DBF(tmp_file, encoding=encoding)
        df = pl.DataFrame(iter(dbf))
        return df
    except Exception as e:
        raise e
    finally:
        rm(tmp_file)


def rm(file: str):
    if path.exists(file):
        os.remove(file)

"""
Module with functions used to batch multiple imports from DATASUS's ftp server in parallel 
"""

from typing import Callable
import os
import polars as pl
import re
import logging
from typing import Iterable
from .ftp import get_matching_files
from .ftp import fetch_dbc_as_df
import duckdb
from dbfread import DBF
import urllib.request as request
from .db import (
    create_import_table,
    check_new_files,
    import_dataframe,
    is_file_imported,
    mark_file_as_imported,
)

def import_from_ftp(
    ftp_globs: Iterable[str],
    ftp_host="ftp.datasus.gov.br",
    ftp_exclude_regex: str = None,
):

    files = get_matching_files(ftp_host, ftp_globs)
    if ftp_exclude_regex:
        files = remove_matching(files, ftp_exclude_regex)
    new_filepaths = [f"ftp://{ftp_host}{file}" for file in files]
    print(new_filepaths)
    for filepaths in new_filepaths:
        print(f"⬇️  Downloading file from ftp: '{filepaths}'")
        fetch_dbc_as_df(filepaths)
    print("Finished downloading and converting to DBF")

    filepath_dbf = "dbf" # pasta onde estão os dbfs
    dbf_files = find_dbf_files(filepath_dbf)
    db_file="datasus.db"
    table = "dados" # tabela do duckdb onde serão incluido os dados
    with duckdb.connect(db_file) as db_con:
        create_import_table(db_con)
        for dbf_file in dbf_files:
            if is_file_imported(dbf_file, table, db_con):
                msg = f"🗃️ [{table}] File '{dbf_file}' already imported"
                print(msg)
                continue
            df = pl.DataFrame(iter(DBF(dbf_file, encoding='iso-8859-1')))
            import_table_data(df, table, dbf_file, db_con)

def remove_matching(list: list[str], regex: str):
    compiled = re.compile(regex)
    return [e for e in list if not compiled.match(e)]

def find_dbf_files(folder_path):
    dbf_files = []

    # Check if the specified folder path exists
    if not os.path.isdir(folder_path):
        print(f"Error: The specified folder '{folder_path}' does not exist.")
        return dbf_files

    # Iterate through all files in the specified folder
    for file_name in os.listdir(folder_path):
        if file_name.lower().endswith(".dbf"):  # Check if the file has a .dbf extension
            dbf_files.append(os.path.join(folder_path, file_name))

    return dbf_files


def has_table(table_name: str, db_con: duckdb.DuckDBPyConnection) -> bool:
    return db_con.execute(
        "SELECT count(*) == 1 as has_table FROM duckdb_tables where table_name = ?",
        [table_name],
    ).pl()["has_table"][0]

def import_table_data(
    df: pl.DataFrame,
    target_table: str,
    filepath,
    db_con: duckdb.DuckDBPyConnection,
):
    filename = os.path.basename(filepath)
    print(f"💾 [{target_table}] Saving data to database from: {filepath}")
    row_count = df.select(pl.len())[0, 0]

    if row_count != 0:
        import_dataframe(target_table, df, db_con)
    else:
        logging.warning(f"⚠️ [{target_table}] '{filename}' has no data")  
    mark_file_as_imported(filepath, target_table, db_con)


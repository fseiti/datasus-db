import polars as pl
import datasus
import ftp
import utils
from pl_utils import to_schema, Column, DateColumn
import logging

MAIN_TABLE = "PO"


def import_po(db_file="datasus.db", years=["*"]):
    """
    Import PO (Painel de Oncologia) data (since 2013).

    Args:
        `db_file (str)`: path to the duckdb file in which the data will be imported to.

        `years (list[int])`: list of years for which data will be imported (if available). Eg: `[2013, 2020]`
    """

    logging.info(f"⏳ [{MAIN_TABLE}] Starting import...")

    datasus.import_from_ftp(
        [MAIN_TABLE],
        [
            f"/dissemin/publicos/PAINEL_ONCOLOGIA/DADOS/POBR{utils.format_year(year, digits=4)}.dbc*"
            for year in years
        ],
        fetch_po,
        db_file=db_file,
    )


def fetch_po(ftp_path: str):
    df = ftp.fetch_dbc_as_df(ftp_path)
    return {MAIN_TABLE: map_po(df)}


def map_po(df: pl.DataFrame):
    df = df.with_columns(
        [
            pl.when(pl.col(pl.Utf8).str.len_chars() == 0)
            .then(None)
            .otherwise(pl.col(pl.Utf8))
            .name.keep(),
        ]
    )
    return to_schema(
        df,
        [
            Column("ANO_DIAGN", pl.UInt16),
            Column("ANOMES_DIA", pl.UInt32),
            Column("ANO_TRATAM", pl.UInt16),
            Column("ANOMES_TRA", pl.UInt32),
            Column("UF_RESID", pl.UInt8),
            Column("MUN_RESID", pl.UInt32),
            Column("UF_TRATAM", pl.UInt8),
            Column("MUN_TRATAM", pl.UInt32),
            Column("UF_DIAGN", pl.UInt8),
            Column("MUN_DIAG", pl.UInt32),
            Column("TRATAMENTO", pl.UInt8),
            Column("DIAGNOSTIC", pl.UInt8),
            Column("IDADE", pl.UInt8, strict=False),
            Column("SEXO", pl.Utf8),
            Column("ESTADIAM", pl.UInt8),
            Column("CNES_DIAG", pl.UInt32),
            Column("CNES_TRAT", pl.UInt32),
            Column("TEMPO_TRAT", pl.Utf8),
            Column("CNS_PAC", pl.Utf8),
            Column("DIAG_DETH", pl.Utf8),
            DateColumn("DT_DIAG", "%d/%m/%Y"),
            DateColumn("DT_TRAT", "%d/%m/%Y"),
            DateColumn("DT_NASC", "%d/%m/%Y"),
        ],
    )

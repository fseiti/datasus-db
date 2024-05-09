import polars as pl
import logging
from ..pl_utils import to_schema, Column, DateColumn
from ..datasus import import_from_ftp
from ..utils import format_year, format_month
from ..ftp import fetch_dbc_as_df

MAIN_TABLE = "SIA_PA"


def import_sia_pa(years=["*"], states=["*"], months=["*"]):
    """Import PA (Produção Ambulatorial) from SIASUS (Sistema de Informações Ambulatorial do SUS).

    Args:
        db_file (str, optional): path to the duckdb file in which the data will be imported to. Defaults to "datasus.db".
        years (list, optional): list of years for which data will be imported (if available). Eg: `[2012, 2000, 2010]`. Defaults to ["*"].
        states (list, optional): list of brazilian 2 letters state for which data will be imported (if available). Eg: `["SP", "RJ"]`. Defaults to ["*"].
        months (list, optional): list of months numbers (1-12) for which data will be imported (if available). Eg: `[1, 12, 6]`. Defaults to ["*"].

    ---

    Extra:
    - **Data description**: https://github.com/mymatsubara/datasus-db/blob/main/docs/sia_pa.pdf
    - **ftp path**: ftp.datasus.gov.br/dissemin/publicos/SIASUS/200801_/Dados/PA*.dbc
    """
    logging.info(f"⏳ [{MAIN_TABLE}] Starting import...")
    import_from_ftp(
        [
            f"/dissemin/publicos/SIASUS/200801_/Dados/PA{state.upper()}{format_year(year)}{format_month(month)}*.dbc"
            for year in years
            for state in states
            for month in months
        ]
    )


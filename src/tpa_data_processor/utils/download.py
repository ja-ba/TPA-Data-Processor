from datetime import datetime

import pandas as pd
from cloud_storage_wrapper.oci_access.config import OCI_Connection

# import polars as pl


pd.set_option("compute.use_bottleneck", False)
pd.set_option("compute.use_numexpr", False)


def _convert_date(someDate: datetime) -> tuple[str, str, str]:
    """_summary_

    Args:
        someDate (datetime): _description_

    Returns:
        tuple: _description_
    """

    # Format year by accessing the .year attribute of someDate
    year = str(someDate.year)

    # Format months as two digits
    month = "{:02d}".format(someDate.month)

    # Format days as two digits
    day = "{:02d}".format(someDate.day)

    return (year, month, day)


def download_file(
    someDate: datetime,
    dataset_type: str,
    pre_Link: str,
    post_Link: str,
    add_date_column: bool = False,
    verbose: bool = False,
):
    # Check dataset_type for correct content
    if dataset_type not in {"prices", "stations"}:
        raise ValueError("dataset_type must be either 'prices' or 'stations'")

    # Check for correct date
    if not isinstance(someDate, datetime):
        raise ValueError(
            "someDate is not of type datetime. Please pass a datetime object for someDate!"
        )

    # Convert date to year, month, day
    year, month, day = _convert_date(someDate)

    # Create download link
    dl_link = f"{pre_Link}/{dataset_type}/{year}/{month}/{year}-{month}-{day}-{dataset_type}.csv{post_Link}"

    # Download from dl_link --> 3 times
    try:
        current_df = pd.read_csv(dl_link, dtype_backend="pyarrow", engine="pyarrow")
    except Exception:
        try:
            current_df = pd.read_csv(dl_link, dtype_backend="pyarrow", engine="pyarrow")
        except Exception:
            try:
                current_df = pd.read_csv(
                    dl_link, dtype_backend="pyarrow", engine="pyarrow"
                )
            except Exception as ex:
                print(
                    ex,
                    f"ERROR: File for {year}-{month}-{day} couldn't be downloaded after 3 tries!",
                )

    # Add date column if add_date_column:
    if add_date_column:
        current_df["current_date"] = someDate

    # Log if requested
    if verbose:
        if len(current_df):
            print(f"SUCCESS: Downloaded df successfully for date {year}-{month}-{day}")
        else:
            print(
                f"WARNING: Attempted to download df for date {year}-{month}-{day} but got empty df"
            )

    return current_df


def download_multiple_files(
    dateList: list,
    dataset_type: str,
    pre_Link: str,
    post_Link: str,
    add_date_column: bool = False,
    verbose: bool = False,
) -> pd.DataFrame:
    # Iterate date list to download for each date
    df_list = [
        download_file(d, dataset_type, pre_Link, post_Link, add_date_column, verbose)
        for d in dateList
    ]

    # Return concatenated df
    return pd.concat(df_list, ignore_index=True)


def download_df(
    path: str,
    config_: OCI_Connection,
    columns: list[str] = [],
    df_format: str = "csv",
    verbose: bool = False,
) -> pd.DataFrame:
    if df_format not in {"csv", "parquet", "ftr"}:
        raise ValueError(
            f"{df_format} is an invalid df_format, please use one of the following: 'csv', 'parquet' or 'feather'"
        )

    if f".{df_format}" not in path:
        raise ValueError(
            f"The provided path {path} does not point to a file of format {df_format}"
        )

    ref_path = f"oci://{config_.bucket_name}@{config_.namespace}/{path}"

    if verbose:
        print(f"Attempting to download from {ref_path}")

    if df_format == "csv":
        if columns:
            return pd.read_csv(
                ref_path,
                usecols=columns,
                storage_options={"config": config_.config},
                dtype_backend="pyarrow",
                engine="pyarrow",
            )
        else:
            return pd.read_csv(
                ref_path,
                storage_options={"config": config_.config},
                dtype_backend="pyarrow",
                engine="pyarrow",
            )
    elif df_format == "parquet":
        if columns:
            return pd.read_parquet(
                ref_path,
                columns=columns,
                storage_options={"config": config_.config},
                dtype_backend="pyarrow",
                engine="pyarrow",
            )
        else:
            return pd.read_parquet(
                ref_path,
                storage_options={"config": config_.config},
                dtype_backend="pyarrow",
                engine="pyarrow",
            )
    else:
        if columns:
            return pd.read_feather(
                ref_path,
                columns=columns,
                storage_options={"config": config_.config},
                dtype_backend="pyarrow",
            )
        else:
            return pd.read_feather(
                ref_path,
                storage_options={"config": config_.config},
                dtype_backend="pyarrow",
            )

import pandas as pd


def upload_df(
    df: pd.DataFrame,
    path: str,
    format: str,
    config: dict,
    chunk_size: int = 5000,
    verbose: bool = False,
) -> None:
    """A helper function to write df to cloud storage in different formats ()

    Args:
        df (pd.DataFrame): The df to write to the cloud storage
        path (str): The path on the cloud storage to which to write the df
        format (str): The format in which to write the df to cloud storage
        config (dict): The config dict, containing the specification of the cloud storage
        chunk_size (int, optional): A chunk size by which to write feather files. Defaults to 5000.
        verbose (bool, optional): Whether to print output. Defaults to False.

    Raises:
        ValueError: If df is empty
        ValueError: if format is not given as 'csv', 'feather', 'ftr' or 'parquet'
    """
    if len(df) == 0:
        raise ValueError(f"df which should be written to {path} is empty")
    elif format not in {"csv", "parquet", "feather", "ftr"}:
        raise ValueError(
            f"format {format} was given, but only 'csv', 'feather', 'ftr' or 'parquet' allowed"
        )
    else:
        if verbose:
            print(f"writing to {path}")

        if format == "csv":
            df.to_csv(path, storage_options={"config": config})

        elif format == "parquet":
            df.to_parquet(
                path,
                engine="pyarrow",
                compression="snappy",
                storage_options={"config": config},
            )

        elif format in {"feather", "ftr"}:
            df.to_feather(
                path, storage_options={"config": config}, chunksize=chunk_size
            )

        if verbose:
            print(f"successfully written to {path}")

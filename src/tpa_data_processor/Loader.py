import gc
from datetime import datetime
from datetime import timedelta
from typing import Generator
from typing import Optional

import numpy as np
import pandas as pd
import psutil
import yaml  # type: ignore
from cloud_storage_wrapper.oci_access.config import create_OCI_Connection_from_dict
from cloud_storage_wrapper.oci_access.pandas import create_PandasOCI_from_dict
from pydantic import BaseModel
from tpa_data_processor.utils import date_helpers
from tpa_data_processor.utils import download
from tpa_data_processor.utils.map_class import Map_List

# Set string storage to pyarrow as default
pd.options.mode.string_storage = "pyarrow"


# Pydantic Config_Base
class Config_Base(BaseModel):
    existing_price_df_path: str
    existing_price_df_date_col: str
    existing_price_df_format: str
    target_price_df: str
    target_price_df_format: str
    wide_df: str
    wide_df_format: str
    wide_df_date_col: str
    station_df: str
    station_df_format: str
    station_365: str
    station_365_format: str
    station_30: str
    station_30_format: str
    df_weekly: str
    df_weekly_format: str
    week_mapper: str
    week_mapper_format: str
    map_directory: str
    oci_config: dict
    verbose: bool
    pre_Link_price: str
    post_Link_price: str
    pre_Link_stations: str
    post_Link_stations: str


def get_config(config_path: str) -> dict:
    """A shared function taking a path to the config YAML and returning the contents as a dictionary

    Args:
        config_path (str): The path to the config file or the content of the

    Returns:
        dict: A dictionary with the loaded config
    """

    with open(config_path, "r") as file:
        # Load the contents of the file
        configDict = yaml.safe_load(file)

    return Config_Base(**configDict).model_dump()

    # TODO: Change argument to Pathlib type


# Define chunk-creating function
def make_chunks(aList: list, n: int) -> Generator:
    """A helper function building n chunks from a provided list

    Args:
        xs (list): a list to chunk
        n (int): the number of chunks

    Yields:
        Generator: _description_
    """
    n = max(1, n)
    return (aList[i : i + n] for i in range(0, len(aList), n))


class DailyLoader:
    """A class performing the Daily Loading of gas station prices and station information"""

    def __init__(
        self,
        config_path: str,
        full_load: bool,
        verbose: bool = True,
        start_: Optional[datetime] = None,
        end_: Optional[datetime] = None,
    ) -> None:
        """Create an object of class DailyLoader to perform the loading of daily gas station prices

        Args:
            config_path (Path): Path to the config file
            full_load (bool): Whether to perform a full load or a delta load (False)
            verbose (bool, optional): Whether to print progress information. Defaults to True.
            start_ (Optional[datetime], optional): A datetime object defining the start date for data processing. This is only relevant for full_load=True and will be ignored otherwise. Defaults to None.
            end_ (Optional[datetime], optional): A datetime object defining the end date for data processing. This can be used independent of the full_load argument to limit how until when dates are processed. Defaults to None.
        """
        self.fullLoad = full_load
        self.start_ = start_
        self.end_ = end_
        self.verbose = verbose
        self.priceDF = pd.DataFrame()
        self.stationDF = pd.DataFrame()
        self.stationDF_reduced = pd.DataFrame()
        self.configDict = get_config(config_path)
        self.ociConfig = create_OCI_Connection_from_dict(self.configDict)
        self.ociPandasConfig = create_PandasOCI_from_dict(self.configDict)

        self.dateList: list = []

    def get_dates(self):
        """This function determines the dates for processing and stores them in the datelist attribute of the class

        Raises:
            ValueError: If fullLoad was set to True, but no start date was provided.
        """
        if not self.fullLoad:
            # If no fullLoad is requried, the existing_price_df has to be downloaded
            existing_price_df = self.ociPandasConfig.retrieve_df(
                path=self.configDict["existing_price_df_path"],
                df_format=self.configDict["existing_price_df_format"],
                columns=[self.configDict["existing_price_df_date_col"]],
            )

            # In the case that no end_ date is provided, we use get_time_interval_from_df() which creates the date list until yesterday
            if not self.end_:
                self.dateList = date_helpers.get_time_interval_from_df(
                    df=existing_price_df,
                    date_col=self.configDict["existing_price_df_date_col"],
                    verbose=self.verbose,
                )
            # If an end_ date is provided, we can pass this argument to get_time_interval_from_df()
            else:
                self.dateList = date_helpers.get_time_interval_from_df(
                    df=existing_price_df,
                    date_col=self.configDict["existing_price_df_date_col"],
                    verbose=self.verbose,
                    end_=self.end_,
                )
            # Since the red_df is only needed for get_time_interval_from_df() it is deleted after usage
            del existing_price_df

        # Otherwise, if a start_value was provided, the dateList is created via create_time_interval()
        elif self.start_:
            if not self.end_:
                self.dateList = date_helpers.create_time_interval(
                    start_=self.start_, verbose=self.verbose
                )
            else:
                self.dateList = date_helpers.create_time_interval(
                    start_=self.start_, verbose=self.verbose, end_=self.end_
                )

        # If fullLoad is false and start_ date is missing, throw an error
        else:
            raise ValueError("fullLoad was set to True, but no start date was provided")

        # Check if datelist is empty and raise ValueError if so
        if len(self.dateList) == 0 & self.verbose:
            print("Datelist was empty")

    def _get_price_df(self) -> tuple:
        """Downloads the individual price data points and combines them to the relevant data frame

        Returns:
            tuple: A tuple (current_df, existing_price_df) where current_df is the df with the new individual price df and existing_price_df is the previously existing price data in case of a delta load
        """
        # Ref df initialized to None, if not fullLoad (i.e. delta-load), we download the old price df
        existing_price_df = pd.DataFrame()
        if not self.fullLoad:
            existing_price_df = self.ociPandasConfig.retrieve_df(
                path=self.configDict["existing_price_df_path"],
                df_format=self.configDict["existing_price_df_format"],
            )

        # Download the price files according to self.dateList
        current_df = download.download_multiple_files(
            dateList=self.dateList,
            dataset_type="prices",
            pre_Link=self.configDict["pre_Link_price"],
            post_Link=self.configDict["post_Link_price"],
            add_date_column=False,
            verbose=self.configDict["verbose"],
        )

        # Convert to correct timezone
        current_df["date"] = current_df["date"].dt.tz_convert("Europe/Berlin")

        # Delete unneccesary columns
        current_df = current_df.drop(["dieselchange", "e5change", "e10change"], axis=1)
        gc.collect()

        # Change format of date
        current_df["date_converted"] = pd.to_datetime(
            current_df["date"].astype("str").str[0:19],
            errors="raise",
            utc=True,
        )

        # Make time-column with 30 minute intervals
        current_df["zeit_30"] = (
            current_df["date_converted"].dt.floor("30min").dt.time.astype("str")
        )

        # Convert date to string
        current_df["datum_dt"] = current_df["date_converted"].dt.date.astype("str")

        # delete original date since no longer needed
        del current_df["date"]
        gc.collect()

        if self.configDict["verbose"]:
            print("SUCCESS: Price data ready")

        return current_df.convert_dtypes(
            dtype_backend="pyarrow"
        ), existing_price_df.convert_dtypes(dtype_backend="pyarrow")

    def _get_station_df(self) -> tuple:
        """Downloads the individual station data points and combines them to the relevant data frame

        Returns:
            tuple: A tuple (current_df, existing_price_df) where current_df is the df with the new individual station df and existing_price_df is the previously existing station data in case of a delta load
        """
        existing_price_df = pd.DataFrame()
        if not self.fullLoad:
            existing_price_df = self.ociPandasConfig.retrieve_df(
                path=self.configDict["station_df"],
                df_format=self.configDict["station_df_format"],
            )

        current_df = download.download_multiple_files(
            dateList=self.dateList,
            dataset_type="stations",
            pre_Link=self.configDict["pre_Link_stations"],
            post_Link=self.configDict["post_Link_stations"],
            add_date_column=True,
            verbose=self.configDict["verbose"],
        )

        # Delete opening times since not needed
        del current_df["openingtimes_json"]

        if self.configDict["verbose"]:
            print("SUCCESS: Station data ready")

        return current_df.convert_dtypes(
            dtype_backend="pyarrow"
        ), existing_price_df.convert_dtypes(dtype_backend="pyarrow")

    def process_station_df(self) -> None:
        """A function to process the station data and coerce it into a df. The result is stored in self.stationDF"""
        # Load the stations and existing_price_df via _get_station_df() method
        stations, existing_price_df = self._get_station_df()

        if len(existing_price_df) > 0:
            # Extract uuid-short_id combinations from existing_price_df
            stations_alt = existing_price_df[["uuid", "short_id"]].copy()
            # Merge old uuid and short_id with new station list --> this identifies stations which don't have short_ids yet
            stations = stations.merge(stations_alt, how="left", on="uuid")

            # Identify last globally given short_id
            max_short_id = stations["short_id"].max()

            # Create df of all stations which don't have a short_id yet
            stations_without_shortid = pd.DataFrame(
                {"uuid": stations.loc[stations["short_id"].isna(), "uuid"].unique()}
            )
            # Build short_id on top if max_short_id for these stations
            stations_without_shortid["short_id"] = (
                stations_without_shortid.groupby(["uuid"]).ngroup() + max_short_id
            ).astype("int32")

            # Integrate these in original stations df
            for stat in list(stations_without_shortid.uuid):
                stations.loc[
                    stations["uuid"] == stat, "short_id"
                ] = stations_without_shortid.loc[
                    stations_without_shortid["uuid"] == stat, "short_id"
                ].values[  # type: ignore
                    0
                ]

            if self.configDict["verbose"]:
                print("SUCCESS: New Stations IDs created")

        else:
            stations["short_id"] = stations.groupby(["uuid"]).ngroup()

        # Create reduced stations df (just short_id, uuid and date) to join the short_id on df
        self.stationDF_reduced = stations[["uuid", "short_id", "current_date"]].copy()
        self.stationDF_reduced["current_date"] = self.stationDF_reduced[
            "current_date"
        ].astype("str")
        self.stationDF_reduced.rename(
            columns={"uuid": "station_uuid", "current_date": "datum_dt"}, inplace=True
        )

        # Determine recent date per station and join it on the stations df. Then filter on date==recent_date
        recent_date_per_station = stations.groupby("short_id", as_index=False)[
            "current_date"
        ].max()
        self.stationDF = recent_date_per_station.merge(
            stations, how="inner", on="short_id"
        )
        self.stationDF = self.stationDF[
            self.stationDF["current_date_x"] == self.stationDF["current_date_y"]
        ]

        # Add Tankstellen_Name Field as a combination of Name + Street + City
        self.stationDF["empty"] = ""
        self.stationDF["Tankstellen_Name"] = (
            self.stationDF["brand"].combine_first(self.stationDF["name"])
            + ", "
            + self.stationDF["street"].combine_first(self.stationDF["empty"])
            + " "
            + self.stationDF["house_number"].combine_first(self.stationDF["empty"])
            + ", "
            + self.stationDF["post_code"].combine_first(self.stationDF["empty"])
            + " "
            + self.stationDF["city"].combine_first(self.stationDF["empty"])
        )
        self.stationDF["Tankstellen_Name_block"] = (
            self.stationDF["brand"].combine_first(self.stationDF["name"])
            + " \n"
            + self.stationDF["street"].combine_first(self.stationDF["empty"])
            + " "
            + self.stationDF["house_number"].combine_first(self.stationDF["empty"])
            + " \n"
            + self.stationDF["post_code"].combine_first(self.stationDF["empty"])
            + " "
            + self.stationDF["city"].combine_first(self.stationDF["empty"])
        )

        # Delete not needed fields
        del self.stationDF["empty"]
        del self.stationDF["current_date_y"]
        del self.stationDF_reduced["datum_dt"]

        # Set data types to pyarrow
        self.stationDF_reduced = self.stationDF_reduced.convert_dtypes(
            dtype_backend="pyarrow"
        )

        # Change datatypes to save disk space
        self.stationDF["short_id"] = self.stationDF["short_id"].astype("int32")

        # Rename column
        self.stationDF.rename(columns={"current_date_x": "last_date"}, inplace=True)

        # Reset index for feather format
        self.stationDF.reset_index(drop=True, inplace=True)

        # Set data types to pyarrow
        self.stationDF = self.stationDF.convert_dtypes(dtype_backend="pyarrow")

        if self.configDict["verbose"]:
            print("SUCCESS: final station df created")

        # Clean up
        del stations
        gc.collect()

    def process_price_df(self):
        """A function to process the price data and coerce it into a df. The result is stored in self.priceDF"""
        current_df, existing_price_df = self._get_price_df()
        current_df = pd.merge(current_df, self.stationDF_reduced)

        # Goal: every half hour intervall between opening and closing should have a price
        min_datum = current_df["datum_dt"].min()  # Capture min datum in df
        max_datum = str(
            (pd.to_datetime(current_df["datum_dt"]).max() + timedelta(days=1)).date()
        )  # Capture max datum in df

        # Make df with 30 min time intervals for all dates between min_datum and max_datum
        dummy_30_min_df = pd.DataFrame(
            # {'Day_Hours': pd.date_range(min_datum, max_datum, freq='30min', inclusive='left')}
            {
                "Day_Hours": pd.date_range(
                    min_datum, max_datum, freq="30min", inclusive="left"
                )
            }
        ).convert_dtypes(dtype_backend="pyarrow")

        # Create additional columns
        dummy_30_min_df["datum_dt"] = dummy_30_min_df["Day_Hours"].dt.date.astype(
            "str"
        )  # Date as string for later joins
        dummy_30_min_df["zeit_30"] = dummy_30_min_df["Day_Hours"].dt.time.astype(
            "str"
        )  # time intervalls as string for later joins
        dummy_30_min_df["zeit_30_nr"] = dummy_30_min_df[
            "Day_Hours"
        ].dt.time  # time intervalls as dt.time for later comparison

        # Ignore these times when determining the opening times
        open_ignore = [
            "00:00:00",
            "00:30:00",
            "01:00:00",
            "01:30:00",
            "02:00:00",
            "02:30:00",
            "03:00:00",
            "03:30:00",
        ]

        # Merge with all short_ids in df
        dummy_30_min_df_tmp = dummy_30_min_df.merge(
            pd.DataFrame(data={"short_id": current_df["short_id"].unique()}),
            how="cross",
        ).convert_dtypes(dtype_backend="pyarrow")

        # Aggregate original df for 30 min intervalls
        df_aggregated_mean = current_df.groupby(
            by=["short_id", "datum_dt", "zeit_30"], group_keys=True, as_index=False
        )[["e5", "e10", "diesel"]].mean()
        df_aggregated_last = (
            current_df.sort_values(by=["short_id", "datum_dt", "zeit_30"])
            .groupby(
                by=["short_id", "datum_dt", "zeit_30"], group_keys=True, as_index=False
            )[["e5", "e10", "diesel"]]
            .last()
            .rename(
                columns={"e5": "e5_last", "e10": "e10_last", "diesel": "diesel_last"}
            )
        )

        # Merge on dummy_30_min_df_tmp (cartesian df)
        df_aggregated_extended = dummy_30_min_df_tmp.merge(
            df_aggregated_mean, how="left", on=["datum_dt", "zeit_30", "short_id"]
        ).merge(df_aggregated_last, how="left", on=["datum_dt", "zeit_30", "short_id"])

        ##Check if the station is open:
        # Create column is_open which is 1 if the station reported a price in one of the sorts
        df_aggregated_extended["is_open"] = np.where(
            (~df_aggregated_extended["e5"].isna()) & (df_aggregated_extended["e5"] > 0)
            | (~df_aggregated_extended["e10"].isna())
            & (df_aggregated_extended["e10"] > 0)
            | (~df_aggregated_extended["diesel"].isna())
            & (df_aggregated_extended["diesel"] > 0),
            True,
            False,
        )

        df_aggregated_extended_open_close = (
            df_aggregated_extended[
                (df_aggregated_extended["is_open"])
                & (~(df_aggregated_extended["zeit_30"].isin(open_ignore)))
            ]
            .groupby(by=["short_id", "datum_dt"], group_keys=True, as_index=False)
            .agg({"zeit_30_nr": ["min", "max"]})
        ).reset_index(drop=True)
        df_aggregated_extended_open_close.columns = [
            "_".join(i).rstrip("_")
            for i in df_aggregated_extended_open_close.columns.values
        ]

        # Delete station on dates with only one time
        df_aggregated_extended_open_close = df_aggregated_extended_open_close[
            df_aggregated_extended_open_close["zeit_30_nr_min"]
            != df_aggregated_extended_open_close["zeit_30_nr_max"]
        ]

        # Join this on the original df (inner join, so stations with only one time per date are deleted)
        df_aggregated_extended = df_aggregated_extended.merge(
            df_aggregated_extended_open_close, how="inner", on=["datum_dt", "short_id"]
        )

        # Delete entries out of min and max range
        df_aggregated_extended = df_aggregated_extended[
            (
                df_aggregated_extended["zeit_30_nr"]
                >= df_aggregated_extended["zeit_30_nr_min"]
            )
            & (
                df_aggregated_extended["zeit_30_nr"]
                <= df_aggregated_extended["zeit_30_nr_max"]
            )
        ]

        # Sort dataframe
        df_aggregated_extended.sort_values(
            by=["short_id", "Day_Hours"], axis=0, inplace=True
        )

        # Apply forward and backward filling for each price column
        for sorte in ("e5", "e10", "diesel"):
            # df_aggregated_extended[sorte].fillna(method="ffill", inplace=True)
            df_aggregated_extended[f"{sorte}_last"] = df_aggregated_extended.groupby(
                "short_id"
            )[f"{sorte}_last"].transform(
                lambda x: x.fillna(method="ffill")
            )  # fill nas forward within group (short_id)
            df_aggregated_extended[f"{sorte}_last"] = df_aggregated_extended.groupby(
                "short_id"
            )[f"{sorte}_last"].transform(
                lambda x: x.fillna(method="bfill")
            )  # fill nas backward within group (short_id)

            df_aggregated_extended[sorte] = df_aggregated_extended[sorte].fillna(
                df_aggregated_extended[f"{sorte}_last"]
            )

        # Delete temporary dfs which are no longer needed
        del dummy_30_min_df_tmp
        del df_aggregated_mean
        del df_aggregated_last
        del df_aggregated_extended_open_close
        gc.collect()

        if self.configDict["verbose"]:
            print("SUCCESS: Full price df created")

        # Delete unnecessary columns from final df
        df_aggregated_extended = df_aggregated_extended.drop(
            columns=[
                "zeit_30_nr_min",
                "zeit_30_nr_max",
                "is_open",
                "datum_dt",
                "zeit_30",
                "zeit_30_nr",
                "e5_last",
                "e10_last",
                "diesel_last",
            ]
        )

        # Change datatypes to save disk space
        df_aggregated_extended["short_id"] = df_aggregated_extended["short_id"].astype(
            "int32[pyarrow]"
        )
        df_aggregated_extended["e10"] = df_aggregated_extended["e10"].astype(
            "float[pyarrow]"
        )
        df_aggregated_extended["e5"] = df_aggregated_extended["e5"].astype(
            "float[pyarrow]"
        )
        df_aggregated_extended["diesel"] = df_aggregated_extended["diesel"].astype(
            "float[pyarrow]"
        )

        # Optimize memory
        gc.collect()

        # If this is not a full load, concatenate existing_price_df and df_aggregated_extended
        if not self.fullLoad:
            # Concatenate with new df
            self.priceDF = pd.concat(
                [existing_price_df, df_aggregated_extended], ignore_index=True
            )
        else:
            self.priceDF = df_aggregated_extended

        # Ensure all data types are pyarrow
        self.priceDF = self.priceDF.convert_dtypes(dtype_backend="pyarrow")

        # Clean up
        del current_df, existing_price_df, df_aggregated_extended
        gc.collect()

        if self.configDict["verbose"]:
            print("SUCCESS: Final df created")

    def process(self) -> None:
        """A function calling all the necessary steps (i.e. the class methods) in the correct order"""
        self.get_dates()
        if len(self.dateList) > 0:
            self.process_station_df()
            self.process_price_df()
        elif self.verbose:
            print("Skipping processing because datelist was empty")

    def save_dfs(self) -> None:
        """A function uploading all processed dfs (self.priceDF and self.stationDF) to cloud storage"""

        if len(self.dateList) > 0:
            # Upload price df:
            price_ref_path = f"oci://{self.configDict['oci_config']['bucket_name']}@{self.ociConfig.namespace}/{self.configDict['target_price_df']}"
            self.ociPandasConfig.write_df(
                df=self.priceDF,
                path=price_ref_path,
                df_format=self.configDict["target_price_df_format"],
                chunk_size=10000,
            )

            # Upload station df:
            station_ref_path = f"oci://{self.configDict['oci_config']['bucket_name']}@{self.ociConfig.namespace}/{self.configDict['station_df']}"
            self.ociPandasConfig.write_df(
                df=self.stationDF,
                path=station_ref_path,
                df_format=self.configDict["station_df_format"],
                chunk_size=10000,
            )


class DailyWideLoader:
    """A class performing the Daily Loading of the wide (i.e. one column per station and gas type) price data"""

    def __init__(
        self,
        config_path: str,
        verbose: bool = True,
    ) -> None:
        self.verbose = verbose
        self.priceDF = pd.DataFrame()
        self.Tankstellen_info = pd.DataFrame()
        self.Tankstellen_info_365 = pd.DataFrame()
        self.Tankstellen_info_30 = pd.DataFrame()
        self.df_wide_final = pd.DataFrame()
        self.short_ids_last_365: list = []
        self.dateList: list = []
        self.configDict = get_config(config_path)
        self.ociConfig = create_OCI_Connection_from_dict(self.configDict)
        self.ociPandasConfig = create_PandasOCI_from_dict(self.configDict)
        self.max_date: datetime = datetime(1970, 1, 1)
        self.min_date: datetime = datetime(1970, 1, 2)

        self.dateList = []

    def load_df_alt(self) -> None:
        """Loads the old price df for the last 365 days (with respect to the maximum date in the price df) --> This serves as a basis for the calculation of the wide df"""
        # Load df_alt
        self.priceDF = self.ociPandasConfig.retrieve_df(
            path=self.configDict["existing_price_df_path"],
            df_format=self.configDict["existing_price_df_format"],
        )

        # Reduce it to the last 365 days of data
        self.max_date = self.priceDF["Day_Hours"].max()
        self.min_date = self.max_date - timedelta(days=365)
        self.priceDF = self.priceDF[
            (self.priceDF["Day_Hours"] >= self.min_date)
            & (self.priceDF["Day_Hours"] <= self.max_date)
        ]

    def create_dateList(self) -> None:
        """Creates a date list based on self.max_date and self.min_date"""
        if self.verbose:
            print("date ranges from ", self.min_date, self.max_date)
        self.dateList = date_helpers.create_time_interval(
            start_=self.min_date, end_=self.max_date, verbose=self.verbose
        )

    def create_Tankstellen_info_365(self) -> None:
        """Builds a filtered version of Tankstellen_info with only stations that reported prices in the last 365 days"""
        # Identify stations with transactions in last 365 days
        self.short_ids_last_365 = self.priceDF["short_id"].unique()  # type: ignore

        # Load Tankstellen_info
        self.Tankstellen_info = self.ociPandasConfig.retrieve_df(
            path=self.configDict["station_df"],
            df_format=self.configDict["station_df_format"],
        )

        self.Tankstellen_info_365 = self.Tankstellen_info[
            self.Tankstellen_info.short_id.isin(self.short_ids_last_365)
        ].reset_index(drop=True)

    def create_Tankstellen_info_30(self):
        """Builds a filtered version of Tankstellen_info with only stations that reported prices in the last 30 days"""

        min_date = self.max_date - timedelta(days=30)

        df_alt_30days = self.priceDF[self.priceDF["Day_Hours"] >= min_date]

        # Count number of dates with data in last 30 days
        df_alt_30days["date"] = df_alt_30days["Day_Hours"].dt.date
        df_alt_30days_grouped = df_alt_30days.groupby("short_id", as_index=False)[
            "date"
        ].nunique()

        # Filter on short_ids with at least 2/3 days of data in the last 30 days compared to the station with most dates
        short_ids_last_30 = df_alt_30days_grouped.loc[
            df_alt_30days_grouped.date >= ((2 / 3) * df_alt_30days_grouped.date.max()),  # type: ignore
            "short_id",
        ].unique()  # type: ignore

        # Filter Tankstellen_info
        self.Tankstellen_info_30 = self.Tankstellen_info[
            self.Tankstellen_info.short_id.isin(short_ids_last_30)
        ].reset_index(drop=True)

    def create_wide_format(self) -> None:
        """Creates the wide format of the price DF by transforming self.priceDF to wide format"""

        # Initialize df_list and counter i
        df_list, i = [], 0

        # Make chunk list with 100 chunks
        chunk_list = list(
            make_chunks(
                list(self.short_ids_last_365), len(self.short_ids_last_365) // 100
            )
        )

        if self.verbose:
            print("STARTING Chunk processing...")

        # Loop over chunks
        for chunk in chunk_list:
            # Create df_temp which is filtered on the chunk and the dates
            df_temp = self.priceDF[(self.priceDF.short_id.isin(chunk))]

            # To wide format with index Day_Hours, columns stations and values sorts
            df_temp_wide = pd.pivot_table(
                df_temp,
                index=["Day_Hours"],
                columns="short_id",
                values=["e5", "e10", "diesel"],
            )

            # Adjust column names, since they are saved hierarchical
            df_temp_wide.columns = [
                a + "_" + str(b) for a, b in df_temp_wide.columns.values
            ]

            # Sort df according to index (here: Day_Hours)
            df_temp_wide.sort_index(inplace=True)

            # Append to df_list
            df_list.append(df_temp_wide)
            if self.verbose:
                print("...processed chunk", i)

            # Increment i, which is just used for printing the status
            i += 1

            # Print memory utilization
            print(
                "Memory availability: ",
                psutil.virtual_memory().available * 100 / psutil.virtual_memory().total,
            )

            # Delete unnecessary dfs
            del df_temp_wide
            del df_temp
            gc.collect()

        # Combine list into df_wide_final
        self.df_wide_final = pd.concat(
            df_list, axis=1, ignore_index=False
        ).reset_index()
        print("SUCCESS: Chunks combined")

    def process(self) -> None:
        """A function calling all the necessary steps (i.e. the class methods) in the correct order"""
        self.load_df_alt()
        self.create_dateList()
        self.create_Tankstellen_info_365()
        self.create_Tankstellen_info_30()
        self.create_wide_format()

    def save_dfs(self):
        """A function uploading all processed dfs (self.Tankstellen_info_30, self.Tankstellen_info_365 and self.df_wide_final_ref_path) to cloud storage"""
        # Upload Tankstellen_info_30:
        Tankstellen_info_30_ref_path = f"oci://{self.configDict['oci_config']['bucket_name']}@{self.ociConfig.namespace}/{self.configDict['station_30']}"
        self.ociPandasConfig.write_df(
            df=self.Tankstellen_info_30,
            path=Tankstellen_info_30_ref_path,
            df_format=self.configDict["station_30_format"],
            chunk_size=1000,
        )

        # Upload Tankstellen_info_365:
        Tankstellen_info_365_ref_path = f"oci://{self.configDict['oci_config']['bucket_name']}@{self.ociConfig.namespace}/{self.configDict['station_365']}"
        self.ociPandasConfig.write_df(
            df=self.Tankstellen_info_365,
            path=Tankstellen_info_365_ref_path,
            df_format=self.configDict["station_365_format"],
            chunk_size=1000,
        )

        # Upload df_wide_final:
        df_wide_final_ref_path = f"oci://{self.configDict['oci_config']['bucket_name']}@{self.ociConfig.namespace}/{self.configDict['wide_df']}"
        self.ociPandasConfig.write_df(
            df=self.df_wide_final,
            path=df_wide_final_ref_path,
            df_format=self.configDict["wide_df_format"],
            chunk_size=20000,
        )


class WeeklyLoader:
    """A class performing the Weekly aggregation of the price data"""

    def __init__(
        self,
        config_path: str,
        fullLoad: bool,
        verbose: bool = True,
    ) -> None:
        self.verbose = verbose
        self.fullLoad = fullLoad
        self.priceDF = pd.DataFrame()
        self.current_week: int = datetime.now().date().isocalendar()[1]
        self.current_year: int = datetime.now().date().isocalendar()[0]
        self.df_weekly_final = pd.DataFrame()
        self.df_weekly_old = pd.DataFrame()
        self.df_weekly_forMap = pd.DataFrame()
        self.week_mapper = pd.DataFrame()
        self.Tankstellen_info = pd.DataFrame()

        self.Tankstellen_info_365 = pd.DataFrame()
        self.Tankstellen_info_30 = pd.DataFrame()
        self.df_wide_final = pd.DataFrame()
        self.short_ids_last_365: list = []
        self.dateList: list = []
        self.configDict = get_config(config_path)
        self.ociConfig = create_OCI_Connection_from_dict(self.configDict)
        self.ociPandasConfig = create_PandasOCI_from_dict(self.configDict)
        self.max_date: datetime = datetime(1970, 1, 1)
        self.min_date: datetime = datetime(1970, 1, 2)

        self.dateList = []

    def load_df_alt(self) -> None:
        """Load the daily priceDF and the Tankstellen_info dataset"""
        # Load df_alt
        self.priceDF = self.ociPandasConfig.retrieve_df(
            path=self.configDict["existing_price_df_path"],
            df_format=self.configDict["existing_price_df_format"],
        )

        # Filter df_alt on last 400 days for performance improvement
        self.max_date = self.priceDF["Day_Hours"].max()
        self.min_date = self.max_date - timedelta(days=400)
        self.priceDF = self.priceDF[
            (self.priceDF["Day_Hours"] >= self.min_date)
            & (self.priceDF["Day_Hours"] <= self.max_date)
        ]

        # Update current_week and current_year
        self.current_week = self.max_date.date().isocalendar()[1]
        self.current_year = self.max_date.date().isocalendar()[0]

        # Add Day_Hours in py format
        self.priceDF["Day_Hours_py"] = self.priceDF["Day_Hours"].dt.to_pydatetime()

        # Load Tankstellen_info
        self.Tankstellen_info = self.ociPandasConfig.retrieve_df(
            path=self.configDict["station_df"],
            df_format=self.configDict["station_df_format"],
        )

        # Coalesce between brand and name
        self.Tankstellen_info["Name"] = self.Tankstellen_info["brand"].combine_first(
            self.Tankstellen_info["name"]
        )

    def create_weekly_date_format(self) -> None:
        """Create the weekly data frame by processing chunks"""

        list_of_ids = self.priceDF["short_id"].unique()
        chunk_list = list(make_chunks(list(list_of_ids), round(len(list_of_ids) / 3)))

        # Initialize empty df_list which will be filled through chunking
        df_list = []
        i = 0
        gc.collect()

        if self.verbose:
            print("STARTING Chunk processing...")

        # Loop over chunks
        for chunk in chunk_list:
            # Create df_temp which is filtered on the chunk
            df_temp = self.priceDF[self.priceDF.short_id.isin(chunk)]
            # Extract year as column in df
            df_temp["year"] = (
                df_temp["Day_Hours_py"].dt.isocalendar().year.astype("int32[pyarrow]")
            )
            # Filter based on new column for current and last year
            df_temp = df_temp.loc[
                df_temp["year"].isin([self.current_year, self.current_year - 1]), :
            ]

            # Extract week number as column in df
            df_temp["week"] = (
                df_temp["Day_Hours_py"].dt.isocalendar().week.astype("int32[pyarrow]")
            )

            # Replace weeks 0 with 1 as well as 52 with 53
            df_temp.loc[df_temp["week"] == 0, "week"] = 1
            df_temp.loc[df_temp["week"] == 53, "week"] = 52

            # Filter df on current and last week
            df_temp = df_temp.loc[
                df_temp["week"].isin([self.current_week, self.current_week - 1]), :
            ]
            # Aggregate prices (mean) by short_id, year and week
            df_agg_tmp = df_temp.groupby(
                ["short_id", "year", "week"], as_index=False
            ).agg({"e10": "mean", "e5": "mean", "diesel": "mean"})

            # Append aggregated df to df_list
            df_list.append(df_agg_tmp)
            if self.verbose:
                print("...processed chunk", i)
            i += 1
            if self.verbose:
                print(
                    "Memory availability: ",
                    psutil.virtual_memory().available
                    * 100
                    / psutil.virtual_memory().total,
                )

            # Delete dfs which are not needed anymore
            del df_agg_tmp
            del df_temp
            gc.collect()

        # delete old df
        self.priceDF = pd.DataFrame()
        gc.collect()
        if self.verbose:
            print("SUCCESS: Processed chunks")

        # Combine dfs in list and write to file
        self.df_weekly_final = pd.concat(df_list, axis=0, ignore_index=True)
        if self.verbose:
            print("SUCCESS: Chunks combined")

        # delete df_list
        del df_list
        gc.collect()

    def finalize_df_weekly(self):
        """If the class should not be processed as a fullLoad, concatenate it to the old df"""

        if not self.fullLoad:
            # Identify weeks and years in df_weekly_final
            all_weeks = list(self.df_weekly_final["week"].unique())
            all_years = list(self.df_weekly_final["year"].unique())

            # Load the old df
            self.df_weekly_old = self.ociPandasConfig.retrieve_df(
                path=self.configDict["df_weekly"],
                df_format=self.configDict["df_weekly_format"],
            )

            # Initiate delete variable which will flag rows to delete due to being in the new df
            self.df_weekly_old["delete"] = 0

            # Set delete variable to 1 for all weeks & year which exist in the new df
            self.df_weekly_old.loc[
                (self.df_weekly_old.week.isin(all_weeks))
                & (self.df_weekly_old.year.isin(all_years)),
                "delete",
            ] = 1

            # Delete all these entries (delete==0 means keep all where delete is not 1)
            self.df_weekly_old = self.df_weekly_old[self.df_weekly_old.delete == 0]

            # Remove delete variable since no longer needed
            del self.df_weekly_old["delete"]
            gc.collect()

            # Concatenate old and new
            self.df_weekly_final = pd.concat(
                [self.df_weekly_old, self.df_weekly_final], axis=0, ignore_index=True
            )

            if self.verbose:
                print("SUCCESS: Merged df_weekly with df_weekly_old")

    def create_week_mapper(self) -> None:
        """The week mapper table defines the 4 euqally distant weeks in the last 52 weeks. These are a basis for the HTMLs which are created later"""
        # Join Tankstellen_info on df_weekly_final to append name and filter on the needed columns
        self.df_weekly_final_forMap = self.df_weekly_final.join(
            self.Tankstellen_info.set_index("short_id"), on="short_id", rsuffix="__"
        )[
            [
                "short_id",
                "year",
                "week",
                "e10",
                "e5",
                "diesel",
                "latitude",
                "longitude",
                "Name",
            ]
        ]
        # Drop NAs
        self.df_weekly_final_forMap.dropna(inplace=True)
        # Create week total column by converting year & week to string, then concatenating them and finally converting the result back to string
        self.df_weekly_final_forMap["week_total"] = (
            self.df_weekly_final_forMap["year"].astype("str")
            + self.df_weekly_final_forMap["week"].astype("str").str.zfill(2)
        ).astype("int32")
        # Build a week string in the format W01/2022
        self.df_weekly_final_forMap["week_string"] = (
            "W"
            + self.df_weekly_final_forMap["week"]
            .astype("str")
            .str.zfill(2)  # apply(lambda x: f"{x:02d}")
            + "/"
            + self.df_weekly_final_forMap["year"].astype("str")
        )
        # Build a color column
        self.df_weekly_final_forMap["color_col"] = (
            self.df_weekly_final_forMap["e10"]
            + self.df_weekly_final_forMap["e5"]
            + self.df_weekly_final_forMap["diesel"]
        ) / 3
        # Sort the values according to the color column
        self.df_weekly_final_forMap.sort_values(by="color_col", inplace=True)

        if self.verbose:
            print(
                "SUCCESS: df_weekly_final prepared for map creation as df_weekly_final_forMap"
            )

        # Create a week mapper by setting the unique values of year and week
        self.week_mapper = self.df_weekly_final_forMap[
            ["year", "week", "week_total", "week_string"]
        ].drop_duplicates()
        self.week_mapper.sort_values(by="week_total", inplace=True, ascending=False)

        # make week mapper only contain every 13th week --> so that there are 4 weeks in the year for which the maps are created
        self.week_mapper = self.week_mapper[::13].head(4)
        # len(week_mapper)-53

        # Filter df_weekly_final on weeks in week_mapper
        self.df_weekly_final_forMap = self.df_weekly_final_forMap[
            self.df_weekly_final_forMap.week_total.isin(self.week_mapper.week_total)
        ]
        if self.verbose:
            print("SUCCESS: df_weekly_final filtered on weeks in week_mapper")

    def process(self) -> None:
        """A function calling all the necessary steps (i.e. the class methods) in the correct order"""

        self.load_df_alt()
        self.create_weekly_date_format()
        self.finalize_df_weekly()
        self.create_week_mapper()

    def save_dfs(self) -> None:
        """A function uploading all processed dfs (self.df_weekly_final, self.week_mapper) to cloud storage"""

        # Upload df_weekly_final:
        df_weekly_final_path = f"oci://{self.configDict['oci_config']['bucket_name']}@{self.ociConfig.namespace}/{self.configDict['df_weekly']}"
        self.ociPandasConfig.write_df(
            self.df_weekly_final,
            path=df_weekly_final_path,
            df_format=self.configDict["df_weekly_format"],
            chunk_size=10000,
        )

        # Upload week_mapper
        week_mapper_path = f"oci://{self.configDict['oci_config']['bucket_name']}@{self.ociConfig.namespace}/{self.configDict['week_mapper']}"
        self.ociPandasConfig.write_df(
            self.week_mapper,
            path=week_mapper_path,
            df_format=self.configDict["week_mapper_format"],
        )

    def create_upload_htmls(self) -> None:
        """Creates and uploads the HTMLs for the 4 weeks defined in week_mapper"""
        # First list all existing objects:
        list_existing_maps = self.ociConfig.list_files(
            prefix=self.configDict["map_directory"]
        )

        # For loop for deleting
        for map in list_existing_maps:
            self.ociConfig.delete_files(map.name)

        # Loop through sorte and week to create HTML and save it to bucket via upload_blob function
        for sorte in ("e5", "e10", "diesel"):
            test_map_list = Map_List(self.df_weekly_final_forMap, self.week_mapper)
            test_map_list.build_maps(color_col=sorte)

            for week in self.week_mapper["week_total"]:
                filename = f"{self.configDict['map_directory']}/Map_{sorte}_{week}.html"
                test_map_list.map_dict[week].to_html(outfile=filename)
                self.ociConfig.upload_file(file_to_upload=filename, file_name=filename)

                if self.verbose:
                    print("Created & uploaded map for ", sorte, " and week ", week)

        if self.verbose:
            print("SUCCESS: all HTMLs created & uploaded")

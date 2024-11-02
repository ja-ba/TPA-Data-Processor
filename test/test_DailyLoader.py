from datetime import datetime
from datetime import timedelta

import numpy as np
import pandas as pd
import pytest
from tpa_data_processor.Loader import DailyLoader


@pytest.fixture(scope="session")
def create_Full_Load_fixedRange(provide_oci_config):
    full_Load_DL = DailyLoader(
        config_path=provide_oci_config,
        full_load=True,
        verbose=True,
        start_=datetime(2024, 1, 1),
        end_=datetime(2024, 1, 11),
    )

    full_Load_DL.process()

    return full_Load_DL


@pytest.fixture(scope="session")
def Full_Load_fixedRange_raw_Prices(create_Full_Load_fixedRange):
    full_Load_DL = create_Full_Load_fixedRange

    return full_Load_DL._get_price_df()[0]


@pytest.fixture(scope="session")
def create_Full_Load_relativeRange(provide_oci_config):
    today_ = datetime.combine(datetime.today(), datetime.min.time())

    full_Load_rDL = DailyLoader(
        config_path=provide_oci_config,
        full_load=True,
        verbose=True,
        start_=today_ - timedelta(days=5),
    )

    full_Load_rDL.process()

    return full_Load_rDL


@pytest.fixture(scope="session")
def create_Delta_Load(provide_oci_config, create_Full_Load_fixedRange):
    create_Full_Load_fixedRange.save_dfs()

    today_ = datetime.combine(datetime(2024, 1, 19), datetime.min.time())

    delta_Load_DL = DailyLoader(
        config_path=provide_oci_config, full_load=False, verbose=True, end_=today_
    )

    delta_Load_DL.process()

    return delta_Load_DL


def test_date_range_dl(create_Full_Load_relativeRange, create_Full_Load_fixedRange):
    """This test checks, that after processing the correct length of datelists is saved"""
    DL_full_relativeRange, DL_full_fixedRange = (
        create_Full_Load_relativeRange,
        create_Full_Load_fixedRange,
    )

    assert len(DL_full_fixedRange.dateList) == 10
    assert len(DL_full_relativeRange.dateList) == 5

    assert DL_full_fixedRange.priceDF["Day_Hours"].dt.date.nunique() == len(
        DL_full_fixedRange.dateList
    )
    assert DL_full_relativeRange.priceDF["Day_Hours"].dt.date.nunique() == len(
        DL_full_relativeRange.dateList
    )


def test_dfs_filled_dl(create_Full_Load_relativeRange, create_Full_Load_fixedRange):
    """This test checks, that after processing the relevant data frames are not empty"""
    DL_full_relativeRange, DL_full_fixedRange = (
        create_Full_Load_relativeRange,
        create_Full_Load_fixedRange,
    )

    assert len(DL_full_fixedRange.priceDF) > 0
    assert len(DL_full_relativeRange.priceDF) > 0
    assert len(DL_full_fixedRange.stationDF) > 0
    assert len(DL_full_relativeRange.stationDF) > 0


@pytest.mark.slow
def test_save_dl(create_Full_Load_relativeRange, create_Full_Load_fixedRange):
    """This test checks, that the data frames can be saved"""
    DL_full_relativeRange, DL_full_fixedRange = (
        create_Full_Load_relativeRange,
        create_Full_Load_fixedRange,
    )

    DL_full_relativeRange.save_dfs()
    DL_full_fixedRange.save_dfs()


def test_agg_correct(Full_Load_fixedRange_raw_Prices, create_Full_Load_fixedRange):
    """This test checks, that the aggregation from arbitrary points in time to half hour intervals has worked"""
    priceDF = create_Full_Load_fixedRange.priceDF
    stationDF = create_Full_Load_fixedRange.stationDF

    raw_priceDF = Full_Load_fixedRange_raw_Prices.rename(
        columns={"station_uuid": "uuid"}
    ).merge(stationDF)
    raw_priceDF["Day_Hours"] = pd.to_datetime(
        raw_priceDF["datum_dt"] + " " + raw_priceDF["zeit_30"]
    )
    raw_priceDF = (
        raw_priceDF.groupby(["short_id", "Day_Hours"], as_index=False)[
            ["e5", "e10", "diesel"]
        ]
        .mean()
        .rename(columns={"e5": "e5_agg", "e10": "e10_agg", "diesel": "diesel_agg"})
    )
    raw_priceDF = raw_priceDF.convert_dtypes(dtype_backend="pyarrow")

    priceDF = priceDF.merge(raw_priceDF, how="inner", on=["short_id", "Day_Hours"])
    priceDF[["e5", "e10", "diesel", "e5_agg", "e10_agg", "diesel_agg"]] = priceDF[
        ["e5", "e10", "diesel", "e5_agg", "e10_agg", "diesel_agg"]
    ].fillna(-1000)

    assert len(
        priceDF[
            (np.isclose(priceDF.e5, priceDF.e5_agg, atol=1e-3))
            & (np.isclose(priceDF.e10, priceDF.e10_agg, atol=1e-3))
            & (np.isclose(priceDF.diesel, priceDF.diesel_agg, atol=1e-3))
        ]
    ) == len(priceDF)


def test_delta_load(create_Delta_Load, create_Full_Load_fixedRange):
    DL_full_fixedRange = create_Full_Load_fixedRange
    DL_delta = create_Delta_Load

    stations_overlap = DL_full_fixedRange.stationDF.merge(
        DL_delta.stationDF, how="inner", on="uuid", suffixes=("_full", "_delta")
    )
    price_overlap = DL_full_fixedRange.priceDF.merge(
        DL_delta.priceDF,
        how="inner",
        on=["short_id", "Day_Hours"],
        suffixes=("_full", "_delta"),
    )

    assert len(DL_delta.stationDF) > len(DL_full_fixedRange.stationDF)
    assert len(DL_delta.priceDF) > len(DL_full_fixedRange.priceDF)

    assert len(stations_overlap) == len(
        stations_overlap.query("short_id_full == short_id_delta")
    )
    assert len(
        price_overlap[
            (np.isclose(price_overlap.e5_full, price_overlap.e5_delta, atol=1e-3))
            & (np.isclose(price_overlap.e10_full, price_overlap.e10_delta, atol=1e-3))
            & (
                np.isclose(
                    price_overlap.diesel_full, price_overlap.diesel_delta, atol=1e-3
                )
            )
        ]
    ) == len(price_overlap)

    # Patch where to save to
    DL_delta.configDict["target_price_df"] = "BASE_df_DELTA.parquet"
    DL_delta.configDict["station_df"] = "LU_STATIONS_df_DELTA.ftr"

    # Then save:
    DL_delta.save_dfs()


def test_one_station_dl(create_Full_Load_fixedRange, create_Delta_Load):
    """Test that the correct values are present for one example station"""

    DL_full_fixedRange = create_Full_Load_fixedRange
    DL_delta = create_Delta_Load

    assert (
        DL_full_fixedRange.stationDF.query("short_id==12757")["brand"].item() == "SCORE"
    )
    assert DL_delta.stationDF.query("short_id==12757")["brand"].item() == "SCORE"

    assert len(DL_full_fixedRange.priceDF.query("short_id==12757")) == 361
    assert len(DL_delta.priceDF.query("short_id==12757")) == 650

    assert np.isclose(
        DL_full_fixedRange.priceDF.query("short_id==12757").iloc[-1, :]["diesel"],
        1.719,
        atol=1e-3,
    )
    assert np.isclose(
        DL_delta.priceDF.query("short_id==12757").iloc[-1, :]["diesel"],
        1.729,
        atol=1e-3,
    )

    assert np.isclose(
        DL_full_fixedRange.priceDF.query("short_id==12757")["diesel"].mean(),
        1.712,
        atol=1e-3,
    )
    assert np.isclose(
        DL_full_fixedRange.priceDF.query("short_id==12757")["e5"].mean(),
        1.794,
        atol=1e-3,
    )
    assert np.isclose(
        DL_full_fixedRange.priceDF.query("short_id==12757")["e10"].mean(),
        1.734,
        atol=1e-3,
    )

    assert np.isclose(
        DL_delta.priceDF.query("short_id==12757")["diesel"].mean(), 1.717, atol=1e-3
    )
    assert np.isclose(
        DL_delta.priceDF.query("short_id==12757")["e5"].mean(), 1.795, atol=1e-3
    )
    assert np.isclose(
        DL_delta.priceDF.query("short_id==12757")["e10"].mean(), 1.735, atol=1e-3
    )

    # For safety another station
    assert len(DL_full_fixedRange.stationDF.query("short_id==3679")) == 1
    assert len(DL_full_fixedRange.priceDF.query("short_id==3679")) == 344
    assert (
        len(DL_full_fixedRange.priceDF.query("short_id==3679").dropna(how="all")) == 344
    )

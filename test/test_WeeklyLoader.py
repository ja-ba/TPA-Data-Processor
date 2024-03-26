import numpy as np
import pytest
from tpa_data_processor.Loader import WeeklyLoader


@pytest.fixture(scope="session")
def create_fullLoad_WL(provide_oci_config):
    full_Load_WL = WeeklyLoader(
        config_path=provide_oci_config, verbose=True, fullLoad=True
    )
    full_Load_WL.process()
    return full_Load_WL


@pytest.fixture(scope="session")
def create_deltaLoad_WL(provide_oci_config):
    delta_Load_WL = WeeklyLoader(
        config_path=provide_oci_config, verbose=True, fullLoad=False
    )
    delta_Load_WL.configDict["existing_price_df_path"] = "BASE_df_DELTA.parquet"
    delta_Load_WL.configDict["target_station_df"] = "LU_STATIONS_df_DELTA.ftr"

    delta_Load_WL.process()
    return delta_Load_WL


def test_date_range_wl(create_fullLoad_WL, create_deltaLoad_WL):
    """This test checks, that after processing the correct length of datelists is saved"""
    full_WL = create_fullLoad_WL
    delta_WL = create_deltaLoad_WL

    assert full_WL.current_year == 2024
    assert full_WL.current_week == 2
    assert delta_WL.current_year == 2024
    assert delta_WL.current_week == 3

    assert full_WL.week_mapper["year"].values[0] == 2024
    assert full_WL.week_mapper["week"].values[0] == 2
    assert full_WL.week_mapper["week_total"].values[0] == 202402
    assert full_WL.week_mapper["week_string"].values[0] == "W02/2024"
    assert delta_WL.week_mapper["year"].values[0] == 2024
    assert delta_WL.week_mapper["week"].values[0] == 3
    assert delta_WL.week_mapper["week_total"].values[0] == 202403
    assert delta_WL.week_mapper["week_string"].values[0] == "W03/2024"

    assert len(full_WL.df_weekly_final.query("short_id==12757")) == 2
    assert len(delta_WL.df_weekly_final.query("short_id==12757")) == 3


def test_dfs_filled_wl(create_fullLoad_WL, create_deltaLoad_WL):
    """This test checks, that after processing the relevant data frames are not empty"""
    full_WL = create_fullLoad_WL
    delta_WL = create_deltaLoad_WL

    assert len(full_WL.df_weekly_final) > 0
    assert len(full_WL.week_mapper) > 0

    assert len(delta_WL.df_weekly_final) > 0
    assert len(delta_WL.week_mapper) > 0


@pytest.mark.slow
def test_save_wl(create_fullLoad_WL, create_deltaLoad_WL):
    """This test checks, that the data frames can be saved"""
    full_WL = create_fullLoad_WL
    delta_WL = create_deltaLoad_WL

    delta_WL.save_dfs()
    full_WL.save_dfs()


def test_one_station_wl(create_fullLoad_WL, create_deltaLoad_WL):
    """Test that the correct values are present for one example station"""

    full_WL = create_fullLoad_WL
    delta_WL = create_deltaLoad_WL

    assert np.isclose(
        full_WL.df_weekly_final.query("short_id==12757 & week==1")["e10"].values[0],
        1.742,
        atol=1e-3,
    )
    assert np.isclose(
        full_WL.df_weekly_final.query("short_id==12757 & week==1")["e5"].values[0],
        1.802,
        atol=1e-3,
    )
    assert np.isclose(
        full_WL.df_weekly_final.query("short_id==12757 & week==1")["diesel"].values[0],
        1.717,
        atol=1e-3,
    )

    assert np.isclose(
        full_WL.df_weekly_final.query("short_id==12757 & week==2")["e10"].values[0],
        1.717,
        atol=1e-3,
    )
    assert np.isclose(
        full_WL.df_weekly_final.query("short_id==12757 & week==2")["e5"].values[0],
        1.777,
        atol=1e-3,
    )
    assert np.isclose(
        full_WL.df_weekly_final.query("short_id==12757 & week==2")["diesel"].values[0],
        1.702,
        atol=1e-3,
    )

    assert np.isclose(
        delta_WL.df_weekly_final.query("short_id==12757 & week==1")["e10"].values[0],
        1.742,
        atol=1e-3,
    )
    assert np.isclose(
        delta_WL.df_weekly_final.query("short_id==12757 & week==1")["e5"].values[0],
        1.802,
        atol=1e-3,
    )
    assert np.isclose(
        delta_WL.df_weekly_final.query("short_id==12757 & week==1")["diesel"].values[0],
        1.717,
        atol=1e-3,
    )

    assert np.isclose(
        delta_WL.df_weekly_final.query("short_id==12757 & week==2")["e10"].values[0],
        1.727,
        atol=1e-3,
    )
    assert np.isclose(
        delta_WL.df_weekly_final.query("short_id==12757 & week==2")["e5"].values[0],
        1.787,
        atol=1e-3,
    )
    assert np.isclose(
        delta_WL.df_weekly_final.query("short_id==12757 & week==2")["diesel"].values[0],
        1.712,
        atol=1e-3,
    )

    assert np.isclose(
        delta_WL.df_weekly_final.query("short_id==12757 & week==3")["e10"].values[0],
        1.740,
        atol=1e-3,
    )
    assert np.isclose(
        delta_WL.df_weekly_final.query("short_id==12757 & week==3")["e5"].values[0],
        1.800,
        atol=1e-3,
    )
    assert np.isclose(
        delta_WL.df_weekly_final.query("short_id==12757 & week==3")["diesel"].values[0],
        1.726,
        atol=1e-3,
    )


def test_html(create_fullLoad_WL, create_deltaLoad_WL):
    full_WL = create_fullLoad_WL
    delta_WL = create_deltaLoad_WL

    # Delete the existing files in map_directory:
    for file in full_WL.ociConfig.list_files(
        prefix=full_WL.configDict["map_directory"]
    ):
        full_WL.ociConfig.delete_files(file.name)
    # Upload for full_WL
    full_WL.create_upload_htmls()

    assert (
        len(full_WL.ociConfig.list_files(prefix=full_WL.configDict["map_directory"]))
        == 3
    )

    # Delete the existing files in map_directory:
    for file in delta_WL.ociConfig.list_files(
        prefix=delta_WL.configDict["map_directory"]
    ):
        delta_WL.ociConfig.delete_files(file.name)
    # Upload for delta_WL
    delta_WL.create_upload_htmls()

    assert (
        len(delta_WL.ociConfig.list_files(prefix=delta_WL.configDict["map_directory"]))
        == 3
    )

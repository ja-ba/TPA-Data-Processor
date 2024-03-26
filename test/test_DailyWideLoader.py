import numpy as np
import pytest
from tpa_data_processor.Loader import DailyWideLoader


@pytest.fixture(scope="session")
def create_DWL(provide_oci_config):
    full_Load_DWL = DailyWideLoader(
        config_path=provide_oci_config,
        verbose=True,
    )
    full_Load_DWL.process()
    return full_Load_DWL


def test_date_range_dwl(create_DWL):
    """This test checks, that after processing the correct length of datelists is saved"""
    DWL = create_DWL

    assert len(DWL.dateList) == 365
    assert DWL.priceDF["Day_Hours"].dt.date.nunique() == 10


def test_dfs_filled_dwl(create_DWL):
    """This test checks, that after processing the relevant data frames are not empty"""
    DWL = create_DWL

    assert len(DWL.df_wide_final) > 0
    assert len(DWL.Tankstellen_info_365) > 0
    assert len(DWL.Tankstellen_info_30) > 0


@pytest.mark.slow
def test_save_dwl(create_DWL):
    """This test checks, that the data frames can be saved"""
    DWL = create_DWL

    DWL.save_dfs()


def test_one_station_dwl(create_DWL):
    """Test that the correct values are present for one example station"""
    DWL = create_DWL

    assert DWL.Tankstellen_info_30.query("short_id==12757")["brand"].item() == "SCORE"
    assert DWL.Tankstellen_info_365.query("short_id==12757")["brand"].item() == "SCORE"

    assert np.isclose(DWL.df_wide_final["diesel_12757"].mean(), 1.712, atol=1e-3)
    assert np.isclose(DWL.df_wide_final["e5_12757"].mean(), 1.794, atol=1e-3)
    assert np.isclose(DWL.df_wide_final["e10_12757"].mean(), 1.734, atol=1e-3)
    assert np.isclose(
        DWL.df_wide_final.dropna(axis=0, subset=["diesel_12757"]).iloc[-1, :][
            "diesel_12757"
        ],
        1.719,
        atol=1e-3,
    )

    assert DWL.df_wide_final["diesel_12757"].count() == 361
    assert DWL.df_wide_final["diesel_3679"].count() == 344

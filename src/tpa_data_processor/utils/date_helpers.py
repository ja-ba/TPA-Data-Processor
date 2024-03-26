from datetime import datetime
from datetime import timedelta

import pandas as pd


def get_time_interval_from_df(
    df: pd.DataFrame,
    date_col: str,
    end_: datetime = datetime.today(),
    verbose: bool = False,
) -> list[datetime]:
    # What is th last available date in the df --> make the start_ variable by adding one day
    last_df_date: datetime = df[date_col].max()
    start_ = last_df_date + timedelta(days=1)
    print(start_)

    print(last_df_date)
    # Initiate empty day list:
    date_list = []

    # Append dates to list
    for i in range((end_ - start_).days):
        day = start_ + timedelta(days=i)
        date_list.append(day)

    if verbose:
        print("Days to process: ", date_list)

    return date_list


def create_time_interval(
    start_: datetime, end_: datetime = datetime.today(), verbose: bool = False
):
    # Initiate empty day list:
    date_list = []

    # Set start to the start of the day, even when start_ has a time during the day
    start_ = datetime.combine(start_, datetime.min.time())

    # Append dates to list
    for i in range((end_ - start_).days):
        day = start_ + timedelta(days=i)
        date_list.append(day)

    if verbose:
        print("Days to process: ", date_list)

    return date_list

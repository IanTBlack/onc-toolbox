import os
from datetime import datetime, timedelta
from netrc import netrc
import numpy as np
import pandas as pd
import xarray as xr


def dt2str(dt: datetime) -> str:
    """
    Convert a Pythonic datetime object to a string that is compatible with the ONC Oceans 3.0 API dateFrom and dateTo
    API query parameters.

    :param dt: A Python datetime object.
    :return: An ISO8601 formatted string representing the datetime.
    """
    dt = pd.to_datetime(dt)
    dtstr = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    return dtstr


def get_onc_token(netrc_path: os.PathLike | None = None) -> str:
    """
    Retrieve an ONC token from the password portion of a .netrc file entry.

        machine data.oceannetworks.ca
        login <username>
        password <onc_token>

    :param netrc_path: The path to the .netrc file.
        If None, the netrc module looks for a .netrc file in the user directory.
    :return: The ONC token as a string. DO NOT SHARE OR PRINT THIS TOKEN.
    """

    if netrc_path is None:
        _, __, onc_token = netrc().authenticators("data.oceannetworks.ca")
    else:
        _, __, onc_token = netrc(netrc_path).authenticators("data.oceannetworks.ca")
    return onc_token


def nan_onc_flags(ds: xr.Dataset, flags_to_nan: list[int] = [4]) -> xr.Dataset:
    """
    Set corresponding data values to NaN if the flag is in flags_to_nan.

    :param ds: The input xarray Dataset. The dataset must contain data variables and matching flag variables prepended
        with 'flag_'.
    :param flags_to_nan: A list of integer flag values to set to NaN if they are present.
    :return: The modified xarray Dataset with data values set to NaN where the corresponding flag is in flags_to_nan.
    """

    flag_vars = [v for v in ds.data_vars if v.startswith('flag_')]
    if len(flag_vars) != 0:
        for fv in flag_vars:
            dv = fv.replace('flag_', '')
            if dv in ds.data_vars:
                ds[dv] = ds[dv].where(~ds[fv].isin([flags_to_nan]), np.nan)
    return ds


def remove_onc_flag_vars(ds: xr.Dataset) -> xr.Dataset:
    """
    Remove all flag variables from an xarray Dataset.
    Usually this can be implemented after using nan_onc_flags to set bad data to NaN, or if you don't care about the
    flag output.

    :param ds: The input xarray Dataset.
    :return: A modified xarray Dataset with all flag variables removed.
    """

    flag_vars = [v for v in ds.data_vars if v.startswith('flag_')]
    if len(flag_vars) != 0:
        ds = ds.drop_vars(flag_vars, errors = 'ignore')
    return ds


def split_periods(da_time: xr.DataArray, min_gap: int = 60 * 5) -> list[dict]:
    """
    Split a time series into periods of time containing data with a specified minimum gap in between each period.

    :param da_time: A time-indexed xarray DataArray.
    :param min_gap: The minimum number of seconds between data points that constitutes a break in the time series.
    :return: A list of dictionaries with 'begin_datetime' and 'end_datetime' keys for each period.
    """

    # First sort the data by time if it isn't already sorted.
    da_time = da_time.sortby('time')

    # Identify periods of time where data are separated by more than the # of seconds specified by `separation_time`.
    dts = list(da_time.where(da_time['time'].diff('time') > np.timedelta64(min_gap, 's'), drop=True).get_index('time'))

    if da_time.time.min() != dts[0]:
        dts = [pd.to_datetime(da_time.time.min().values)] + dts

    periods = []
    for dt in dts:
        if dt == dts[-1]:
            start = dt
            stop = None
        else:
            dtidx = dts.index(dt)
            start = dt
            stop = dts[dtidx + 1] - timedelta(seconds=30)  # Look back by 30 seconds to ensure we do not capture next.
        period = da_time.sel(time=slice(start, stop))
        if len(period.time.values) == 0:
            continue
        else:
            _p = {'begin_datetime': pd.to_datetime(period.time.min().values),
                  'end_datetime': pd.to_datetime(period.time.max().values)}
            periods.append(_p)
    if len(periods) == 0:
        _p = {'begin_datetime': pd.to_datetime(da_time.time.min().values),
              'end_datetime': pd.to_datetime(da_time.time.max().values)}
        periods = [_p]
    return periods



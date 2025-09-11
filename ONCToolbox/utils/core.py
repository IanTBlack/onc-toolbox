from datetime import timedelta
import numpy as np
import pandas as pd
import xarray as xr


def split_periods(da: xr.DataArray, min_gap: int = 60 * 5) -> list[dict]:
    """
    Split a time series into periods of time containing data with a specified
        minimum gap in between each period.

    :param da: A time-indexed xarray DataArray.
    :param min_gap: The minimum number of seconds between data points that constitutes
        a break in the time series.
    :return: A list of dictionaries with 'date_from' and 'date_to' keys for each period.
    """

    # First sort the data by time if it isn't already sorted.
    da = da.sortby('time')

    dts = list(da.where(da['time'].diff('time') >
                             np.timedelta64(min_gap, 's'), drop=True).get_index('time'))

    if da.time.min() != dts[0]:
        dts = [pd.to_datetime(da.time.min().values)] + dts

    periods = []
    for dt in dts:
        if dt == dts[-1]:
            start = dt
            stop = None
        else:
            dtidx = dts.index(dt)
            start = dt
            stop = dts[dtidx + 1] - timedelta(seconds=30)
        period = da.sel(time=slice(start, stop))
        if len(period.time.values) == 0:
            continue
        else:
            _p = {'date_from': pd.to_datetime(period.time.min().values),
                  'date_to': pd.to_datetime(period.time.max().values)}
            periods.append(_p)
    if len(periods) == 0:
        _p = {'date_from': pd.to_datetime(da.time.min().values),
              'date_to': pd.to_datetime(da.time.max().values)}
        periods = [_p]
    return periods



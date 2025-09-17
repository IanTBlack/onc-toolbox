import numpy as np
import xarray as xr

FLAG_DTYPE = 'int8'


class FLAG:
    NOT_EVALUATED: int = 0
    OK: int = 1
    PROBABLY_OK: int = 2
    PROBABLY_BAD: int = 3
    BAD: int = 4
    MISSING_DATA: int = 9

def flat_line_test(data: xr.DataArray,
                   fail_window_size: int = 5,
                   suspect_window_size: int = 3,
                   max_allowed_std: float = 0) -> xr.DataArray:
    """
    Perform a modified version of the QARTOD Flat Line Test.

    :param data: The input dataset.
    :param fail_window_size: The maximum number of consecutive samples that need
        to be within a certain standard deviation to be flagged as bad.
    :param suspect_window_size: The maximum number of consecutive samples that need
        to be within a certain standard deviation to be flagged as probably bad.
    :param max_allowed_std: The maximum standard deviation within the window
        to be considered a flat line.
    :return: An xr.DataArray of flags with the same shape as the input data.
    """

    # Fail Window Construction
    wf = data.rolling({'time': fail_window_size}).construct('window')
    wf_std = wf.std(dim='window')

    # Suspect Window Construction
    ws = data.rolling({'time': suspect_window_size}).construct('window')
    ws_std = ws.std(dim='window')

    flag = xr.full_like(data, FLAG.NOT_EVALUATED, dtype=FLAG_DTYPE)
    flag = xr.where(ws_std <= max_allowed_std, FLAG.PROBABLY_BAD, flag)
    flag = xr.where(wf_std <= max_allowed_std, FLAG.BAD, flag)
    flag = xr.where(flag == FLAG.NOT_EVALUATED, FLAG.OK, flag)

    flag.attrs['ancillary_variables'] = data.name

    return flag


def location_test(latitude: xr.DataArray, longitude: xr.DataArray,
                  latitude_min: float | None = None,
                  latitude_max: float | None = None,
                  longitude_min: float | None = None,
                  longitude_max: float | None = None) -> xr.DataArray:

    # Assign NOT_EVALUATED by default.
    flag = xr.full_like(latitude, fill_value=FLAG.NOT_EVALUATED).astype(FLAG_DTYPE)

    # Flag data as okay if it is within the confines of reality.
    flag = flag.where((np.abs(latitude) > 90) & (np.abs(longitude) > 180), FLAG.OK)

    # Apply optional user defined bounds and flag as probably bad if outside those bounds.
    if latitude_min is not None:
        flag = flag.where(latitude < latitude_min, FLAG.PROBABLY_BAD)
    if latitude_max is not None:
        flag = flag.where(latitude > latitude_max, FLAG.PROBABLY_BAD)
    if longitude_min is not None:
        flag = flag.where(longitude < longitude_min, FLAG.PROBABLY_BAD)
    if longitude_max is not None:
        flag = flag.where(longitude > longitude_max, FLAG.PROBABLY_BAD)

    # Flag bad data if it is outside the confines of reality.
    flag = flag.where((np.abs(latitude) < 90) | (np.abs(longitude) < 180), FLAG.BAD)

    # Set to nan if missing.
    flag = flag.where(~np.isnan(latitude) | ~np.isnan(longitude), FLAG.MISSING_DATA)
    return flag



def gross_range_test(data: xr.DataArray,
                     sensor_min: float, sensor_max: float,
                     operator_min: float or None = None,
                     operator_max: float or None = None) -> xr.DataArray:

    flag = xr.full_like(data, fill_value=FLAG.NOT_EVALUATED).astype(FLAG_DTYPE)

    flag = flag.where((data < sensor_min) & (data > sensor_max), FLAG.OK)
    flag = flag.where((data > sensor_min) | (data < sensor_max), FLAG.BAD)

    if operator_min is not None:
        if sensor_min != operator_min:
             flag = flag.where((data > operator_min) | (data < sensor_min),
                               FLAG.PROBABLY_BAD)
    if operator_max is not None:
        if sensor_max != operator_max:
            flag = flag.where((data < operator_max) | (data > sensor_max),
                              FLAG.PROBABLY_BAD)

    flag = flag.where(~np.isnan(data), FLAG.MISSING_DATA)

    return flag



# 
# 
# def spike_test(data: xr.DataArray, spike_half_window: int = 1, std_half_window: int = 15,
#                low_multiplier: float = 3, high_multiplier: float = 5):
# 
#     data = data.sortby('time')
# 
#     spkref_windows = data.rolling({'time': spike_half_window * 2 + 1}, min_periods=1).construct('window')
#     spkref_left = spkref_windows[:, 0]
#     spkref_right = spkref_windows[:, -1]
#     spkref = (spkref_left + spkref_right) / 2
# 
#     sd = data.rolling({'time': std_half_window * 2 + 1}, center=True, min_periods=1).std()
#     threshold_low = low_multiplier * sd
#     threshold_high = high_multiplier * sd
# 
#     flag = xr.full_like(data, FLAG.NOT_EVALUATED).astype('int8')
#     flag = flag.where(~(np.abs(data - spkref) < threshold_low) & ~(np.abs(data - spkref) > threshold_high),
#                             FLAG.OK)
#     flag = flag.where((np.abs(data - spkref) < threshold_low) | (np.abs(data - spkref) > threshold_high),
#                             FLAG.HIGH_INTEREST)
#     flag = flag.where(~(np.abs(data - spkref) > threshold_high), FLAG.BAD)
#     flag = flag.where((~np.isnan(data)), FLAG.MISSING_DATA)
#     flag = flag.where((~np.isnan(spkref)), FLAG.MISSING_DATA)
#     flag = flag.where((~np.isnan(threshold_low)) | (~np.isnan(threshold_high)), FLAG.NOT_EVALUATED)
# 
# 
#     return flag
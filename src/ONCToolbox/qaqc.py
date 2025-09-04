import numpy as np
import xarray as xr

class FLAG:
    NOT_EVALUATED: int = 0
    PASS: int = 1
    HIGH_INTEREST: int = 2
    SUSPECT: int = 3
    FAIL: int = 4
    MISSING_DATA: int = 9


def location_test(latitude: xr.DataArray, longitude: xr.DataArray) -> xr.DataArray:
    """
    Determine if a given latitude and longitude are valid. This is a modified version of the QARTOD location test.

    :param latitude: xr.DataArray of lat values with a 'time' coordinate.
    :param longitude: xr.DataArray of lon values with a 'time' coordinate that has the same timestamps as the lat.
    :return: xr.DataArray of test flag.
    """
    flag = xr.full_like(latitude, fill_value=FLAG.NOT_EVALUATED).astype('int8')  # Assign NOT_EVALUATED by default.
    flag = flag.where((np.abs(latitude) < 90) | (np.abs(longitude) < 180), FLAG.FAIL)
    flag = flag.where((np.abs(latitude) > 90) & (np.abs(longitude) > 180), FLAG.PASS)
    flag = flag.where(~np.isnan(latitude) | ~np.isnan(longitude), FLAG.MISSING_DATA)  # Set to nan if missing.

    # flag.attrs['test_name'] = 'QARTOD Location Test'
    # flag.attrs[
    #     'description'] = """The QARTOD Location Test verifies if a given latitude and longitude for an associated data
    #     point are within the confines of reality. Reality is defined as -90 to 90 for latitude and -180 to 180 for
    #     longitude. Implementation of this test does not currently support the flagging of suspect values."""
    return flag


def gross_range_test(data: xr.DataArray,
                     sensor_min: float, sensor_max: float,
                     operator_min: float or None = None,
                     operator_max: float or None = None) -> xr.DataArray:
    """
    Determine if data are within appropriate sensor or operator defined ranges.

    :param data: The input data.
    :param sensor_min: The minimum value the sensor can observe.
    :param sensor_max: The maximum value the sensor can observe.
    :param operator_min: An operator/user defined minimum value. Example: Minimum factory calibrated value.
    :param operator_max: An operator/user defined maximum value. Example: Maximum factory calibrated value.
    :return: An xr.DataArray of flag.
    """

    flag = xr.full_like(data, fill_value=FLAG.NOT_EVALUATED).astype('int8')

    flag = flag.where((data < sensor_min) & (data > sensor_max), FLAG.PASS)
    flag = flag.where((data > sensor_min) | (data < sensor_max), FLAG.FAIL)

    if operator_min is not None:
        if sensor_min != operator_min:
             flag = flag.where((data > operator_min) | (data < sensor_min), FLAG.SUSPECT)
    if operator_max is not None:
        if sensor_max != operator_max:
            flag = flag.where((data < operator_max) | (data > sensor_max), FLAG.SUSPECT)

    flag = flag.where(~np.isnan(data), FLAG.MISSING_DATA)

    # flag.attrs['test_name'] = f'QARTOD Gross Range Test - {data.name}'
    #
    # flag.attrs[
    #     'description'] = """The QARTOD Gross Range Test verifies if a given data point is within the limits defined
    #     by the sensor manufacturer (and operator)."""
    #
    # flag.attrs['test_sensor_min'] = sensor_min
    # flag.attrs['test_sensor_max'] = sensor_max
    # if operator_min is not None:
    #     flag.attrs['test_operator_min'] = operator_min
    # else:
    #     flag.attrs['test_operator_min'] = 'None'
    # if operator_max is not None:
    #     flag.attrs['test_operator_max'] = operator_max
    # else:
    #     flag.attrs['test_operator_max'] = 'None'
    return flag


def spike_test(data: xr.DataArray, spike_half_window: int = 1, std_half_window: int = 15,
               low_multiplier: float = 3, high_multiplier: float = 5):
    """
    Run a dynamic spike test on a DataArray.

    :param data: The input data array.
    :param spike_window: The window size to use for identifying spikes. Default is 3 (n-1, n, n+1).
    :param std_window: The window size to use for identifying a dynamic standard deviation.
    :param low_multiplier: The multiplier to use for flagging data as SUSPECT.
    :param high_multiplier: The multiplier to use for flagging data as FAIL.
    :return: An xr.DataArray of flags with the same dimensions as the input DataArray.
    """

    data = data.sortby('time')

    spkref_windows = data.rolling({'time': spike_half_window * 2 + 1}, min_periods=1).construct('window')
    spkref_left = spkref_windows[:, 0]
    spkref_right = spkref_windows[:, -1]
    spkref = (spkref_left + spkref_right) / 2

    sd = data.rolling({'time': std_half_window * 2 + 1}, center=True, min_periods=1).std()
    threshold_low = low_multiplier * sd
    threshold_high = high_multiplier * sd

    flag = xr.full_like(data, FLAG.NOT_EVALUATED).astype('int8')
    flag = flag.where(~(np.abs(data - spkref) < threshold_low) & ~(np.abs(data - spkref) > threshold_high),
                            FLAG.PASS)
    flag = flag.where((np.abs(data - spkref) < threshold_low) | (np.abs(data - spkref) > threshold_high),
                            FLAG.HIGH_INTEREST)
    flag = flag.where(~(np.abs(data - spkref) > threshold_high), FLAG.FAIL)
    flag = flag.where((~np.isnan(data)), FLAG.MISSING_DATA)
    flag = flag.where((~np.isnan(spkref)), FLAG.MISSING_DATA)
    flag = flag.where((~np.isnan(threshold_low)) | (~np.isnan(threshold_high)), FLAG.NOT_EVALUATED)

    # flag.attrs['test_name'] = f'QARTOD Spike Test - {data.name}'
    # flag.attrs[
    #     'description'] = """The QARTOD Spike Test typically uses the neighboring points (n-1, n+1) to determine if data (n)
    #     is unusually high or low, indicative of a spike that could be representative of unusual environmental
    #     variation or sensor failure depending on the sampling rate."""
    # flag.attrs['spike_half_window_size'] = spike_half_window
    # flag.attrs['std_half_window_size'] = std_half_window
    # flag.attrs['lower_threshold_std_multiplier'] = low_multiplier
    # flag.attrs['upper_threshold_std_multiplier'] = high_multiplier
    return flag


def flat_line_test(data: xr.DataArray,
                   max_allowed_std: float = 0,
                   fail_half_window_size: int = 5,
                   suspect_half_window_size: int = 3) -> xr.DataArray:
    """
    Run a modified flat line test on a DataArray. This test is a modified version of the QARTOD flat line test.

    :param data: An xr.DataArray of data.
    :param fail_half_window_size: The half window size (in number of time steps) to use for the fail test.
        QARTOD default is 5.
    :param suspect_half_window_size: The half window size (in number of time steps) to use for the suspect test.
        QARTOD default is 3.
    :param max_allowed_std: The maximum standard deviation allowed to be considered a flat line.
    :return: An xr.DataArray of flags with the same dimensions as the input DataArray.
    """

    # Fail Window Construction
    wf = data.rolling({'time': fail_half_window_size * 2 + 1}).construct('window')
    wfstd = wf.std(dim='window')

    # Suspect Window Construction
    ws = data.rolling({'time': suspect_half_window_size * 2 + 1}).construct('window')
    wsstd = ws.std(dim='window')

    flag = xr.full_like(data, FLAG.NOT_EVALUATED, dtype='int8')
    flag = xr.where(wsstd <= max_allowed_std, FLAG.SUSPECT, flag) # Flag suspect values.
    flag = xr.where(wfstd <= max_allowed_std, FLAG.FAIL, flag) # Flag bad values.
    flag = xr.where(flag == FLAG.NOT_EVALUATED, FLAG.PASS, flag)

    # flag.attrs['test_name'] = f'QARTOD Flat Line Test - {data.name}'
    # flag.attrs[
    #     'description'] = """The QARTOD Flat Line Test typically uses a window to determine if the last point
    #     is equivalent to or near the previous values in the window. If the standard deviation of the window is less than
    #     the max allowed standard deviation, the data is flagged as flat."""
    # flag.attrs['max_allowed_standard_deviation'] = max_allowed_std
    # flag.attrs['fail_half_window_size'] = fail_half_window_size
    # flag.attrs['suspect_half_window_size'] = suspect_half_window_size

    return flag


# NOT TESTED YET
def rate_of_change_test(data: xr.DataArray, std_multiplier: int = 2):
    data['time'] = data.time.astype('datetime64[ns]')
    _delta_data = data.diff(dim='time', n=1)
    _delta_time = data.time.diff(dim='time').astype(float) / 1000000000

    rate = _delta_data / _delta_time
    rate = np.concatenate([np.array([np.nan]), rate])
    flag = xr.full_like(data, FLAG.NOT_EVALUATED).astype('int8')
    flag = flag.where(np.abs(rate) > std_multiplier * rate.std(), FLAG.PASS)
    flag = flag.where(~(np.abs(rate) > std_multiplier * rate.std()), FLAG.HIGH_INTEREST)
    flag = flag.where((~np.isnan(data)), FLAG.MISSING_DATA)
    flag = flag.where((~np.isnan(rate)), FLAG.MISSING_DATA)

    flag.attrs['test_name'] = f'QARTOD Rate of Change Test - {data.name}'
    flag.attrs[
        'description'] = """The QARTOD Rate of Change Test assesses the rate of change of a variable over time. 
        Currently the implementation of this test estimates the rate of change between each data point, 
        regardless of the sampling rate. This test does not flag BAD or FAIL data."""
    flag.attrs['std_multiplier'] = std_multiplier
    return flag

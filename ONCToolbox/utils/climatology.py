import numpy as np
import xarray as xr


def circular_rolling_mean(data: xr.DataArray,
                          half_window_width: int) -> xr.DataArray:
    """
    This function allows for the rolling smoothing of the beginning and end of a full year climatology or threshold dataset,
    which might otherwise be unsmoothed and result in mismatched tails.
    Higher level wrapper users will rarely call this function directly.

    Pitfalls: This function really only works if the input climatology/threshold data has a full year of data
    and no significant gaps between ends. If only a part of a climatology dataset is provided (e.g. dayofyear 250-350),
    then this function will inappropriately smooth the original dataset with the prepended and appended data.
    If a subset of the climatology is needed, it is recommended that you subset after circular smoothing.

    :param data: A dataset that represents a climatology or threshold dataset with a dayofyear coordinate.
    :param half_window_width: The number of days to include on either side of the central day for smoothing.
    :return: A smoothed dataset with the same dayofyear values as the original dataset.
    """

    # Create a new dataset which can be prepended to the original for "circular" smoothing.
    pre = data.copy(deep=True)
    pre['dayofyear'] = data.dayofyear.min() - data.dayofyear
    pre = pre.sortby('dayofyear')

    # Create a new dataset which can be appended to the original data for "circular" smoothing.
    post = data.copy(deep=True)
    post['dayofyear'] = data.dayofyear.max() + data.dayofyear
    post = post.sortby('dayofyear')

    # Combine the original data with the prepended and appended data.
    circ = xr.combine_by_coords([pre, data, post])

    # Smooth the data with a rolling mean using the half window width.
    rda = circ.rolling({'dayofyear': 2 * half_window_width + 1}, center=True).mean(skipna=True)
    rda = rda.sel(dayofyear=data.dayofyear)  # Select the original dayofyear values for return.
    rda = rda[data.name]  # Why does it become a dataset?
    return rda



def fixed_baseline_climatology(data: xr.DataArray,
                               half_window_width: int = 5,
                               use_circular: bool = True,
                               reset_to_input_time: bool = False) -> xr.DataArray:
    """
    Build a fixed baseline daily climatology from an input xr.DataArray.
    The climatology is calculated on a 366 day year.
    This follows the methods of Hobday et al., 2016 for creating a fixed baseline climatology for marine heatwave analysis.
    There are other ways to compute a climatology and this metod may not be appropriate for all use cases.

    :param data: The input xr.DataArray.
    :param half_window_width: The window half width for smoothing the climatology.
    :param use_circular: Setting to True will wrap the climatology during smoothing.
    :param reset_to_input_time: If True, the output climatology will be mapped to the time
        coordinates of the original input dataset.
        At the moment this is only intended functionality for Fixed Baseline analysis.
    :return: An xr.DataArray representing the daily climatology.
    """

    cda = data.groupby('time.dayofyear',
                       restore_coord_dims=True).mean(skipna=True)

    if use_circular is True:
        rcda = circular_rolling_mean(cda, half_window_width)
    else:
        rcda = cda.rolling({'dayofyear': 2 * half_window_width + 1},
                           center=True).mean(skipna=True)
        rcda = rcda.sel(dayofyear=cda.dayofyear)  # Select the original dayofyear values for return.

    # Reset the time coordinate to the original input time for use in shifting or detrended baselines.
    if reset_to_input_time is True:
        reset_bins = []
        years = np.unique(data.time.dt.year)
        for year in years:
            _aligned_rcda = rcda.copy(deep = True)
            _aligned_rcda['time'] = (['dayofyear'], [dt.strptime(f"{year}-{int(dt)}", '%Y-%j')
                                                     for dt in _aligned_rcda.dayofyear.values.tolist()])
            _aligned_rcda = _aligned_rcda.swap_dims({'dayofyear': 'time'})
            _aligned_rcda = _aligned_rcda.drop_duplicates(dim = 'time', keep = 'last')
            reset_bins.append(_aligned_rcda)
        rcda = xr.combine_by_coords(reset_bins)
        rcda = rcda[data.name]  # Why does it become a dataset?
    return rcda

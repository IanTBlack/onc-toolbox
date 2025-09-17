import numpy as np
import xarray as xr
from xarray.groupers import BinGrouper

from ONCToolbox.utils.locations import BCFTerminal

def flag_bcf_terminal(latitude, longitude, bbox_check = 0.01):
    """
    Flag points that are near British Columbia Ferry Terminals.

    :param latitude:
    :param longitude:
    :param bbox_check:
    :return:
    """
    terminals = [k for k,v in BCFTerminal.__dict__.items() if '__' not in k]

    flag = xr.zeros_like(latitude)
    for terminal in terminals:
        loc = getattr(BCFTerminal, terminal)
        flag = xr.where((latitude > loc.latitude - bbox_check) & 
                        (latitude < terminal.lat + bbox_check) & 
                        (longitude > loc.longitude - bbox_check) & 
                        (longitude < loc.longitude + bbox_check), 1, flag)
    return flag



def cut_transit(transit: xr.Dataset, cut_begin: int = 60 * 3, cut_end: int = 60):
    """
    Remove the beginning and end of a transit under the assumption of poor data quality.

    :param transit: An xr.Dataset made up of a single transit.
    :param cut_begin: The number of seconds to cut from the beginning of the transit.
    :param cut_end: The number of seconds to cut from the end of the transit.
    :return: A slightly shorter transit.
    """

    t_begin = transit.time.min() + np.timedelta64(cut_begin, 's')
    t_end = transit.time.max() - np.timedelta64(cut_end, 's')
    _transit = transit.sel(time=slice(t_begin, t_end))
    return _transit



def grid_transit(transit: xr.Dataset,
                 lat_min: float = 48.950,
                 lat_max: float = 49.275,
                 lon_min: float = -123.950,
                 lon_max: float = -123.100,
                 bin_size: float = 0.005,
                 central_buffer: float = 0.0025) -> xr.Dataset:

    transit['ftime'] = transit.time.astype(float)
    lat_grouper = BinGrouper(bins=np.arange(lat_min - central_buffer, lat_max + central_buffer, bin_size))
    lon_grouper = BinGrouper(bins=np.arange(lon_min - central_buffer, lon_max + central_buffer, bin_size))

    bin_lats = lat_grouper.bins[:-1] + central_buffer
    bin_lons = lon_grouper.bins[:-1] + central_buffer

    binned_transit = transit.groupby(latitude=lat_grouper,
                                     longitude=lon_grouper).mean(skipna=True)
    binned_transit['latitude'] = (['latitude_bins'], bin_lats)
    binned_transit['longitude'] = (['longitude_bins'], bin_lons)
    binned_transit = binned_transit.swap_dims({'latitude_bins': 'latitude', 'longitude_bins': 'longitude'})
    binned_transit['time'] = binned_transit.ftime.astype('datetime64[ns]')
    binned_transit = binned_transit.drop_vars(['latitude_bins', 'longitude_bins', 'ftime'], errors='ignore')
    binned_transit = binned_transit.dropna(dim='latitude', how='all')
    binned_transit = binned_transit.dropna(dim='longitude', how='all')

    return binned_transit

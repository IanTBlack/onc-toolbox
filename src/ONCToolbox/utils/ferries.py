import numpy as np
import pandas as pd
import xarray as xr
from xarray.groupers import BinGrouper

from ONCToolbox.utils.locations import BCFTerminal as BCFT
from ONCToolbox.utils.locations import (TsawwassenBox, DepartureBayBox, DukePointBox,
                                        NanaimoAreaBox, GabriolaBox, NanaimoHarborBox,
                                        SwartzBayBox, PortMcneilBox, AlertBayBox,
                                        SointulaBox, HorseshoeBayBox)


def cut_transit(transect: xr.Dataset, cut_begin: int = 60 * 3, cut_end: int = 0):
    """
    Remove the beginning and end of a transect under an assumption of poor data quality.

    :param transect: An xr.Dataset made up of a single transect.
    :param cut_begin: The number of seconds to cut from the beginning of the transect.
    :param cut_end: The number of seconds to cut from the end of the transect.
    :return: A slightly shorter transect.
    """

    t_begin = transect.time.min() + np.timedelta64(cut_begin, 's')
    t_end = transect.time.max() - np.timedelta64(cut_end, 's')
    _transect = transect.sel(time=slice(t_begin, t_end))
    return _transect


def flag_port(ds,
              terminals: tuple | list = (BCFT.Tsawwassen, BCFT.DukePoint, BCFT.DepartureBay, BCFT.Gabriola,
                                         BCFT.NanaimoHarbor, BCFT.SwartzBay, BCFT.PortMcneil, BCFT.AlertBay,
                                         BCFT.Sointula,
                                         BCFT.HorseshoeBay),
              bbox_check: float = 0.01, speed_check: float = 3) -> xr.DataArray:
    flag = xr.zeros_like(ds.latitude, dtype=int)
    for terminal in terminals:
        flag = xr.where((ds.latitude > terminal.lat - bbox_check) & (ds.latitude < terminal.lat + bbox_check) & (
                ds.longitude > terminal.lon - bbox_check) & (ds.longitude < terminal.lon + bbox_check), 1, flag)

    # if 'speed_over_ground' in ds
    # flag = xr.where((ds.speed_over_ground <= speed_check) & (flag == 1), 1, 0)

    flag = flag.astype('int8')
    return flag


def grid_transit(transit: xr.Dataset,
                 lat_min: float = 48.90,
                 lat_max: float = 49.28,
                 lon_min: float = -123.950,
                 lon_max: float = -123.100,
                 bin_size: float = 0.01,
                 central_buffer: float = 0.005) -> xr.Dataset:

    transit_begin = transit.time.min().values
    transit_end = transit.time.max().values
    transit['ftime'] = transit['time'].astype(float)

    lat_grouper = BinGrouper(bins=np.arange(lat_min - central_buffer, lat_max + central_buffer, bin_size))
    lon_grouper = BinGrouper(bins=np.arange(lon_min - central_buffer, lon_max + central_buffer, bin_size))
    bin_lats = lat_grouper.bins[:-1] + central_buffer
    bin_lons = lon_grouper.bins[:-1] + central_buffer
    binned_transit = transit.groupby(latitude=lat_grouper, longitude=lon_grouper).mean(skipna=True)

    binned_transit['binned_latitude'] = (['latitude_bins'], bin_lats)
    binned_transit['binned_longitude'] = (['longitude_bins'], bin_lons)
    binned_transit = binned_transit.swap_dims({'latitude_bins': 'binned_latitude', 'longitude_bins': 'binned_longitude'})
    binned_transit['time'] = binned_transit['ftime'].astype('datetime64[ns]')
    binned_transit = binned_transit.drop_vars(['latitude_bins', 'longitude_bins', 'ftime', 'latitude', 'longitude'],errors='ignore')
    binned_transit = binned_transit.rename({'binned_latitude': 'latitude', 'binned_longitude': 'longitude'})
    binned_transit = binned_transit.dropna(dim='latitude', how='all')
    binned_transit = binned_transit.dropna(dim='longitude', how='all')

    transit_id = pd.to_datetime(transit_begin).floor('h').tz_localize('UTC').tz_convert('America/Vancouver').tz_localize(None)
    binned_transit = binned_transit.assign_coords({'transit_id': [transit_id]})
    binned_transit['transit_begin'] = (['transit_id'], [transit_begin])
    binned_transit['transit_end'] = (['transit_id'], [transit_end])

    for tvar in ['transit_id','time', 'transit_begin', 'transit_end']:
        if tvar in binned_transit.coords or tvar in binned_transit.data_vars:
            binned_transit[tvar] = binned_transit[tvar].astype('datetime64[ms]')

    for posvar in ['latitude', 'longitude']:
        if posvar in binned_transit.coords or posvar in binned_transit.data_vars:
            binned_transit[posvar] = binned_transit[posvar].astype('float32')

    dvs = binned_transit.data_vars
    for dv in dvs:
        if binned_transit[dv].dtype == 'float64':
            binned_transit[dv] = binned_transit[dv].astype('float32')


    return binned_transit


def determine_transit_reference(transit: xr.Dataset) -> xr.Dataset:

    transit = transit.sortby('time')
    subnav = transit[['latitude','longitude']]

    begin_lat = float(subnav.latitude.values[0])
    begin_lon = float(subnav.longitude.values[0])

    end_lat = float(subnav.latitude.values[-1])
    end_lon = float(subnav.longitude.values[-1])

    departure = 'Unknown'  # By default the departure and arrival locations are unknown.
    arrival = 'Unknown'

    bboxes = [TsawwassenBox, DepartureBayBox, DukePointBox, NanaimoAreaBox, SwartzBayBox, GabriolaBox, NanaimoHarborBox,
              PortMcneilBox, AlertBayBox, SointulaBox, HorseshoeBayBox]
    for bbox in bboxes:
        if bbox.lat_min <= begin_lat <= bbox.lat_max and bbox.lon_min <= begin_lon <= bbox.lon_max:
            departure = bbox.name
            break

    for bbox in bboxes:
        if bbox.lat_min <= end_lat <= bbox.lat_max and bbox.lon_min <= end_lon <= bbox.lon_max:
            arrival = bbox.name
            break

    return (departure, arrival)

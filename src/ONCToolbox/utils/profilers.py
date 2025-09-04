from datetime import timedelta
import numpy as np
import pandas as pd
import xarray as xr

from ONCToolbox.utils.core import split_periods
from ONCToolbox.qaqc import flat_line_test


def identify_profiles(cable_length, profile_direction: str = 'all',
                      buffer: int = 10,
                      max_allowed_std: float = 0.02, min_gap: int = 180):
    flag_cl = flat_line_test(cable_length, max_allowed_std=max_allowed_std)
    profiling_state = flag_cl.where(flag_cl == 1, drop=True)
    profiles = split_periods(profiling_state, min_gap = min_gap)

    assigned_profiles = []
    for profile in profiles:
        _cl = cable_length.sel(time=slice(profile['begin_datetime'], profile['end_datetime']))
        _start = _cl.sel(time=_cl.time.min())
        _stop = _cl.sel(time=_cl.time.max())
        if _start - _stop > 0:
            profile_dir = 'down'
        else:
            profile_dir = 'up'

        profile['direction'] = profile_dir
        profile['begin_datetime'] = pd.to_datetime(profile['begin_datetime'] - np.timedelta64(buffer, 's'))
        profile['end_datetime'] = pd.to_datetime(profile['end_datetime'] + np.timedelta64(buffer, 's'))

        assigned_profiles.append(profile)

    if profile_direction == 'all':
        return assigned_profiles
    elif profile_direction == 'up':
        up_pros = [p for p in assigned_profiles if p['direction'] == 'up']
        return up_pros
    elif profile_direction == 'down':
        down_pros = [p for p in assigned_profiles if p['direction'] == 'down']
        return down_pros

def identify_stops(cable_length, buffer: int = 10, max_allowed_std: float = 0.01):
    flag_cl = flat_line_test(cable_length, max_allowed_std=max_allowed_std)
    stop_state = flag_cl.where(flag_cl != 1, drop = True)
    stops = split_periods(stop_state, min_gap = 60)

    assigned_stops = []
    for stop in stops:
        _cl = cable_length.sel(time = slice(stop['begin_datetime'], stop['end_datetime']))
        stop_cl_out = int(np.ceil(_cl.median()))
        stop['begin_datetime'] = pd.to_datetime(stop['begin_datetime']) - np.timedelta64(buffer,'s')
        stop['end_datetime'] = pd.to_datetime(stop['end_datetime']) + np.timedelta64(buffer,'s')
        stop['cable_length_out'] = stop_cl_out
        assigned_stops.append(stop)
    return stops


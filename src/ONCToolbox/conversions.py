import xarray as xr

def to_ms(ds: xr.Dataset, time_vars = ['time','transit_id']):
    for tvar in time_vars:
        if tvar in ds.data_vars:
            ds[tvar] = ds[tvar].astype('datetime64[ms]')
    return ds
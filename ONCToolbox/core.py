from datetime import datetime
import numpy as np
from onc import ONC
import pandas as pd
import re
import xarray as xr

from .utils.token import get_onc_token_from_netrc

FlagTerm = 'qaqc_flag'

def format_datetime(dt: datetime | None | str) -> str:
    """
    Format any incoming datetime representation to a format that is compatible
        with the ONC REST API. If None is provided, then the API will default
        to using the tail end of the data available.

    :param dt: A datetime object, string representation of a date, or None.
    :return: A string in the format of 'YYYY-mm-ddTHH:MM:SS.fffZ'.
    """
    if dt is None:
        return None
    else:
        dt = pd.to_datetime(dt)
        dtstr = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        return dtstr

def scrub_token(query_url: str) -> str:
    token_regex = r'(&token=[a-f0-9-]{36})'
    token_qp = re.findall(token_regex, query_url)[0]
    redacted_url = query_url.replace(token_qp, '&token=REDACTED')
    return redacted_url


def nan_onc_flags(data: pd.DataFrame | xr.Dataset,
                  flags_to_nan: list[int] = [4]) -> pd.DataFrame | xr.Dataset:
    if isinstance(data, pd.DataFrame):
        vars = data.columns
    elif isinstance(data, xr.Dataset):
        vars = data.data_vars
    flag_vars = [v for v in vars if v.startswith(f"{FlagTerm}_")]
    if len(flag_vars) != 0:
        for fv in flag_vars:
            dv = fv.replace(f"{FlagTerm}_", '')
            if dv in vars:
                data[dv] = data[dv].where(~data[fv].isin([flags_to_nan]), np.nan)
    return data


def remove_onc_flags(data: pd.DataFrame | xr.Dataset) -> pd.DataFrame | xr.Dataset:
    if isinstance(data, pd.DataFrame):
        vars = data.columns
    elif isinstance(data, xr.Dataset):
        vars = data.data_vars
    flag_vars = [v for v in vars if v.startswith(f"{FlagTerm}_")]
    if len(flag_vars) != 0:
        if isinstance(data, pd.DataFrame):
            data = data.drop(columns = flag_vars, errors = 'ignore')
        elif isinstance(data, xr.Dataset):
            data = data.drop_vars(flag_vars, errors = 'ignore')


    return data



class ONCToolbox(ONC):
    def __init__(self, token: str = get_onc_token_from_netrc(),
                 show_info: bool | None = False,
                 show_warning: bool | None = False,
                 timeout: int = 60) -> None:

        super().__init__(token=token,
                         showInfo=show_info,
                         showWarning=show_warning,
                         timeout=timeout)


    def get_fullres_data(self, location_code: str | None,
                         device_category_code: str | None = None,
                         property_code: str | list[str] | None = None,
                         sensor_category_codes: str | list[str] | None = None,
                         device_code: str | None = None,
                         date_from: datetime | None = None,
                         date_to: datetime | None = None,
                         out_as: str = 'json',
                         add_metadata: bool = False):

        ## Input Checks
        if (location_code is None
                and device_category_code is None
                and device_code is None):
            raise ValueError("Either both a location_code and a device_category_code "
                             "or just a device_code must be provided.")

        if out_as not in ['json', 'pandas', 'xarray']:
            raise ValueError("out_as must be one of 'json', 'pandas', or 'xarray'")

        if isinstance(property_code, str):
            pcs = property_code
        elif isinstance(property_code, list):
            pcs = ','.join(property_code)
        elif property_code is None:
            pcs = None

        if isinstance(sensor_category_codes, str):
            scc = sensor_category_codes
        elif isinstance(sensor_category_codes, list):
            scc = ','.join(sensor_category_codes)
        elif sensor_category_codes is None:
            scc = None

        params = {'locationCode': location_code,
                  'deviceCategoryCode': device_category_code,
                  'deviceCode': device_code,
                  'propertyCode': pcs,
                  'sensorCategoryCodes': scc,
                  'dateFrom': format_datetime(date_from),
                  'dateTo': format_datetime(date_to),
                  'rowLimit': 100000,
                  'outputFormat': 'Array',
                  'qualityControl': 'raw',
                  'fillGaps': False,
                  'metadata': 'Full',
                  'byDeployment': False}
        params = {k: v for k, v in params.items() if v is not None}
        json_data = self.getScalardata(filters=params, allPages=True)

        if json_data is None:
            return None

        if out_as == 'json':
            return json_data
        else:
            data = self.convert_json(json_data, out_as = out_as)
            if add_metadata is True:
                data = self.add_metadata(data)
            return data

    def add_metadata(self, data: xr.Dataset | pd.DataFrame):

        # Assign Variable Level Attributes
        if isinstance(data, pd.DataFrame):
            vars = data.columns
        elif isinstance(data, xr.Dataset):
            vars = data.data_vars
        vars = [v for v in vars if not v.startswith(FlagTerm)]
        for var in vars:
            lc = data[var].attrs['locationCode']
            dcc = data[var].attrs['deviceCategoryCode']
            pc = data[var].attrs['propertyCode']
            prop = self.get_properties(location_code=lc,
                                       device_category_code=dcc, property_code=pc)
            for col in prop.columns:
                if col in ['hasDeviceData', 'hasPropertyData', 'cvTerm.property',
                           'cvTerm.uom']:
                    continue
                else:
                    col_vals = prop[col].values.tolist()
                    if len(col_vals) == 1:
                        col_vals = col_vals[0]
                    if isinstance(col_vals, dict | list):
                        col_vals = str(col_vals)
                    data[var].attrs[col] = col_vals

        # Assign Root Level Attributes
        dev_cat_info = self.get_device_categories(
            location_code=data.attrs['locationCode'],
            device_category_code=data.attrs['deviceCategoryCode'])
        for col in dev_cat_info.columns:
            if col in ['cvTerm.deviceCategory', 'hasDeviceData']:
                continue
            else:
                col_vals = dev_cat_info[col].values.tolist()
                if len(col_vals) == 1:
                    col_vals = col_vals[0]
                if isinstance(col_vals, dict | list):
                    col_vals = str(col_vals)
                data.attrs[col] = col_vals

        loc_info = self.get_locations(location_code = data.attrs['locationCode'],
                                      device_category_code=data.attrs['deviceCategoryCode'],
                                      date_from = data.time.min().values.tolist(),
                                      date_to = data.time.max().values.tolist())
        for col in loc_info.columns:
            if col in ['hasDeviceData', 'hasPropertyData', 'cvTerm.device']:
                continue
            else:
                col_vals = loc_info[col].values.tolist()
                if len(col_vals) == 1:
                    col_vals = col_vals[0]
                if isinstance(col_vals, dict | list):
                    col_vals = str(col_vals)
                data.attrs[col] = col_vals

        dev_info = self.get_devices(location_code = data.attrs['locationCode'],
                                    device_category_code=data.attrs['deviceCategoryCode'],
                                    date_from = data.time.min().values.tolist(),
                                    date_to = data.time.max().values.tolist())
        for col in dev_info.columns:
            if col in ['hasDeviceData', 'hasPropertyData', 'cvTerm.device']:
                continue
            else:
                col_vals = dev_info[col].values.tolist()
                if len(col_vals) == 1:
                    col_vals = col_vals[0]
                if isinstance(col_vals, dict | list):
                    col_vals = str(col_vals)
                data.attrs[col] = col_vals

        return data

    def var_name_from_sensor_name(self,sensor_name: str) -> str:
        var_name = sensor_name.replace(' ', '_').lower()
        var_name = var_name.replace('(', '')
        var_name = var_name.replace(')', '')
        return var_name

    def json_var_data_to_dataframe(self,var_data):
        var_name = self.var_name_from_sensor_name(var_data['sensorName'])
        flag_var_name = '_'.join((FlagTerm, var_name))
        var_times = var_data['data']['sampleTimes']
        var_values = var_data['data']['values']
        var_flags = var_data['data']['qaqcFlags']
        vdf = pd.DataFrame({'time': var_times,
                            var_name: var_values,
                            flag_var_name: var_flags})

        vdf['time'] = pd.to_datetime(vdf['time']).dt.tz_localize(None)
        vdf['time'] = vdf['time'].astype('datetime64[ms]')
        vdf.index = vdf['time']
        vdf = vdf.drop(columns=['time'])

        var_metadata = {k: v for k, v in var_data.items() if
                        k not in ['actualSamples', 'data', 'outputFormat']}
        return (vdf, var_metadata)


    def convert_json(self, json_response_data, out_as='xr', scrub_url: bool = True):
        qaqc_flag_info = json_response_data['qaqcFlagInfo']
        qaqc_flag_info = '\n'.join(
            [':'.join((k, v)) for k, v in qaqc_flag_info.items()])

        dev_cat_code = json_response_data['metadata']['deviceCategoryCode']
        loc_name = json_response_data['metadata']['locationName']
        loc_code = json_response_data['parameters']['locationCode']
        sensor_data = json_response_data['sensorData']

        if sensor_data is None:
            if scrub_url is True:
                query_url = scrub_token(json_response_data['queryUrl'])
            else:
                query_url = json_response_data['queryUrl']
            raise UserWarning(f"No data found for request: {query_url}")
            return None

        dfs, var_metadata = zip(*[self.json_var_data_to_dataframe(vd)
                                  for vd in sensor_data])
        df = pd.concat(dfs, axis=1)
        if out_as == 'pandas':
            out = df
            vars = out.columns
        elif out_as == 'xarray':
            out = df.to_xarray()
            vars = out.data_vars

        for vmd in var_metadata:
            var_name = self.var_name_from_sensor_name(vmd['sensorName'])
            out[var_name].attrs = vmd
            out[var_name].attrs['deviceCategoryCode'] = dev_cat_code
            out[var_name].attrs['locationName'] = loc_name
            out[var_name].attrs['locationCode'] = loc_code

            flag_var_name = '_'.join((FlagTerm, var_name))
            if flag_var_name in vars:
                out[flag_var_name].attrs['ancillary_variable'] = var_name
                out[flag_var_name].attrs['flag_meanings'] = qaqc_flag_info


        out.attrs['deviceCategoryCode'] = dev_cat_code
        out.attrs['locationName'] = loc_name
        out.attrs['locationCode'] = loc_code
        out.attrs['qaqcFlagInfo'] = qaqc_flag_info
        return out



    def get_properties(self, location_code: str | None = None,
                       device_category_code: str = None,
                       property_code: str = None, property_name: str = None,
                       description: str = None, device_code: str = None):
        params = {'locationCode': location_code,
                  'deviceCategoryCode': device_category_code,
                  'propertyCode': property_code,
                  'propertyName': property_name,
                  'description': description,
                  'device_code': device_code}

        params = {k: v for k, v in params.items() if v is not None}
        json_response = self.getProperties(filters=params)

        df = pd.json_normalize(json_response)

        df = df[sorted(df.columns)]
        return df


    def get_device_categories(self,
                              location_code: str = None,
                              device_category_code: str = None,
                              device_category_name: str = None,
                              description: str = None,
                              property_code: str = None):
        params = {'locationCode': location_code,
                  'deviceCategoryCode': device_category_code,
                  'deviceCategoryName': device_category_name,
                  'propertyCode': property_code,
                  'description': description}
        params = {k: v for k, v in params.items() if v is not None}
        json_response = self.getDeviceCategories(filters=params)
        df = pd.json_normalize(json_response)
        df = df[sorted(df.columns)]
        return df


    def get_locations(self, location_code: str | None = None,
                      date_from: datetime | None = None,
                      date_to: datetime | None = None,
                      device_category_code: str = None,
                      property_code: str = None,
                      data_product_code: str = None,
                      location_name: str = None,
                      device_code: str = None,
                      include_children: bool | None = None,
                      aggregate_deployments: bool | None = None) -> pd.DataFrame:
        params = {'locationCode': location_code,
                  'dateFrom': format_datetime(date_from) if date_from is not None else date_from,
                  'dateTo': format_datetime(date_to) if date_from is not None else date_to,
                  'deviceCategoryCode': device_category_code,
                  'propertyCode': property_code,
                  'dataProductCode': data_product_code,
                  'locationName': location_name,
                  'deviceCode': device_code,
                  'includeChildren': include_children,
                  'aggregateDeployments': aggregate_deployments}
        params = {k: v for k, v in params.items() if v is not None}
        json_response = self.getLocations(filters=params)
        df = pd.json_normalize(json_response)
        df = df[sorted(df.columns)]
        return df


    def get_devices(self, location_code: str | None = None,
                    device_category_code: str = None,
                    date_from: datetime | None = None,
                    date_to: datetime | None = None,
                    device_code: str = None,
                    device_id: str = None,
                    device_name: str = None,
                    include_children: bool | None = None,
                    data_product_code: str = None,
                    property_code: str = None, ):
        params = {'locationCode': location_code,
                  'deviceCategoryCode': device_category_code,
                  'dateFrom': format_datetime(date_from),
                  'dateTo': format_datetime(date_to),
                  'deviceCode': device_code,
                  'deviceId': device_id,
                  'deviceName': device_name,
                  'includeChildren': include_children,
                  'dataProductCode': data_product_code,
                  'propertyCode': property_code}
        params = {k: v for k, v in params.items() if v is not None}
        json_response = self.getDevices(filters=params)
        df = pd.json_normalize(json_response)
        df = df[sorted(df.columns)]
        return df



    def get_deployments(self,
                        location_code: str | None = None,
                        date_from: datetime | None = None,
                        date_to: datetime | None = None,
                        device_category_code: str = None, property_code: str = None,
                        device_code: str = None):
        params = {'locationCode': location_code,
                  'deviceCategoryCode': device_category_code,
                  'deviceCode': device_code,
                  'dateFrom': format_datetime(date_from),
                  'dateTo': format_datetime(date_to),
                  'propertyCode': property_code, }
        params = {k: v for k, v in params.items() if v is not None}
        json_response = self.getDeployments(filters=params)

        df = pd.json_normalize(json_response)
        df = df.drop(columns=['citation'], errors='ignore')

        df['begin'] = pd.to_datetime(df['begin'])
        df['end'] = pd.to_datetime(df['end'])
        df = df[sorted(df.columns)]

        return df
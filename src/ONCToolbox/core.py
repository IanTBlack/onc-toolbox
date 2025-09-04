from datetime import datetime, timezone
from onc import ONC
import os
import pandas as pd
import requests
import xarray as xr

from .utils import dt2str, get_onc_token

DEFAULT_BDT = datetime(2001,1,1,0,0,0,0)
DEFAULT_EDT = datetime(2099,12,31,23,59,59,999999)


class ONCToolbox(ONC):
    def __init__(self, token: str = get_onc_token(),
                 show_info: bool = False,
                 show_warning:bool = False,
                 timeout: int = 60) -> None:
        super().__init__(token = token, showInfo = show_info, showWarning = show_warning, timeout = timeout)


    def get_locations(self, location_code: str = None,
                      begin_datetime: datetime = DEFAULT_BDT,
                      end_datetime: datetime = DEFAULT_EDT,
                      device_category_code: str = None,
                      property_code: str = None,
                      data_product_code: str = None,
                      location_name: str = None,
                      device_code: str = None,
                      include_children: bool | None = None,
                      aggregate_deployments: bool | None = None) -> list[dict]:
        params = {'locationCode': location_code,
                  'dateFrom': dt2str(begin_datetime),
                  'dateTo': dt2str(end_datetime),
                  'deviceCategoryCode': device_category_code,
                  'propertyCode': property_code,
                  'dataProductCode': data_product_code,
                  'locationName': location_name,
                  'deviceCode': device_code,
                  'includeChildren': include_children,
                  'aggregateDeployments': aggregate_deployments}
        params = {k: v for k, v in params.items() if v is not None}
        json_response = self.getLocations(filters=params)
        return json_response



    def get_all_location_codes(self,
                               primary_location_code: str,
                               begin_datetime: datetime = DEFAULT_BDT,
                               end_datetime: datetime = DEFAULT_EDT,
                               device_category_code: str = None,
                               property_code: str = None,
                               data_product_code: str = None,
                               location_name: str = None,
                               device_code: str = None) -> list[str]:
        """
        Obtain a list of all location codes associated with a primary location.
        Some "location codes" are actually deployments of different types of instruments at each primary location/platform.
        This function will return a list of all possible location codes for a given platform.

        :param primary_location_code: The primary location code (e.g. BACVP, TWSB, TWDP, etc.).
        :param begin_datetime: The beginning time limit to the search request.
        :param end_datetime: The ending time limit to the search request.
        :param device_category_code: An ONC deviceCategoryCode of interest.
        :param property_code: An ONC propertyCode of interest.
        :param data_product_code: An ONC dataProductCode of interest.
        :param location_name: An ONC locationName of interest.
        :param device_code: An ONC deviceCode of interest.
        :return: A list of location codes.
        """

        params = {'locationCode': primary_location_code,
                  'dateFrom': dt2str(begin_datetime),
                  'dateTo': dt2str(end_datetime),
                  'deviceCategoryCode': device_category_code,
                  'propertyCode': property_code,
                  'dataProductCode': data_product_code,
                  'locationName': location_name,
                  'deviceCode': device_code}
        params = {k: v for k, v in params.items() if v is not None}
        json_response = self.getLocationsTree(filters=params)

        location_codes = []
        for location_info in json_response:
            primary_location = location_info['locationCode']
            node = location_info['children']
            for node_info in node:
                location_code = node_info['locationCode']
                location_codes.append(location_code)
        location_codes = sorted([primary_location] + location_codes)
        return location_codes





    def get_device_categories(self,
                              location_code: str,
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
        dccs = []
        for dcc_info in json_response:
            dcc = dcc_info['deviceCategoryCode']
            dccs.append(dcc)
        return dccs


    def get_deployments(self,
                        location_code: str, begin_datetime: datetime = datetime(2001,1,1),
                        end_datetime: datetime = datetime(2099,12,31,23,59,59,999999),
                        device_category_code: str = None, property_code: str = None, device_code: str = None):
        params = {'locationCode': location_code,
                  'deviceCategoryCode': device_category_code,
                  'deviceCode': device_code,
                  'dateFrom': dt2str(datetime(2001, 1, 1, 0, 0, 0, 0)),
                  'dateTo': dt2str(datetime.now(timezone.utc)),
                  'propertyCode': property_code,
                  }
        params = {k: v for k, v in params.items() if v is not None}
        json_response = self.getDeployments(filters=params)

        all_deployments = {}
        for dep in json_response:
            dep_dcc = dep['deviceCategoryCode']
            if dep_dcc not in all_deployments.keys():
                all_deployments[dep_dcc] = {}
                dnum = 1
            else:
                dnum = len(all_deployments[dep_dcc]) + 1
            dnumstr = 'D' + str(dnum).zfill(5)
            if dep['end'] is None:
                dep_end = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                dep_end = dep['end']
            dep_info = {'device_code': dep['deviceCode'],
                        'begin_datetime': datetime.strptime(dep['begin'], '%Y-%m-%dT%H:%M:%S.%fZ'),
                        'end_datetime': datetime.strptime(dep_end, '%Y-%m-%dT%H:%M:%S.%fZ')}

            all_deployments[dep_dcc][dnumstr] = dep_info

        requested_deployments = {}
        for dcc, deps in all_deployments.items():
            for dnum, dinfo in deps.items():
                dbdt = dinfo['begin_datetime']
                dedt = dinfo['end_datetime']

                if (dbdt <= begin_datetime <= dedt) or (dbdt <= end_datetime <= dedt) or (
                        begin_datetime <= dbdt <= end_datetime) or (begin_datetime <= dedt <= end_datetime):
                    if dcc not in requested_deployments.keys():
                        requested_deployments[dcc] = {}
                    requested_deployments[dcc][dnum] = dinfo

        return requested_deployments


    def get_properties(self, location_code: str, device_category_code: str = None,
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
        return json_response


    def get_devices(self, location_code: str, device_category_code: str = None,
                    date_from: datetime = DEFAULT_BDT,
                    date_to: datetime = DEFAULT_EDT,
                    device_code: str = None,
                    device_id: str = None,
                    device_name: str = None,
                    include_children: bool | None = None,
                    data_product_code: str = None,
                    property_code: str = None,):
        params = {'locationCode': location_code,
                  'deviceCategoryCode': device_category_code,
                  'dateFrom': dt2str(date_from),
                  'dateTo': dt2str(date_to),
                  'deviceCode': device_code,
                  'deviceId': device_id,
                  'deviceName': device_name,
                  'includeChildren': include_children,
                  'dataProductCode': data_product_code,
                  'propertyCode': property_code}
        params = {k: v for k, v in params.items() if v is not None}
        json_response = self.getDevices(filters=params)
        return json_response


    def get_data_formats(self, location_code: str,
                          device_category_code: str,
                          extension: str = 'nc',
                          data_product_code: str = None,
                          data_product_name: str = None,
                          device_code: str = None,
                          property_code: str = None):
        params = {'locationCode': location_code,
                  'deviceCategoryCode': device_category_code,
                  'extension': extension,
                  'dataProductCode': data_product_code,
                  'dataProductName': data_product_name,
                  'deviceCode': device_code,
                  'propertyCode': property_code}
        params = {k: v for k, v in params.items() if v is not None}
        json_response = self.getDataProducts(filters=params)
        return json_response


    def get_data(self, location_code: str,
                 device_category_code: str | None = None,
                 begin_datetime: datetime = datetime(2001,1,1,0,0,0,0),
                 end_datetime: datetime = datetime(2099,12,31,23,59,59,999999),
                 row_limit: int = 100000,
                 property_codes: list[str] | None = None) -> xr.Dataset | list:

        params = {'locationCode': location_code.upper(),
                  'deviceCategoryCode': device_category_code,
                  'dateFrom': dt2str(begin_datetime),
                  'dateTo': dt2str(end_datetime),
                  'rowLimit': row_limit,
                  'outputFormat': 'Array',
                  'qualityControl': 'raw',
                  'fillGaps': False,
                  'metadata': 'Full',
                  'propertyCode': ','.join(property_codes) if property_codes is not None else None}
        params = {k: v for k, v in params.items() if v is not None}

        json_data = self.getScalardata(filters = params, allPages = True)
        ds = self._json2xr(json_data)

        if len(ds) == 0 or ds is None:
            return None
        else:
            return ds


    def _json2xr(self, response_data: dict) -> xr.Dataset:
        dev_cat_code = response_data['metadata']['deviceCategoryCode']
        sensor_data = response_data['sensorData']
        if sensor_data is None or len(sensor_data) == 0:
            return None

        vds_list = []
        for var_data in sensor_data:  # This could probably be parallelized in the future.
            var_name = var_data['sensorName'].replace(' ', '_').lower() # Remove blanks.
            var_name = var_name.replace('(', '')
            var_name = var_name.replace(')', '')

            var_units = var_data['unitOfMeasure']
            var_times = var_data['data']['sampleTimes']
            var_values = var_data['data']['values']
            var_flags = var_data['data']['qaqcFlags']

            vds = xr.Dataset()
            vds = vds.assign_coords({'time': pd.to_datetime(var_times).tz_localize(None)})
            vds[var_name] = (('time'), var_values)
            vds[var_name].attrs['units'] = var_units
            vds[f"flag_{var_name}"] = (('time'), var_flags)

            # Fill any potential NaNs with a flag indicating no QAQC performed (0).
            vds[f"flag_{var_name}"] = vds[f"flag_{var_name}"].fillna(0)

            # Convert time dtypes to reduce object size.
            vds['time'] = vds['time'].astype('datetime64[ms]')
            vds[f"flag_{var_name}"] = vds[f"flag_{var_name}"].astype('int8')

            # Assign attributes.
            vds[var_name].attrs['onc_propertyCode'] = var_data['propertyCode']
            vds[var_name].attrs['onc_sensorCategoryCode'] = var_data['sensorCategoryCode']
            vds[var_name].attrs['onc_sensorName'] = var_data['sensorName']
            vds[var_name].attrs['onc_sensorCode'] = var_data['sensorCode']
            vds[var_name].attrs['onc_deviceCategoryCode'] = dev_cat_code
            vds_list.append(vds)

        ds = xr.combine_by_coords(vds_list)
        ds = ds[sorted(ds.data_vars)]

        ds.attrs['onc_deviceCategoryCode'] = dev_cat_code

        return ds



    def find_data(self, primary_location_code, device_category_codes, begin_datetime, end_datetime):
        loc_codes = self.get_all_location_codes(primary_location_code=primary_location_code,
                                                begin_datetime=begin_datetime,
                                                end_datetime=end_datetime)
        deps_of_interest = {}
        for loc_code in loc_codes:
            deps = self.get_deployments(loc_code, begin_datetime=begin_datetime, end_datetime=end_datetime)
            if device_category_codes is not None:
                doi = {dcc:dinfo for dcc, dinfo in deps.items() if dcc in device_category_codes}
            else:
                doi = deps
            if len(doi) != 0:
                deps_of_interest[loc_code] = doi
        return deps_of_interest



    def find_archive_files(self, location_code: str, device_category_code: str,
                           begin_datetime: None | datetime = datetime(2009,8,26),
                           end_datetime: None | datetime = None) -> list[str]:

        params = {'locationCode': location_code,
                  'deviceCategoryCode': device_category_code,
                  'dateFrom': dt2str(begin_datetime) if begin_datetime is not None else None,
                  'dateTo': dt2str(end_datetime) if end_datetime is not None else None}
        params = {k: v for k, v in params.items() if v is not None}

        response = self.getArchivefile(filters = params, allPages = True)
        files = response['files']
        return files


    def find_archive_file_urls(self, location_code: str, device_category_code: str,
                           begin_datetime: None | datetime = datetime(2009,8,26),
                           end_datetime: None | datetime = None) -> list[str]:

        params = {'locationCode': location_code,
                  'deviceCategoryCode': device_category_code,
                  'dateFrom': dt2str(begin_datetime) if begin_datetime is not None else None,
                  'dateTo': dt2str(end_datetime) if end_datetime is not None else None}
        params = {k: v for k, v in params.items() if v is not None}

        response = self.getArchivefileUrls(filters = params, allPages = True)
        #file_urls = response['fileUrls']
        return response


    def download_archive_files(self, archive_files: list[str], save_dir: os.PathLike, overwrite:bool = False) -> list[os.PathLike]:
        self.outPath = os.path.normpath(save_dir)

        local_files = []
        for archive_file in archive_files:
            save_fp = os.path.join(self.outPath,archive_file)
            if os.path.isfile(save_fp) and overwrite is False:
                local_files.append(save_fp)
                continue
            else:
                self.downloadArchivefile(archive_file, overwrite = overwrite)
                local_files.append(save_fp)
        return local_files



    def import_txt_from_url(self, url, keep_conditions: list[str] | None = None,
                            drop_conditions: list[str] | None= None,
                            splitter: str = '\n',
                            stream = True, timeout = 60):
        with requests.get(url, stream=stream, timeout = timeout) as response:
            txt = response.text
        lines = txt.split(splitter)
        if keep_conditions is not None:
            for keep_condition in keep_conditions:
                lines = [line for line in lines if keep_condition in line]
        if drop_conditions is not None:
            for drop_conditions in drop_conditions:
                lines = [line for line in lines if drop_conditions not in line]
        return lines
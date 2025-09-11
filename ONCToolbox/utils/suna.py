from datetime import datetime
import numpy as np
from numpy.typing import NDArray, ArrayLike
import os
import pandas as pd
import requests
from typing import NamedTuple
import warnings
import xarray as xr



NUM_WVLS = 256  # The number of wavelengths output by the SUNAv2.
FILL_VALUE = -9999  # The fill value to use if a value is not present in the SUNAv2 output.


class SynchronizedFrame(NamedTuple):
    """
    A data structure representing the Full ASCII output of a SUNAv2 instrument.

    Note that this structure is slightly different from the style and naming conventions described in the SUNAv2
    manual. For example, the frame type and serial number are separated into different fields.
    """
    time: datetime
    frame_type: str  # The frame  descriptor. For FullASCII frames this will either be SATSDF (dark) or SATSLF (light).
    instrument_sn: str  # The zero-padded serial number of the SUNAv2.
    instrument_date: int  # The date of the instrument in the format YYYYJJJ, where JJJ is the Juian day of the year.
    instrument_time: float  # The floating point time of in the instrument in hours based on a 24 hour clock.
    nitrate_concentration_uncorrected: float  # Nitrate uncorrected for the effects of temperature, salinity, and pressure.
    nitrogen_in_nitrate_uncorrected: float
    absorbance_at_254_nm: float
    absorbance_at_350_nm: float
    bromide_trace: float
    spectrum_average: int
    fit_dark_value: int
    integration_time_factor: int
    intensity: list[int]
    internal_temperature: float
    spectrometer_temperature: float
    lamp_temperature: float
    lamp_on_time: int
    internal_relative_humidity: float
    main_voltage: float
    lamp_voltage: float
    internal_voltage: float
    main_current: float
    fit_aux_1: float
    fit_aux_2: float
    fit_base_1: float
    fit_base_2: float
    fit_rmse: float

    # ONC does not (and should not) connect a CTD to the SUNAv2, so this data can be discarded later.
    connected_ctd_time: float | int
    connected_ctd_practical_salinity: float | int
    connected_ctd_temperature: float | int
    connected_ctd_pressure: float | int

    checksum: int


class CalibrationInfo(NamedTuple):
    line_id: str
    wavelength: float
    nitrate_extinction_coefficient: float
    swa: float
    tswa: float
    reference: float



class SUNAv2ArchiveFileParser:
    def __init__(self):
        pass

    def read_suna_cal_file(self, filepath: os.PathLike) -> tuple[list[str], list[str]]:
        """
        Ingest an ONC SUNAv2 calibration file and return the sensor metadata and calibration informatin lines.
        :param filepath: The filepath of the SUNAv2 calibration file.
        :return: A tuple of lists containing header lines and calibration lines, respectively.
        """
        with open(filepath, 'r') as _file:
            lines = _file.readlines()
        header_lines = [line for line in lines if line.startswith('H,')]
        cal_lines = [line for line in lines if line.startswith('E,')]
        return (header_lines, cal_lines)

    def parse_cal_info(self, cal_lines: list[str]) -> xr.Dataset:
        calibration_info = []
        for cal_line in cal_lines:
            line_split = cal_line.split(',')
            cinfo = CalibrationInfo(line_id=line_split[0],
                                    wavelength=float(line_split[1]),
                                    nitrate_extinction_coefficient=float(line_split[2]),
                                    swa=float(line_split[3]),
                                    tswa=float(line_split[4]),
                                    reference=float(line_split[5].strip()))
            calibration_info.append(cinfo._asdict())
        caldf = pd.DataFrame(calibration_info)
        calds = caldf.to_xarray()
        calds = calds.swap_dims({'index': 'wavelength'})
        calds = calds.drop_vars(['index', 'line_id'], errors='ignore')
        return calds

    def read_suna_archive_file(self, filepath: os.PathLike) -> tuple[list[str], list[str], list[str]]:
        """
        Ingest an ONC SUNAv2 archive file and return data in the form of diagnostic lines, frame lines and malformed lines.

        Diagnostic lines consist of both ONC controller messages and SUNAv2 internal driver messages.
        Frame lines consist of actual frames containing data from the SUNAv2. The validity of these frames is not tested
            at this point.
        Malformed lines consist of frame lines that are incomplete because they contain diagnostic information.

        :param filepath: The filepath of the SUNAv2 archive file.
        :return: A tuple of lists containing diagnostic lines, frame lines, and malformed lines, respectively.
        """
        with open(filepath, 'r') as _file:
            lines = _file.readlines()
        diagnostic_lines = [line for line in lines if 'SAT' not in line]
        frame_lines = [line for line in lines if 'SAT' in line and ':' not in line]
        malformed_lines = [line for line in lines if 'SAT' in line and ':' in line]
        return (diagnostic_lines, frame_lines, malformed_lines)

    def parse_frame(self, archive_file_line: str) -> SynchronizedFrame:
        """
        Parse a single line containing frame data from a SUNAv2 archive file and return a SynchronizedFrame object.

        :param archive_file_line: A single line from a SUNAv2 archive file containing frame data, including the common
            network time from ONC and the line terminator.
        :return: A SynchronizedFrame object containing the parsed data.
        """
        try:
            time, frame = archive_file_line.split(' ')
        except:
            return None

        time = datetime.strptime(time, '%Y%m%dT%H%M%S.%fZ')
        parts = frame.split(',')
        sf = SynchronizedFrame(time=time,
                               frame_type=parts[0][0:6],
                               instrument_sn=parts[0][6:],
                               instrument_date=int(parts[1]),
                               # Sensor date in the format of year and julian day (YYYYJJJ)
                               instrument_time=float(parts[2]),
                               nitrate_concentration_uncorrected=float(parts[3]),
                               nitrogen_in_nitrate_uncorrected=float(parts[4]),
                               absorbance_at_254_nm=float(parts[5]),
                               absorbance_at_350_nm=float(parts[6]),
                               bromide_trace=float(parts[7]),
                               spectrum_average=int(parts[8]),
                               fit_dark_value=int(parts[9]),
                               integration_time_factor=int(parts[10]),
                               intensity=[int(v) for v in parts[11:-19]],  # Expectation is a list of 256 integers.
                               internal_temperature=float(parts[-19]),
                               spectrometer_temperature=float(parts[-18]),
                               lamp_temperature=float(parts[-17]),
                               lamp_on_time=int(parts[-16]),
                               internal_relative_humidity=float(parts[-15]),
                               main_voltage=float(parts[-14]),
                               lamp_voltage=float(parts[-13]),
                               internal_voltage=float(parts[-12]),
                               main_current=float(parts[-11]),
                               fit_aux_1=float(parts[-10]),
                               fit_aux_2=float(parts[-9]),
                               fit_base_1=float(parts[-8]),
                               fit_base_2=float(parts[-7]),
                               fit_rmse=float(parts[-6]),
                               connected_ctd_time=int(parts[-5]) if len(parts[-5]) > 0 else FILL_VALUE,
                               connected_ctd_practical_salinity=float(parts[-4]) if len(
                                   parts[-4]) > 0 else FILL_VALUE,
                               connected_ctd_temperature=float(parts[-3]) if len(parts[-3]) > 0 else FILL_VALUE,
                               connected_ctd_pressure=float(parts[-2]) if len(parts[-2]) > 0 else FILL_VALUE,
                               checksum=int(parts[-1].strip()))
        return sf

    def parse_frames(self, archive_file_lines: list[str]) -> xr.Dataset:
        #frames = [self.parse_frame(afl)._asdict() for afl in archive_file_lines]

        frames = []
        for afl in archive_file_lines:
            try:
                frame = self.parse_frame(afl)
                if frame is None:
                    continue
                elif len(frame.intensity) != NUM_WVLS:
                    continue
                else:
                    frames.append(frame._asdict())
            except:
                continue

        df = pd.DataFrame(frames)
        df.index = df.time
        df = df.drop(columns=['time'])
        df = df.sort_values(by='time')  # Time is now the primary index.
        ds = df.to_xarray()
        return ds


    def process_suna_lines(self, frame_lines):
        wvl_placeholder = range(1, NUM_WVLS + 1)

        ds = self.parse_frames(frame_lines)

        ds = ds.drop_vars(['connected_ctd_time', 'connected_ctd_practical_salinity',
                           'connected_ctd_temperature', 'connected_ctd_pressure'], errors='ignore')

        str_vars = ['frame_type', 'instrument_sn']
        for str_var in str_vars:
            if str_var in ds.data_vars:
                ds[str_var] = ds[str_var].astype(str)

        big_int_vars = ['instrument_date', 'spectrum_average']
        for big_int_var in big_int_vars:
            if big_int_var in ds.data_vars:
                ds[big_int_var] = ds[big_int_var].astype('int64')

        small_int_vars = ['fit_dark_value', 'integration_time_factor', 'lamp_on_time']
        for small_int_var in small_int_vars:
            if small_int_var in ds.data_vars:
                ds[small_int_var] = ds[small_int_var].astype('int32')

        ds = ds.assign_coords({'wvl_idx': wvl_placeholder})


        nd = np.array(ds.intensity.values.tolist())

        ds['intensity'] = (('time','wvl_idx'), nd)

        return ds


    def import_suna_archive_file(self, filepath: os.PathLike):




        diagnostic_lines, frame_lines, malformed_lines = self.read_suna_archive_file(filepath)
        ds = self.process_suna_lines(frame_lines)


        return ds





    def import_suna_cal_file(self, filepath: os.PathLike):
        header_lines, cal_lines = self.read_suna_cal_file(filepath)
        calds = self.parse_cal_info(cal_lines)
        return calds


    def valid_frame_line(self, frame: str | SynchronizedFrame) -> bool:
        """
        If the input is a string, check if a line within a SUNAv2 archive file is a valid frame line.
        If the input is a SynchronizedFrame, check the length of the intensity list.

        If invalid, issue a warning with the timestamp of the line.

        :param archive_file_line: The line data from an ONC SUNAv2 archive file that is expected to contain a frame.
        :return: A boolean indicating whether the line is a valid frame line.
        """

        if isinstance(frame, str) and ':' in frame:
            msg = f"Malformed frame located at {frame.split(' ')[0]}."
            warnings.warn(msg)
            return False
        elif isinstance(frame, SynchronizedFrame):
            if len(frame.intensity) != NUM_WVLS:
                t = frame.common_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                msg = f"Invalid frame located at {t}. Expected {NUM_WVLS} wavelengths, but found {len(frame.intensity)}."
                warnings.warn(msg)
                return False
        else:
            return True


    def compute_raw_absorbance(self, i_s: xr.DataArray | NDArray,
                               i_ref: xr.DataArray | ArrayLike,
                               i_dark: xr.DataArray | ArrayLike) -> xr.DataArray | NDArray:
        """
        Compute raw absorbance from SUNAv2 intensity counts.

        All inputs must have units of counts.
        i_s is the intensity of the sample, as measured by the SUNAv2 during deployment. This value is the intensity
            from the SUNAv2 output.
            It should be 2-dimensional (time x wavelength).
        i_ref is the intensity of a DI water reference, measured by the SUNAv2 in a controlled cold room before deployment.
            It should be 1-dimensional (wavelength).
        i_dark is the intensity of the dark frame preceding the light frames during deployment. This value is the
            fit_dark_value from the SUNAv2 output. It is also known as the dark current in literature and represents the
            non-zero noise when the system is not taking samples.
            It should be 1-dimensional (time).

        # Note: Occasionally the reference intensity counts from the SUNAv2 cal files may be saved as floating point values.

        :param i_s: The intensity of the sample, as measured by the SUNAv2 during deployment. Units of counts.
        :param i_ref: The intensity of a DI water reference measured prior to deployment. Units of counts.
        :param i_dark: The intensity of the dark frame preceding the light frames during deployment. Units of counts.
        :return: Raw absorbance. Unitless.
        """

        raw_absorbance = -np.log((i_s - i_dark) / (i_ref - i_dark))
        return raw_absorbance



    def split_frames_by_type(self, ds: xr.Dataset):

        dark_frames = ds.where(ds.frame_type.str.contains('DF'), drop=True)
        light_frames = ds.where(ds.frame_type.str.contains('LF'), drop=True)

        return light_frames, dark_frames




    def import_suna_from_url(self,url, keep_conditions: list[str] | None = ['SAT'],
                            drop_conditions: list[str] | None= ['::', '<', '[',']'],
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
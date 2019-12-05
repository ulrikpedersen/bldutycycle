from datetime import datetime
from aa.js import JsonFetcher
from controlsystem import get_shtr_short_name, get_shutter_pvs, ShutterStatus
import pandas as pd
import pytz
from os.path import abspath, join, curdir

DLS_ARCHIVE_URL_PORT = ('archappl.diamond.ac.uk', 80)

start_time = pytz.utc.localize(datetime(2019, 9, 3))
end_time = pytz.utc.localize(datetime(2019, 10, 24))


def fetch_and_store_archived_pvs(pvs, start, end, file_name):
    archive = JsonFetcher(*DLS_ARCHIVE_URL_PORT)
    # For compression see: http://pandas-docs.github.io/pandas-docs-travis/user_guide/io.html#compression
    with pd.HDFStore(file_name, 'w') as store:
        for pv in pvs:
            pv_data = archive.get_values(pv, start, end)
            shutter_name = get_shtr_short_name(pv_data.pv)
            df = pd.DataFrame({F"{shutter_name}": pv_data.values.squeeze(),
                               # 'severity': pv_data.severities
                               },
                              index=pv_data.utc_datetimes)
            store.put(shutter_name, df)


def fetch_and_store_shutters(beamline: str, start, end, dest=None):
    if dest is None:
        dest = curdir
    beamline_file = {}
    fname = abspath(join(dest, F"{beamline}-shutters.h5"))
    fetch_and_store_archived_pvs(get_shutter_pvs(beamline), start, end, fname)
    beamline_file.update({beamline: fname})
    return beamline_file


def load_shutter_archive_data_from_file(file_name):
    with pd.HDFStore(file_name, 'r') as store:
        print(store.keys())
        data_frames = {}
        for key in store.keys():
            df = store.get(key)
            data_frames.update({key: df})
    return data_frames


def merge_archive_dataframes(data_frames: dict, keys: list):
    if keys is None:
        keys = list(data_frames.keys())
    first = data_frames[keys[0]]
    # Join note: when passing a list, the lsuffix and rsuffix are not used
    #            - BUT then the column names in each dataframe MUST be unique.
    merged_dataframe = first.join([data_frames[key] for key in keys[1:]],
                                  how='outer')
    merged_dataframe.fillna(method='ffill', inplace=True)
    return merged_dataframe


def get_open_shutters(merged_dataframes):
    """Return a dataframe with two columns: machine_shutter, beamline_shutters"""
    open_shutters = merged_dataframes == ShutterStatus.OPEN.value
    machine_shutter = open_shutters['fe_shtr1']
    machine_shutter.name = 'machine_shutter'
    beamline_shutters = open_shutters['fe_shtr2'] & \
                        open_shutters['bl_shtr1'] & \
                        open_shutters['bl_shtr2']
    beamline_shutters.name = 'beamline_shutters'

    all_open_shutters = machine_shutter & beamline_shutters
    all_open_shutters.name = 'all_shutters'

    result = merged_dataframes.join([machine_shutter, beamline_shutters, all_open_shutters], how='outer')
    return result


def load_process_shutter_data(file_name):
    """Convenience function to load shutter data and extract it into a simple view of machine and beamline shutters"""
    dfs = load_shutter_archive_data_from_file(file_name)
    df = merge_archive_dataframes(dfs, None)
    df = get_open_shutters(df)
    return df


def get_delta_times(shutter_state: pd.Series):
    # Convert the boolean shutter state into integers to enable calculations
    changes = pd.Series(shutter_state, dtype='int8')
    # Detect changes of state
    changes = changes.diff()
    # Drop all samples between state changes
    changes = changes.mask(changes == 0).dropna()
    # Convert changes back in to boolean to get state
    state = pd.Series(changes.replace(-1, 0), dtype='bool')
    # Calculate state time periods by differentiating between neighbouring timestamps
    state_periods_series = (changes.index - pd.Series(changes.index).shift()).shift(periods=-1)
    state_periods_series.index = changes.index
    # Finally generate a DataFrame with two columns for state and period.
    # The two columns have the same DateTime index.
    state_periods = pd.DataFrame({'period': state_periods_series,
                                  'state': state})
    return state_periods


def duty_cycle_report(file_name):
    open_shutters = load_process_shutter_data(file_name)

    beamline_open_shutters = get_delta_times(open_shutters['all_shutters'])
    machine_open_shutters = get_delta_times(open_shutters['machine_shutter'])

    machine_open_time = machine_open_shutters.loc[machine_open_shutters['state']==True]['period'].sum()
    beamline_open_time = beamline_open_shutters.loc[beamline_open_shutters['state']==True]['period'].sum()

    print(F"Period from: {open_shutters.index[0]} to: {open_shutters.index[-1]}")
    print(F"Machine shutters open:  {machine_open_time} ({machine_open_time.seconds} seconds)")
    print(F"Beamline shutters open: {beamline_open_time} ({beamline_open_time.seconds} seconds)")
    print(F"BL use of available beam: {beamline_open_time / machine_open_time}")

from datetime import datetime
import re
from aa.js import JsonFetcher
import numpy
import pandas as pd
import pytz
import requests
from os.path import abspath, join, curdir

DLS_ARCHIVE_URL_PORT = ('archappl.diamond.ac.uk', 80)

start_time = pytz.utc.localize(datetime(2019, 9, 3))
end_time = pytz.utc.localize(datetime(2019, 10, 24))

shutter_pvs = {
    'i13': [
        "FE13I-PS-SHTR-01:STA",
        "FE13I-PS-SHTR-02:STA",
        "BL13I-PS-SHTR-01:STA",
        "BL13I-PS-SHTR-02:STA"],
    'j13': [
        "FE13I-PS-SHTR-01:STA",
        "FE13I-PS-SHTR-02:STA",
        "BL13J-PS-SHTR-01:STA",
        "BL13J-PS-SHTR-02:STA",
    ],
    'i12': [
        "FE12I-PS-SHTR-01:STA",
        "FE12I-PS-SHTR-02:STA",
        "BL12I-PS-SHTR-01:STA",
        "BL12I-PS-SHTR-02:STA",
        "BL12I-PS-SHTR-03:STA",
    ]
}

# EPICS Shutter status (:STA) PV states
SHUTTER_FAIL=0
SHUTTER_OPEN=1
SHUTTER_OPENING=2
SHUTTER_CLOSED=3
SHUTTER_CLOSING=4


class MachineScheduleItem:
    def __init__(self, item: dict):
        self._item = item

    @staticmethod
    def _str_to_datetime(dt_str: str):
        return pd.to_datetime(dt_str, utc=True)

    @property
    def start(self):
        return self._str_to_datetime(self._item['fromdatetime'])

    @property
    def end(self):
        return self._str_to_datetime(self._item['todatetime'])

    @property
    def duration(self):
        return self.end - self.start

    @property
    def description(self):
        year = self.start.year
        return F"{self.run} - {self._item['typedescription']}"

    @property
    def run(self):
        return F"{self.start.year} {self._item['description']}"

    def __str__(self):
        s = F"<MachineScheduleItem: {self.description}>"
        return s

    def __repr__(self):
        return str(self)


class MachineSchedule:
    def __init__(self):
        self._url = 'http://rdb.pri.diamond.ac.uk/php/opr/cs_oprgetjsonyearcal.php'
        self._cal = {}

    def _get_machine_calendar(self, year):
        # Machine Calendar REST API: https://confluence.diamond.ac.uk/x/zAB_Aw
        resp = requests.get('http://rdb.pri.diamond.ac.uk/php/opr/cs_oprgetjsonyearcal.php',
                            params={'CALYEAR': year},
                            headers={'content-type': 'application/json'})
        if resp.status_code != 200:
            raise RuntimeError('Unable to get machine calendar from http://rdb.pri.diamond.ac.uk')

        cal = resp.json()
        if len(cal) == 1:
            cal = cal[0]

        self._cal.update({year: cal})
        return cal

    def get_run(self, year, run):
        if year not in self._cal:
            self._get_machine_calendar(year)

        run_items = []
        for run_item in self._cal[year]['run'][str(run)]:
            run_items.append(MachineScheduleItem(run_item))
        return run_items

    def get_year(self, year):
        if year not in self._cal:
            self._get_machine_calendar(year)

        runs = []
        for run in self._cal[year]['run'].keys():
            runs.append(self.get_run(year, run))
        return runs


REGEXP_SHTR = re.compile(r'^([A-Z]{2})(\d{2}[IJK])-[A-Z]{2}-SHTR-(\d{2}):STA$')
def get_shtr_short_name(shtr_pv):
    match_groups = REGEXP_SHTR.findall(shtr_pv)[0]
    area = match_groups[0].lower()
    area_no = match_groups[1][2].lower() + match_groups[1][:-1]
    shtr_no = int(match_groups[2])
    short_name = F"{area}_shtr{shtr_no}"
    return short_name


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


def fetch_and_store_shutters(beamlines_shutter_pvs: dict, start, end, dest=None):
    if dest is None:
        dest = curdir

    beamline_file = {}
    for beamline in beamlines_shutter_pvs:
        fname = abspath(join(dest, F"{beamline}-shutters.h5"))
        fetch_and_store_archived_pvs(shutter_pvs[beamline], start, end, fname)
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
    open_shutters = merged_dataframes == SHUTTER_OPEN
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

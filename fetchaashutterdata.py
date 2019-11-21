from datetime import datetime
import re
from aa.js import JsonFetcher
import numpy
import pandas as pd
import pytz
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


def normalise_pv_name(pv_name):
    """Normalise a PV name for use as panda/pytables key in data store (hdf5)
    Panda/pytables keys can't have dashes '-' or colon ':'"""
    return pv_name.replace('-', '_').replace(':', '_')


# TODO: implement function to create a shorthand for shutters. Like fe_shtr1, bl_shtr2, etc..
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

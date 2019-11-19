from datetime import datetime
from aa.js import JsonFetcher
import numpy
import pandas as pd
import pytz
from os.path import abspath, join, curdir

archive = JsonFetcher('archappl.diamond.ac.uk', 80)

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


def normalise_pv_name(pv_name):
    """Normalise a PV name for use as panda/pytables key in data store (hdf5)
    Panda/pytables keys can't have dashes '-' or colon ':'"""
    return pv_name.replace('-', '_').replace(':', '_')


def fetch_and_store_archived_pvs(pvs, start, end, file_name):
    # For compression see: http://pandas-docs.github.io/pandas-docs-travis/user_guide/io.html#compression
    with pd.HDFStore(file_name, 'w') as store:
        for pv in pvs:
            pv_data = archive.get_values(pv, start, end)
            df = pd.DataFrame({'values': pv_data.values.squeeze(),
                               #'severity': pv_data.severities
                               },
                              index=pv_data.utc_datetimes)
            shutter_name = normalise_pv_name(pv_data.pv)
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
    # combined = s2.join(s3, how='outer', lsuffix='_s2', rsuffix='_s3')
    # combined.fillna(method='ffill')
    if keys is None:
        keys = list(data_frames.keys())
    first = data_frames[keys[0]]
    merged_dataframe = first.join([data_frames[key] for key in keys[1:]],
                                  how='outer')

    return merged_dataframe

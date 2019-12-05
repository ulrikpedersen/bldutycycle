import re
from enum import Enum

# TODO: stuff this information into a DB/file somehow
# TODO: extend with all beamline shutter PVs
shutter_pvs = {
    'i13': {
        'machine': [
            "FE13I-PS-SHTR-01:STA"],
        'beamline': [
            "FE13I-PS-SHTR-02:STA",
            "BL13I-PS-SHTR-01:STA",
            "BL13I-PS-SHTR-02:STA"]
    },
    'j13': {
        'machine': [
            "FE13I-PS-SHTR-01:STA"],
        'beamline': [
            "FE13I-PS-SHTR-02:STA",
            "BL13J-PS-SHTR-01:STA",
            "BL13J-PS-SHTR-02:STA"]
    },
    'i12': {
        'machine': [
            "FE12I-PS-SHTR-01:STA"],
        'beamline': [
            "FE12I-PS-SHTR-02:STA",
            "BL12I-PS-SHTR-01:STA",
            "BL12I-PS-SHTR-02:STA",
            "BL12I-PS-SHTR-03:STA"]
    }
}


class ShutterStatus(Enum):
    """EPICS Shutter status PV (:STA) enumerated values"""
    FAIL = 0
    OPEN = 1
    OPENING = 2
    CLOSED = 3
    CLOSING = 4


_REGEXP_SHTR = re.compile(r'^([A-Z]{2})(\d{2}[IJK])-[A-Z]{2}-SHTR-(\d{2}):STA$')


def get_shtr_short_name(shtr_pv):
    match_groups = _REGEXP_SHTR.findall(shtr_pv)[0]
    area = match_groups[0].lower()
    area_no = match_groups[1][2].lower() + match_groups[1][:-1]
    shtr_no = int(match_groups[2])
    short_name = F"{area}_shtr{shtr_no}"
    return short_name


def get_shutter_pvs(beamline):
    return shutter_pvs[beamline]['machine'] + shutter_pvs[beamline]['beamline']

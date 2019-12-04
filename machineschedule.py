import requests
import pandas as pd

_MACHINE_SCHEDULE_RDB_URL='http://rdb.pri.diamond.ac.uk/php/opr/cs_oprgetjsonyearcal.php'


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
        self._url = _MACHINE_SCHEDULE_RDB_URL
        self._cal = {}

    def _get_machine_calendar(self, year):
        # Machine Calendar REST API: https://confluence.diamond.ac.uk/x/zAB_Aw
        resp = requests.get(self._url,
                            params={'CALYEAR': year},
                            headers={'content-type': 'application/json'})
        if resp.status_code != 200:
            raise RuntimeError(F'Unable to get machine calendar from {self._url}')

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

    def get_run_total_beamtime(self, year, run):
        run = self.get_run(year, run)
        beamtimes = pd.Series([r.duration for r in run])
        return beamtimes.sum()

    def get_year(self, year):
        if year not in self._cal:
            self._get_machine_calendar(year)

        runs = []
        for run in self._cal[year]['run'].keys():
            runs.append(self.get_run(year, run))
        return runs

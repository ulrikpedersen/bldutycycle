At home install with local hdf5 and blosc:

```
export HDF5_DIR=/Users/ulrik/develop/install
export BLOSC_DIR=/usr/local/Cellar/c-blosc/1.15.0
```


PyCharm autoreload: first install ipython with pip. 
Then in the ipython shell:

```shell script
In [1]: %load_ext autoreload

In [2]: %autoreload 2

```

About filtering out combined data from multiple columns; use DataFrame.loc:
https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.loc.html#pandas.DataFrame.loc

For example to get all events where shutter was open:

```dfs['/FE12I_PS_SHTR_02_STA'].loc[dfs['/FE12I_PS_SHTR_02_STA']['values']==3]```

Shutter Enum Statuses:
```shell script
[ 0] Fault
[ 1] Open
[ 2] Opening
[ 3] Closed
[ 4] Closing
```


Console history:
```python
import os.path
os.path.join(os.path.curdir(), 'hello', '.h5')
os.path.join(os.path.curdir, 'hello', '.h5')
runfile('/Users/ulrik/PycharmProjects/bldutycycle/fetch-aa-shutter-data.py', wdir='/Users/ulrik/PycharmProjects/bldutycycle')
fetch_and_store_shutters(shutter_pvs, start_time, end_time)
pd
store = pd.HDFStore('i13-shutters.h5', 'r')
store.keys()
s1=store.get('BL13I_PS_SHTR_01_STA')
s1
%load_ext autoreload

%autoreload 2

bl_files = fetch_and_store_shutters(shutter_pvs, start_time, end_time)
bl_files = fetch_and_store_shutters(shutter_pvs, start_time, end_time, dest='data')
dfs = load_shutter_archive_data_from_file(bl_files['i12'])
bl_files
merge_archive_dataframes(dfs, None)
dfs
dfs['/FE12I_PS_SHTR_02_STA'] == 4
dfs['/FE12I_PS_SHTR_02_STA'].loc[dfs['/FE12I_PS_SHTR_02_STA'] == 4]
dfs['/FE12I_PS_SHTR_02_STA']
type(dfs['/FE12I_PS_SHTR_02_STA'])
dfs['/FE12I_PS_SHTR_02_STA'].loc[dfs['/FE12I_PS_SHTR_02_STA', 'values'] == 4]
dfs['/FE12I_PS_SHTR_02_STA'].loc[dfs['/FE12I_PS_SHTR_02_STA', ['values']] == 4]
dfs['/FE12I_PS_SHTR_02_STA'].loc[dfs['/FE12I_PS_SHTR_02_STA']==4, ['values']]
dfs['/FE12I_PS_SHTR_02_STA'].loc[dfs['/FE12I_PS_SHTR_02_STA']==4, 'values']
df = dfs['/FE12I_PS_SHTR_02_STA']
df.loc[df['values'] ==4]
dfs['/FE12I_PS_SHTR_02_STA'].loc[dfs['/FE12I_PS_SHTR_02_STA']['values']==4]
dfs['/FE12I_PS_SHTR_02_STA'].loc[dfs['/FE12I_PS_SHTR_02_STA']['values']==3]
%load_ext autoreload


type(df)
df.columns
df_open(df)
type(df_open)
a=df_open.plot.line()
type(a)
a.figure.get_figure()
a.figure.show()
df_open.columns
sp = df_open.plot.line()
sp
sp.axis()
sp.show()
df = load_shutter_archive_data_from_file('data/i12-shutters.h5')
df = merge_archive_dataframes(df, None)
df_open = get_open_shutters(df)
df_open['machine_shutter']
type(df_open['machine_shutter'])
ms = df_open['machine_shutter']
import pandas as pd
import numpy as np
pd.Series(ms, dtype=np.int8)
ms = pd.Series(ms, dtype=np.int8)
ms
ms.plot.line()
sp = ms.plot.line()
sp.figure.show()
df = load_process_shutter_data('data/i12-shutters.h5')
df
df.index.tz_localize()
df.index.tz_localize(None)
df.index = df.index.tz_localize(None)
df.index
df.to_excel('data/i12-shutters.xlsx')
%load_ext autoreload
%matplotlib
%autoreload 2
from fetchaashutterdata import *
sd = load_process_shutter_data('data/i12-shutters.h5')
sd
sd.columns
changes = pd.Series(sd['all_shutters'], dtype='int8')
changes = changes.diff()
changes.where(changes = 0)
changes.mask(changes = 0)
changes.mask(changes == 0)
changes.mask(changes == 0).dropna()
changes = changes.mask(changes == 0).dropna()
changes_shft_tstamp = changes.index.shift()
changes_shft_tstamp = changes.index.shift(1)
changes
changes.index
changes_tstamp = pd.Series(changes.index)
changes_tstamp
type(changes_tstamp)
type(changes.index)
changes_tstamp.shift()
changes.index - changes_tstamp.shift()
```
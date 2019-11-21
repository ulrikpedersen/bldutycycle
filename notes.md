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

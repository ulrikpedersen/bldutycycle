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

# To get matplotlib plotting support in ipython shell:
In [3]: %matplotlib
Using matplotlib backend: TkAgg
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

Plotting
--------

Not possible to plot pd.Series of booleans. Have to convert to int first...

To plot time series events/states use `.plot(drawstyle="steps-post")` (for pandas polotting)
or `plt.step(where='post')` for [matplotlib plotting](https://matplotlib.org/examples/pylab_examples/step_demo.html) 

Get a histogram of how long shutters are open for: first convert deltatime into seconds, then plot hist: 
`df.loc[df['state']==True, ['period']].astype('timedelta64[m]').hist(bins=np.arange(0, 60*8, 1))`


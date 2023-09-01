# %%
import pandas as pd
import matplotlib.pylab as plt
import numpy as np

# %%
s = pd.Series([1,2,3,4,5])
dates = pd.date_range("20230101",periods=6)
df = pd.DataFrame(np.random.randn(6,4), index=dates, columns=list("ABCD"))
ts = pd.Series(np.random.randn(1000), index=pd.date_range("1/1/2000", periods=1000))
ts = ts.cumsum()
ts.plot();

# %% 
df = pd.DataFrame(
    np.random.randn(1000, 4), index=ts.index, columns=["A", "B", "C", "D"]
)
df = df.cumsum()
plt.figure();
df.plot();
plt.legend(loc='best');
# %%

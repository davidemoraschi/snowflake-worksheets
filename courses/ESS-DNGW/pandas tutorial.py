import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

s = pd.Series([1,2,3,4,5])
print(s)

dates = pd.date_range("20230101",periods=6)
print(dates)

df = pd.DataFrame(np.random.randn(6,4), index=dates, columns=list("ABCD"))
print(df)

plt.close("all")
ts = pd.Series(np.random.randn(1000), index=pd.date_range("1/1/2000", periods=1000))
ts = ts.cumsum()
ts.plot();
import numpy as np
import pandas as pd
from patterns import Hammer,HangingMan,Engulfing


df = pd.read_csv('./data.csv')
df = df.rename(columns={"open": "Open", "close": "Close", "high":"High", "low":"Low", "volume":"Volume"})
hammer = Engulfing(df)
output_df = hammer.compute_pattern()
output_df.to_csv('./hammer.csv')
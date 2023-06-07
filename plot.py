import plotly.graph_objects as go
import pandas as pd
df = pd.read_csv('./signals/ZRXUSDT_5m_now.csv')
tmp_df = df.iloc[:10]
fig = go.Figure(data=[go.Candlestick(x=tmp_df['time'],
        open=tmp_df['open'],
        high=tmp_df['high'],
        low=tmp_df['low'],
        close=tmp_df['close'])])

fig.show()
fig.write_image("./fig1.jpeg")
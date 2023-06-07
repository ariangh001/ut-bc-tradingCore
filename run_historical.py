from binance_data import Binance_Data
# from ichimoku import Ichimoku
# from supertrend import SuperTrend
import pandas as pd
import os


SYMBOLS = ["ETHUSDT","XRPUSDT","ADAUSDT","SOLUSDT","SANDUSDT","BNBUSDT","LTCUSDT","LINKUSDT","MATICUSDT","BTCUSDT","ETHUSDT_221230"]
TF = ["5m","15m","30m","1h","2h","4h"]
for symbol in SYMBOLS:
    if(symbol == 'ETHUSDT'):
        # os.mkdir(f'./signals/{symbol}/new')
        for tf in TF:
            if(tf == '5m'):
                a = Binance_Data(symbol,tf)
                a.run_historical(False,1685997000000,1686120146126)
                # df = pd.read_csv(f'./signals/{symbol}/{tf}.csv')
                # # df = df[-3000:]
                # df.reset_index(drop=True,inplace=True)
                # # print(df)
                # # b = SuperTrend(df,True)
                # # df = b.historical_run(lookback=5,multiplier=2.5)
                # # print(df)
                # ichi = Ichimoku(df,True)
                # ichi.run_historical(f'./signals/others/{tf}_{symbol}_signals.csv')
                # print(tf, symbol)
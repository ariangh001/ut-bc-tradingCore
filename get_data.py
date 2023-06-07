import bitfinex as bfx
import pandas as pd
import numpy as np
import time
from datetime import datetime,timedelta,timezone
import sys


# # Initialize the api
# api = bfx.api_v2()

# # Select a trading pair
# pair = 'btcusd'

# # Get the current ticker data for the pair
# # api.candles(symbol=pair,interval='1m',limit=900)

# SYMBOL = 'matic:ust'
# TIME = '1D'
# NAME = 'data_1D_maticusd.csv'
# OUT = './1D_maticusd.csv'

CANDLES_NUM_LIVE = 2

TIMEFRAMES = ['1m','5m','15m','30m','1h','3h','4h','6h','12h','1D','7D','14D','1M']


class Get_data:

    def __init__(self,market,timeframe):
        self.api = bfx.api_v2()
        self.symbol = market
        self.fetch_limit = 1000
        self.time_frame = timeframe
        self.make_timedelta_list(CANDLES_NUM_LIVE)

    def get_live_data(self):
        end_date = datetime.now()
        step = self.make_step(self.time_frame)
        start_date = self.decode_time(end_date - step)
        end_date = self.decode_time(end_date)
        while(True):
            try:
                print(f"fetching candles of {self.symbol} of {self.time_frame} timeframe from {datetime.fromtimestamp(start_date // 1000)} to {datetime.fromtimestamp(end_date // 1000)}")
                candles = self.api.candles(symbol=self.symbol,interval=self.time_frame,limit=self.fetch_limit,start=start_date,end=end_date)
                break
            except:
                time.sleep(120)
                print('PROBLEM IN GETTING LIVE DATA !!!')

        column = ['time', 'open', 'close', 'high', 'low', 'volume']
        df = pd.DataFrame(candles, columns=column)
        df['time'] = pd.to_datetime(df['time'],unit = 'ms')
        return df

    def find_start_date(self, start_date, end_date, step):
        temp = end_date
        while temp > start_date:
            temp -= step
        return temp

    # Convert list to dataframe
    def decode_candles(self,df):
        columns = ['time', 'open', 'close', 'high', 'low', 'volume']
        ind = [np.ndim(x) != 0 for x in df]
        df = [i for (i, v) in zip(df, ind) if v]
        df = pd.DataFrame(df, columns=columns)
        return df

    #return miliseconds of datetime
    def decode_time(self,datetime):
        return int(datetime.timestamp()) * 1000

    # return timedelta needed for last n candles
    def make_step(self,time_frame):
        return self.time_deltas[time_frame]

    def make_timedelta_list(self,candles_num):
        self.time_deltas = {
            '1m' : timedelta(minutes = 1 * candles_num),
            '5m' : timedelta(minutes = 5 * candles_num),
            '15m' : timedelta(minutes = 15 * candles_num),
            '30m' : timedelta(minutes = 30 * candles_num),
            '1h' : timedelta(hours = 1 * candles_num),
            '3h' : timedelta(hours = 3 * candles_num),
            '4h' : timedelta(hours = 4 * candles_num),
            '6h' : timedelta(hours = 6 * candles_num),
            '12h' : timedelta(hours = 12 * candles_num),
            '1D' : timedelta(days = 1 * candles_num),
            '7D' : timedelta(days = 7 * candles_num),
            '14D' : timedelta(days = 14 * candles_num),
            '1M' : timedelta(days = 31 * candles_num)
        }

    def get_historical_data(self,start_date,end_date):
        # start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        # end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S.%f')
        self.make_timedelta_list(700)
        step = self.make_step(self.time_frame)
        start = self.find_start_date(start_date, end_date, step)
        start = start - step
        while(start < end_date):
            start = start + step
            end = start + step
            start_ms = self.decode_time(start)
            end_ms = self.decode_time(end)
            candles = self.api.candles(self.symbol,self.time_frame,self.fetch_limit,start_ms,end_ms)
            if('df' in locals()):
                df = df.append(self.decode_candles(candles))
            else:
                df = self.decode_candles(candles)
            time.sleep(1)
            print(len(df))

        # start_date = self.decode_time(start_date)
        print(df)
        df['time'] = pd.to_datetime(df['time'],unit='ms')
        # df.reset_index(drop=True,inplace=True)
        # df = df.rename(columns = {'0' : 'time' , '1' : 'open', '2' : 'close' , '3' : 'high', '4' : 'low', '5' : 'volume' })
        df = df[df['time'] >= start_date]
        return df

    def historical_data(self,start=1621987200000, stop=1631001600000, symbol='btcusd', interval='1h', tick_limit=1000, step=1000000000):
        data = pd.DataFrame()
        start = start - step
        while start < stop:

            start = start + step
            end = start + step
            res = self.api.candles(symbol=symbol, interval=interval, limit=tick_limit, start=start, end=end)
            data = data.append(res)
            print(data)
            print('Retrieving data from {} to {} for {}'.format(pd.to_datetime(start, unit='ms'),
                                                                pd.to_datetime(end, unit='ms'), symbol))
            time.sleep(1.5)

        data = data.reset_index(drop=True)
        data = data.iloc[:-6]
        print(data.columns)
        data.to_csv('tmp_data.csv')
        data = pd.read_csv('tmp_data.csv')
        data = data.sort_values(by='0')
        data['0'] = pd.to_datetime(data['0'],unit='ms')
        # data = data.drop(self.df.columns[0],axis=1,inplace=True)
        data.reset_index(drop=True,inplace=True)
        data = data.rename(columns = {'0' : 'time' , '1' : 'open', '2' : 'close' , '3' : 'high', '4' : 'low', '5' : 'volume' })
        data.to_csv('historical_data.csv')
        return data

    def get_symbols(self):
        api1 = bfx.api_v1()
        symbols = api1.symbols()
        return symbols


# L = Get_data('btcusd','1h')
# L.listen()
# end_date = datetime.now()
# step = L.make_step(L.time_frame)
# start_date = L.decode_time(end_date - step)
# end_date = L.decode_time(end_date)
# a = L.historical_data(stop=int(datetime.timestamp(datetime.now()) * 1000), symbol='btcusd',interval='1M')
# print(a)

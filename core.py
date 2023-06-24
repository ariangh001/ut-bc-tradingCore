from binance_data import Binance_Data
from database import Database
from telegram_bot import Telegram_Bot
from ichimoku import Ichimoku
from candles import Candles
import sys
import plotly.graph_objects as go
import pandas as pd
import time
from datetime import datetime
from dateutil import tz
from binance.websocket.spot.websocket_client import SpotWebsocketClient as wssClient
import logging

FULL_SYMBOL = 'ETHEREUM'
SYMBOL = 'ETHUSDT'
TIMEFRAME = '5m'

LIVE_DF_ADDRESS = './live_data.csv'
LIVE_SIGNAL_ADDRESS = './live_signals.csv'
CLOUD_FILE_ADDRESS = './cloud.csv'
DB_ADDRESS = './database.db'


live_candles = []
live_df = pd.DataFrame()

def make_data():
    global live_df
    global live_candles
    for i in range(len(live_candles)):
        live_candles[i]['time'] = pd.to_datetime(live_candles[i]['mts'],unit = 'ms')
    column = ['time', 'open', 'close', 'high', 'low', 'volume']
    live_df = pd.DataFrame(live_candles, columns=column)
    # live_df['time'] = live_df['time'].apply(GMT2UTC)
    live_df = live_df.reindex(index=live_df.index[::-1])

def new_candle(msg):
    try:
        global live_candles
        candle = {'mts': msg['k']['t'], 'open':float(msg['k']['o']), 'close':float(msg['k']['c']),\
                  'high':float(msg['k']['h']), 'low':float(msg['k']['l']),\
                  'volume':float(msg['k']['v']), 'symbol':msg['k']['s']}
        # print(f'New Candle : {candle}')
        live_candles.append(candle)
        if(len(live_candles) == 2):
            make_data()
            live_candles.clear()
    except:
        print('Error in Getting Live WSS Candle!!!')

def GMT2UTC(t):
        t = str(t)
        from_zone = tz.gettz('UTC')
        to_zone = tz.gettz('Iran/Tehran')
        utc = datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
        utc = utc.replace(tzinfo=from_zone)
        return str(utc.astimezone(to_zone))[:-6]

class Core:
    def __init__(self,symbol=SYMBOL,timeframe=TIMEFRAME):
        self.symbol = symbol
        self.timeframe = timeframe
        self.database = Database(DB_ADDRESS)
        self.live_data = Binance_Data(self.symbol,self.timeframe)
        self.df = self.live_data.get_historical_data(candles_num=1000,futures=False)
        self.df = self.df.sort_values(by='time')
        # self.df['time'] = self.df['time'].apply(GMT2UTC)
        self.df = self.df.iloc[-200:]
        self.df.reset_index(drop=True,inplace=True)
        self.df.to_csv(LIVE_DF_ADDRESS)
        self.telegram_bot = Telegram_Bot(self.timeframe)
        self.signals = pd.DataFrame()
        logging.basicConfig(filename='./core.log', filemode='a', format='%(levelname)s - %(thread)d - %(asctime)s  - %(relativeCreated)d - %(filename)s:%(lineno)s -%(funcName)20s() - %(message)s',level=logging.ERROR,force=True)
        self.logger = logging.getLogger()

    def plotFigure(self):
        tmp_df = self.df.iloc[-10:]
        fig = go.Figure(data=[go.Candlestick(x=tmp_df['time'],
                open=tmp_df['open'],
                high=tmp_df['high'],
                low=tmp_df['low'],
                close=tmp_df['close'])])
        fig.write_image("./fig1.jpeg")

    def run(self):
        global live_df
        signal_flag = False
        while(True):
            try:
                a = self.database.readCodesData(TIMEFRAME,True)
                if(len(live_df) > 0):
                    signal_flag = self.checkNewCandles(signal_flag)
                    buy_signal,sell_signal,message,adx, = self.technicalAnalysis(signal_flag)
                    if(buy_signal or sell_signal):
                        signal_code = self.database.readCodesData(TIMEFRAME,True)
                        self.database.modifyCodeNumber(TIMEFRAME,signal_code + 1,True)
                        self.plotFigure()
                        self.telegram_bot.send_signals(message,SYMBOL,FULL_SYMBOL,signal_code,adx,self.df.iloc[-1]['close'],self.df.iloc[-2]['evening_doji_star'],self.df.iloc[-2]['morning_doji_star'])
                        self.telegram_bot.send_photo(TIMEFRAME,'./fig1.jpeg')
                        signal_flag = True

                    time.sleep(3)
                    print(self.signals[-2:])
            except Exception as error:
                self.logger.error('######################')
                self.logger.error(error)
                print(error)

    def checkNewCandles(self,signal_flag):
        global live_df
        self.candles = Candles(self.df)
        self.df = self.candles.run()
        c = Candles(live_df)
        live_df = c.run()
        last_date = self.df.iloc[-1]['time']
        for ind in live_df.index:
            index = self.df[self.df['time'] == live_df.iloc[ind]['time']].index
            self.df.iloc[index] = live_df.iloc[ind]
        if(live_df.iloc[0]['time'] != last_date):
            self.df = self.live_data.get_historical_data(candles_num=200,futures=False)
            self.df = self.df.sort_values(by='time')
            # self.df['time'] = self.df['time'].apply(GMT2UTC)
            self.df.reset_index(drop=True,inplace=True)
            signal_flag = False
        return signal_flag

    def technicalAnalysis(self,signal_flag):
        buy_signal = False
        sell_signal = False
        message = ''
        self.df.reset_index(drop=True,inplace=True)
        self.ichimoku = Ichimoku(self.df)
        self.signals = self.ichimoku.run(LIVE_SIGNAL_ADDRESS,self.symbol,self.timeframe,CLOUD_FILE_ADDRESS)
        if(self.signals.iloc[-2]['BUY'] == 1):
            buy_signal = True
            message = 'Buy'

        elif(self.signals.iloc[-2]['SELL'] == 1):
            sell_signal = True
            message = 'Sell'

        return (buy_signal and not(signal_flag)),(sell_signal and not(signal_flag)),message,(self.signals.iloc[-1]['ADX'])

    def backtest(self,start,end):
        a = Binance_Data(self.symbol,self.timeframe)
        a.run_historical(futures=False,start=start,end=end)
        self.df = pd.read_csv(f'./data.csv')
        # self.df['time'] = self.df['time'].apply(GMT2UTC)
        self.df.reset_index(drop=True,inplace=True)
        # self.candles = Candles(self.df)
        # self.df = self.candles.run()
        signal = [0 for i in range(len(self.df))]
        ichimoku = Ichimoku(self.df)
        signals = ichimoku.run(LIVE_SIGNAL_ADDRESS,self.symbol,self.timeframe,CLOUD_FILE_ADDRESS)
        for i in range(len(self.df)):
            if(signals.iloc[i-1]['BUY'] == 1):
                signal[i] = 'BUY'

            elif(signals.iloc[i-1]['SELL'] == 1):
                signal[i] = 'SELL'

        self.df['BUY'] = signals['BUY']
        self.df['SELL'] = signals['SELL']
        self.df['Signal'] = signal
        self.df['ADX'] = signals['ADX']
        self.df.to_csv(f'./{self.symbol}_{self.timeframe}.csv')
        return self.df


if __name__ == "__main__":
    try:
        my_client = wssClient()
        my_client.start()
        my_client.kline(symbol=SYMBOL, id=1, interval=TIMEFRAME, callback=new_candle)
        core = Core()
        core.run()
        # core.backtest(start=round(time.time() * 1000) - 604800000,end=round(time.time() * 1000))
    except KeyboardInterrupt:
        my_client.stop()
        sys.exit(0)



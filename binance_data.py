import pandas as pd
from binance.spot import Spot as restClient
from binance.websocket.spot.websocket_client import SpotWebsocketClient as wssClient
from binance.um_futures import UMFutures
class Binance_Data:
    def __init__(self,symbol="BTCUSDT",timeframe="15m"):
        self.symbol = symbol
        self.timeframe = timeframe
        self.makeMiliSeconds()

    def makeMiliSeconds(self):
        self.times = {
            '5m' : 5 * 6000,
            '15m' : 15 * 6000,
            '1h' : 60 * 6000,
            '4h' : 4 * 60 * 6000,
            '1D' : 24 * 60 * 4000,
            '30m' : 30 * 6000,
            '2h' : 2 * 60 * 6000
        }
    def get_historical_data(self,candles_num=200,start=None,end=None,futures=False):
        spot_client = restClient(base_url="https://api1.binance.com")
        um_futures_client = UMFutures()
        historical_data = []
        if(start):
            # 1m = 6000 ms
            limit = 1000 * self.times[self.timeframe]
            while start < end:
                try:
                    tmp = []
                    if(futures):
                        tmp = um_futures_client.klines(symbol=self.symbol,interval=self.timeframe,startTime=start,endTime=end,limit=1000)
                    else:
                        tmp = spot_client.klines(self.symbol,self.timeframe,startTime=start,endTime=end,limit=1000)
                    for i in range(len(tmp)):
                        if (len(historical_data) == 0):
                            historical_data.append(tmp[i])
                        elif(len(historical_data) > 0 and tmp[i][0] > historical_data[-1][0]):
                            historical_data.append(tmp[i])
                except Exception as e:
                    print(e)
                    print('Error In Getting Historical Data')
                    break
                start += limit
        else:
            if(futures):
                historical_data = um_futures_client.klines(symbol=self.symbol,interval=self.timeframe,limit=candles_num)
            else:
                historical_data = spot_client.klines(self.symbol,self.timeframe,limit=candles_num)
        for i in range(len(historical_data)):
            historical_data[i][0] = pd.to_datetime(historical_data[i][0],unit = 'ms')
            historical_data[i] = historical_data[i][0:6]
        final = pd.DataFrame(historical_data,columns=['time', 'open','high', 'low', 'close',  'volume'])
        final[['open','high', 'low', 'close',  'volume']] = final[['open','high', 'low', 'close',  'volume']].apply(pd.to_numeric)
        final = final.reindex(['time', 'open','close','high','low','volume'],axis=1)
        print(final)
        return final


    def message_handler(self,msg):
        try:
            print('close:  ',msg['k']['c'])
            print('open:  ',msg['k']['o'])
            print('high:  ',msg['k']['h'])
            print('low:  ',msg['k']['l'])
            print('volume:  ',msg['k']['v'])
            print('symbol:  ',msg['k']['s'])
            print('###############################')
        except:
            pass

    def websocket(self):
        my_client = wssClient()
        my_client.start()
        my_client.kline(symbol="btcusdt", id=1, interval="1m", callback=self.message_handler)


    def run_historical(self,futures=False,start=1659295800000,end=1669405957182):
        #1609446600000
        #1657103982
        final = self.get_historical_data(10000,start=start,end=end,futures=futures)
        times = final['time'].dt.time
        dates = final['time'].dt.date
        # for i in range(len(final)):
            # final.loc[i , 'Hours'] , final.loc[i , 'Minutes'], final.loc[i , 'Seconds']  = str(times[i]).split(':')
            # final.loc[i , 'Year'] , final.loc[i , 'Month'], final.loc[i , 'Day'] = str(dates[i]).split('-')
        # final = final.reindex(['time','Year','Month','Day','Hours','Minutes','Seconds','open','close','high','low','volume'],axis=1)
        final = final.reindex(['time','open','close','high','low','volume'],axis=1)
        print(final)
        final.to_csv(f'./data.csv')


if __name__ == "__main__":
    tf = "15m"
    symbol = "SOLUSDT"
    a = Binance_Data(symbol,tf)
    a.run_historical()


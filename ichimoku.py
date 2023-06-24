import numpy as np
import pandas as pd
from database import Database
DB_ADDRESS = '../../database.db'

class Ichimoku:
    def __init__(self,df,is_historical=False):
        self.df = df
        # self.df = df[['time', 'open', 'close', 'high', 'low', 'volume']]
        # if(is_historical):
        #     self.df = df[['time','Year','Month','Day','Hours','Minutes','Seconds','open','close','high','low','volume']]
        self.db = Database(DB_ADDRESS)

    def calc_ema(self,df,span,adjust=False):
        return df.ewm(span = span, adjust=adjust).mean()

    def calc_sma(self,df,span):
        return df.rolling(window = span).mean()

    def calc_rma(self,df,span,adjust=False):
        return df.ewm(com = span-1, adjust=adjust).mean()

    def calculate_ADX(self,lookback=14):
        # DM+ & DM-
        dm_plus = self.df['high'].diff()
        dm_minus = self.df['low'].diff()
        dm_plus[dm_plus < 0] = 0
        dm_minus[dm_minus > 0] = 0

        tr1 = pd.DataFrame(self.df['high'] - self.df['low'])
        tr2 = pd.DataFrame(abs(self.df['high'] - self.df['close'].shift(1)))
        tr3 = pd.DataFrame(abs(self.df['low'] - self.df['close'].shift(1)))
        tr = pd.concat([tr1,tr2,tr3],axis=1,join='inner').max(axis = 1)
        atr = self.calc_rma(tr,lookback)

        di_plus = pd.DataFrame(100 * (self.calc_rma(dm_plus,lookback) / atr)).rename(columns={0:'DI+'})
        di_minus = pd.DataFrame(abs(100 * (self.calc_rma(dm_minus,lookback) / atr))).rename(columns={0:'DI-'})

        dx = (abs(di_plus['DI+'] - di_minus['DI-']) / (abs(di_plus['DI+'] + di_minus['DI-'] + 1))) * 100
        adx = ((dx.shift(1) * (lookback - 1)) + dx) / lookback
        adx_smooth = pd.DataFrame(self.calc_rma(adx,lookback)).rename(columns={0:'ADX'})

        self.df = pd.concat([self.df,di_plus,di_minus,adx_smooth],axis=1)

    def calculate_rolling(self,window):
        high_period =  self.df['high'].rolling(window=window).max()
        low_period = self.df['low'].rolling(window=window).min()
        return (high_period + low_period) / 2

    def calculate_ichimoku(self,conversion_length=9,base_line_length=26,span_b_length=52,displacement=-26,no_shift=False):
        if(not(no_shift)):
            displacement = 0
        else :
            displacement = -26
        # Calculate Tenkan-Sen
        tenkansen = pd.DataFrame(self.calculate_rolling(conversion_length), columns = ['Tenkan_Sen'])

        # Calculate Kijun-Sen
        kijunsen = pd.DataFrame(self.calculate_rolling(base_line_length),  columns = ['Kijun_Sen'])

        # Calculate Span A
        span_a = pd.DataFrame(((tenkansen['Tenkan_Sen'] + kijunsen['Kijun_Sen']) / 2).shift(26 + displacement), columns = ['Span_A'])

        # Calculate Span B
        span_b = pd.DataFrame((self.calculate_rolling(span_b_length)).shift(26 + displacement), columns = ['Span_B'])

        # Calculate Cloud Phase
        cloud = pd.DataFrame(np.sign(span_a['Span_A'] - span_b['Span_B']), columns = ['Cloud Phase'])

        # Caculate Cloud Phase Change
        cloud['Cloud_Change'] = cloud.loc[(np.sign(cloud['Cloud Phase']).diff().ne(0))]
        cloud['Cloud_Change'] = (cloud['Cloud_Change'].notnull()).astype('int')

        # Calculate Chiko-Span
        chiko_span = pd.DataFrame((self.df['close'].shift(-26))).rename(columns = {'close' : 'Chiko_Span'})

        self.df = pd.concat([self.df,tenkansen,kijunsen,span_a,span_b,cloud,chiko_span],axis=1)


    def save_cloud_phase(self,symbol,timeframe,address='../../cloud.csv'):
        current_cloud = self.db.readCloudData(timeframe,symbol)
        if(current_cloud != self.df['Cloud Phase'].iloc[-1]):
            self.db.modifyCloudData(timeframe,self.df['Cloud Phase'].iloc[-1],symbol)

    def calculate_signals(self):
        # Check if Chiko-Span is Higher than Price
        buy_condition_1 = pd.DataFrame((self.df['Chiko_Span'] > self.df['close']).shift(26), columns = ['Chiko_Buy_Condition'])
        sell_condition_1 = pd.DataFrame((self.df['Chiko_Span'] < self.df['close']).shift(26), columns = ['Chiko_Sell_Condition'])

        # Check if Price is Above the Cloud
        buy_condition_2 = pd.DataFrame((self.df['close'] > (self.df[['Span_A','Span_B']]).max(axis=1, skipna=False)), columns = ['Cloud_Buy_Condition'])
        sell_condition_2 = pd.DataFrame((self.df['close'] < (self.df[['Span_A','Span_B']]).min(axis=1, skipna=False)), columns = ['Cloud_Sell_Condition'])

        # Calculate Tenkan-Sen & Kijun-Sen Cross
        self.detect_crossover(self.df,'Tenkan_Sen','Kijun_Sen')
        self.df = self.df.rename(columns = {'up_cross_Tenkan_Sen_Kijun_Sen' : 'Ichimoku_Buy_Crossover' , 'down_cross_Tenkan_Sen_Kijun_Sen' : 'Ichimoku_Sell_Crossover'})

        self.df = pd.concat([self.df,buy_condition_1,sell_condition_1,buy_condition_2,sell_condition_2],axis=1)

    def detect_signals(self,use_adx=True):
        ADX_VALUE = 30
        buy_signals = [0 for i in range(len(self.df))]
        sell_signals = [0 for i in range(len(self.df))]
        buy_crosses = self.df[self.df['Ichimoku_Buy_Crossover'] == True].index
        sell_crosses = self.df[self.df['Ichimoku_Sell_Crossover'] == True].index
        all_crosses = []
        for i in range(len(buy_crosses)):
            all_crosses.append([buy_crosses[i],'buy'])
        for i in range(len(sell_crosses)):
            all_crosses.append([sell_crosses[i],'sell'])
        all_crosses = sorted(all_crosses,key=lambda x: (x[0],x[1]))
        for i in range(len(all_crosses)):
            if(i == len(all_crosses) - 1):
                if(all_crosses[i][1] == 'buy'):
                    new_df = self.df.iloc[all_crosses[i][0]:]
                    adx_condition = (new_df['ADX'] > ADX_VALUE) & (new_df['DI+'] > new_df['DI-'])
                    main_condition =  (new_df['Cloud_Buy_Condition'] == True) & (new_df['Chiko_Buy_Condition'] == True)
                    if(use_adx):
                        main_condition &= adx_condition
                    indices = new_df[main_condition].index
                    if(len(indices) > 0):
                        buy_signals[indices[0]] = 1
                else:
                    if(all_crosses[i][1] == 'sell'):
                        new_df = self.df.iloc[all_crosses[i][0]:]
                        adx_condition = (new_df['ADX'] > ADX_VALUE) & (new_df['DI+'] < new_df['DI-'])
                        main_condition = (new_df['Cloud_Sell_Condition'] == True) & (new_df['Chiko_Sell_Condition'] == True)
                        if(use_adx):
                            main_condition &= adx_condition
                        indices = new_df[main_condition].index
                        if(len(indices) > 0):
                            sell_signals[indices[0]] = 1

            else:
                if(all_crosses[i][1] == 'buy'):
                    new_df = self.df.iloc[all_crosses[i][0]:all_crosses[i+1][0]]
                    adx_condition = (new_df['ADX'] > ADX_VALUE) & (new_df['DI+'] > new_df['DI-'])
                    main_condition = (new_df['Cloud_Buy_Condition'] == True) & (new_df['Chiko_Buy_Condition'] == True)
                    if(use_adx):
                        main_condition &= adx_condition
                    indices = new_df[main_condition].index
                    if(len(indices) > 0):
                        buy_signals[indices[0]] = 1
                else:
                    if(all_crosses[i][1] == 'sell'):
                        new_df = self.df.iloc[all_crosses[i][0]:all_crosses[i+1][0]]
                        adx_condition = (new_df['ADX'] > ADX_VALUE) & (new_df['DI+'] < new_df['DI-'])
                        main_condition = (new_df['Cloud_Sell_Condition'] == True) & (new_df['Chiko_Sell_Condition'] == True)
                        if(use_adx):
                            main_condition &= adx_condition
                        indices = new_df[main_condition].index
                        if(len(indices) > 0):
                            sell_signals[indices[0]] = 1

        self.df['BUY'] = buy_signals
        self.df['SELL'] = sell_signals

    def detect_crossover(self,df,src_1,src_2):
        prev_src_1 = df[src_1].shift(1)
        prev_src_2 = df[src_2].shift(1)
        df[f'up_cross_{src_1}_{src_2}'] = (prev_src_1 <= prev_src_2) & (df[src_1] > df[src_2])
        df[f'down_cross_{src_1}_{src_2}'] = (prev_src_2 <= prev_src_1) & (df[src_2] > df[src_1])
        return df

    def save_file(self,signals_address):
        # final_df = self.df[(self.df['BUY'] == 1) | (self.df['SELL'] == 1)]
        # final_df = final_df[['time','open','close','high','low','BUY','SELL']]
        # print(self.df)
        final_df = self.df[['time','open','close','high','low','BUY','SELL','ADX','DI-','DI+','Tenkan_Sen','Kijun_Sen','Span_A','Span_B','Chiko_Span'
                            ,'Cloud Phase','Ichimoku_Buy_Crossover','Ichimoku_Sell_Crossover','Chiko_Buy_Condition','Chiko_Sell_Condition'
                            ,'Cloud_Buy_Condition','Cloud_Sell_Condition']]
        final_df.sort_index(inplace=True)
        final_df.reset_index(drop=True,inplace=True)
        final_df.to_csv(signals_address)
        return final_df

    def run(self,signals_address,symbol,timeframe,cloud_file_address):
        self.df.reset_index(drop=True,inplace=True)
        self.calculate_ADX()
        self.calculate_ichimoku(no_shift=True)
        # self.save_cloud_phase(symbol,timeframe,cloud_file_address)
        self.calculate_signals()
        self.detect_signals()
        self.df.to_csv(signals_address)
        return self.df

    def run_historical(self,signals_address):
        self.df.reset_index(drop=True,inplace=True)
        self.calculate_ADX()
        self.calculate_ichimoku(no_shift=True)
        # self.save_cloud_phase(symbol,timeframe,cloud_file_address)
        self.calculate_signals()
        self.detect_signals()
        final_df = self.save_file(signals_address)
        # self.df.to_csv(signals_address)
        return final_df


if __name__ == "__main__":
    df = pd.read_csv('./signals/SOLUSDT/15m.csv')
    # print(df)
    ichi = Ichimoku(df,True)
    ichi.run_historical('./signals/SOLUSDT/15_signals_new.csv')
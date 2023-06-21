import numpy as np
import pandas as pd

class Candles:
    def __init__(self,df):
        self.df = df

    def is_evening_doji_star(self,i):
        main_condition_1 = (self.df.iloc[i]['close'] == self.df.iloc[i]['open']) or (abs(round(self.df.iloc[i]['open'] - self.df.iloc[i]['close'], 2)) < (self.df.iloc[i]['high'] - self.df.iloc[i]['low']))
        main_condition_2 = (self.df.iloc[i-1]['close'] > self.df.iloc[i-1]['open']) and (self.df.iloc[i-1]['close'] <= min(self.df.iloc[i]['close'] , self.df.iloc[i]['open']))
        if (main_condition_1 and main_condition_2):
            self.df.loc[i ,'evening_doji_star'] = True
        else:
            self.df.loc[i ,'evening_doji_star'] = False

    def is_morning_doji_star(self,i):
        main_condition_1 = (self.df.iloc[i]['close'] == self.df.iloc[i]['open']) or (abs(round(self.df.iloc[i]['open'] - self.df.iloc[i]['close'], 2)) < (self.df.iloc[i]['high'] - self.df.iloc[i]['low']))
        main_condition_2 = (self.df.iloc[i-1]['close'] < self.df.iloc[i-1]['open']) and (self.df.iloc[i-1]['close'] >= self.df.iloc[i]['open'])
        if (main_condition_1 and main_condition_2):
            self.df.loc[i ,'morning_doji_star'] = True
        else:
            self.df.loc[i ,'morning_doji_star'] = False

    def run(self,address='./doji.csv'):
        for i in range(1,len(self.df)):
            self.is_evening_doji_star(i)
            self.is_morning_doji_star(i)
        # self.df.drop(columns=self.df.columns[0], axis=1, inplace=True)
        self.df.to_csv(address)
        return self.df


if __name__ == "__main__":
    df = pd.read_csv('./data.csv')
    jp_candles = Candles(df)
    jp_candles.run()
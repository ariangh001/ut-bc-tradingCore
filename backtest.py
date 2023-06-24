import numpy as np
import pandas as pd


class Backtest:
    def __init__(self,df,symbol,tf):
        self.df = df
        self.symbol = symbol
        self.timeframe = tf
        self.openTrades = []
        self.closeTrades = []
        self.initMoney = 1000
        self.leverage = 1
        self.risk = 1
        self.money = self.initMoney
        self.last_id = 1
        self.profitsNum = 0
        self.lossNum = 0

    def calcInMoney(self,candle):
        return (self.money * self.risk) * 0.9996

    def checkOpen(self,candle):
        if(candle['BUY'] == 1 or candle['SELL'] == 1):
            if(candle['BUY'] == 1):
                sig_type = 'BUY'
            else:
                sig_type = 'SELL'
            self.openTrades.append({'id' : self.last_id,
                                    'Open_Date' : candle['time'],
                                    'type' : sig_type,
                                    'Entry_Price' : candle['close'],
                                    'In_Money' : self.calcInMoney(candle),
                                    'Leverage' : self.leverage,
                                    'Risk' : self.risk})
            self.money -= self.calcInMoney(candle)
            self.last_id += 1

    def closeTrade(self,trade,candle,closeType):
        if(closeType == 'TP'):
            self.profitsNum += 1
            trade['Result'] = 'TP'
            trade['Out_Money'] = trade['In_Money'] * (1 + self.leverage * abs(candle['close'] - trade['Entry_Price'])/trade['Entry_Price']) * 0.9996
            self.money += trade['Out_Money']
            trade['Total_Money'] = self.money
            trade['Close_Date'] = candle['time']
            trade['Close_Price'] = candle['close']
            trade['profitsNum'] = self.profitsNum
            trade['lossNum'] = self.lossNum
            self.closeTrades.append(trade)

        else:
            self.lossNum += 1
            trade['Result'] = 'SL'
            trade['Out_Money'] = trade['In_Money'] * 0.997 *  0.9996
            self.money += trade['Out_Money']
            trade['Total_Money'] = self.money
            trade['Close_Date'] = candle['time']
            trade['Close_Price'] = candle['close']
            trade['profitsNum'] = self.profitsNum
            trade['lossNum'] = self.lossNum
            self.closeTrades.append(trade)

        self.openTrades[:] = [d for d in self.openTrades if d.get('id') != trade['id']]

    def checkClose(self,candle):
        for trade in self.openTrades:
            if(trade['type'] == 'BUY'):
                if(candle['close'] >= 1.003 * trade['Entry_Price'] or candle['close'] <= 0.997 * trade['Entry_Price']):
                    if(candle['close'] >= trade['Entry_Price']):
                        self.closeTrade(trade,candle,'TP')
                    else:
                        self.closeTrade(trade,candle,'SL')
            else:
                if(candle['close'] <= 0.997 * trade['Entry_Price'] or candle['close'] >= 1.003 * trade['Entry_Price']):
                    if(candle['close'] <= trade['Entry_Price']):
                        self.closeTrade(trade,candle,'TP')
                    else:
                        self.closeTrade(trade,candle,'SL')

    def run(self,addr):
        if (addr == None):
            addr = f'./backtest/backtest/{self.symbol}_{self.timeframe}.xlsx'
        for i in range(len(self.df)):
            self.checkClose(self.df.iloc[i])
            if(len(self.openTrades) < 1):
                self.checkOpen(self.df.iloc[i])
        result = pd.DataFrame(self.closeTrades)
        result.to_excel(addr)
        return result


if __name__ == "__main__":
    df = pd.read_csv('./ETHUSDT_5m.csv')
    back = Backtest(df,'ETHUSDT','5m')
    back.run('./final9.xlsx')

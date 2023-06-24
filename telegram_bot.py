# https://betterprogramming.pub/how-to-get-data-from-telegram-82af55268a4b
from numpy import sign
import pandas as pd
import requests


SIGNALS_ADDRESS = f'./live_signals.csv'
TOKEN = '6125508932:AAFuLnnD8_mj5ZRfUdfoZerOVBP6eIXkT0E'
CHAT_ID = {
    '5m': '-1001957911080'
}


class Telegram_Bot:
    def __init__(self,timeframe):
        self.timeframe = timeframe

    def send_photo(self, id, image_path, image_caption=""):
        data = {"chat_id": CHAT_ID[id], "caption": image_caption}
        url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"
        with open(image_path, "rb") as image_file:
            ret = requests.post(url, data=data, files={"photo": image_file})
        return ret.json()

    def send_text(self, data, id):
        url = f'https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID[id]}&text={data}'
        requests.get(url)

    def make_data_private_channel(self, df, symbol_full, symbol_short, signal,adx, code,close,evening_doji,morning_doji):
        signal_color = '🔴'
        adx_color = '🔴'
        morning_doji_color = '🔴'
        evening_doji_color = '🔴'
        if(signal == 'Buy'):
            signal_color = '🟢'
        if(adx >= 25):
            adx_color = '🟢'
        if(morning_doji):
            morning_doji_color = '🟢'
        if(evening_doji):
            evening_doji_color = '🟢'
        return f"\n" \
                f"Code : {code}\n" \
                f"Date : {df.iloc[-1]['time']}\n" \
                f"Pair : {symbol_short} Binance\n" \
                f"Price : {close:.2f}\n" \
                f"Signal : {signal} {signal_color}\n" \
                f"ADX: {adx:.2f} {adx_color}\n" \
                f"Evening Doji: {evening_doji_color}\n" \
                f"Morning Doji: {morning_doji_color}\n"
                # f"Entry : Between {0.997 * df.iloc[-2]['close']:.3f} And {1.003 * df.iloc[-2]['close']:.3f}\n" \

    def make_data_binance_channel(self,trade,code):
        data = "\n"
        data += f"Code : {code}\n"
        for key in trade:
            data += f"{key} : {trade[key]}\n"
        return data

    def send_signals(self, signal, symbol_short,symbol_full,code,adx,close,evening_doji,morning_doji):
        df = pd.read_csv(SIGNALS_ADDRESS)
        private_data = self.make_data_private_channel(df,symbol_full,symbol_short,signal,adx,code,close,evening_doji,morning_doji)
        self.send_text(private_data,f'{self.timeframe}')

    def send_binance_info(self,code,trades):
        for trade in trades:
            data = self.make_data_binance_channel(trade,code)
            self.send_text(data,'Binance_Channel')
        self.send_text('#######################','Binance_Channel')

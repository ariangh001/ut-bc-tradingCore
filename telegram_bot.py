# https://betterprogramming.pub/how-to-get-data-from-telegram-82af55268a4b
from numpy import sign
import pandas as pd
import requests


SIGNALS_ADDRESS = f'./live_data/live_signals.csv'
TOKEN = '2006821052:AAHBjW_kHOnwNzU_CPOfFsCCBW_ceABWu9E'
CHAT_ID = {
    '5m' : ''
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

    def make_data_private_channel(self, df, symbol_full, symbol_short, signal,adx, code,close):
        signal_color = '🔴'
        adx_color = '🔴'
        if(signal == 'Buy'):
            signal_color = '🟢'
        if(adx >= 22):
            adx_color = '🟢'
        return f"\n" \
                f"Code : {code}\n" \
                f"Date : {df.iloc[-1]['time']}\n" \
                f"Pair : {symbol_short} Binance\n" \
                f"Price : {close:.2f}\n" \
                f"Signal : {signal} {signal_color}\n" \
                f"ADX: {adx:.2f} {adx_color}\n"
                # f"Entry : Between {0.997 * df.iloc[-2]['close']:.3f} And {1.003 * df.iloc[-2]['close']:.3f}\n" \

    def make_data_binance_channel(self,trade,code):
        data = "\n"
        data += f"Code : {code}\n"
        for key in trade:
            data += f"{key} : {trade[key]}\n"
        return data

    def send_signals(self, signal, symbol_short,symbol_full,code,adx,close):
        df = pd.read_csv(SIGNALS_ADDRESS)
        private_data = self.make_data_private_channel(df,symbol_full,symbol_short,signal,adx,code,close)
        self.send_text(private_data,f'{self.timeframe}')

    def send_binance_info(self,code,trades):
        for trade in trades:
            data = self.make_data_binance_channel(trade,code)
            self.send_text(data,'Binance_Channel')
        self.send_text('#######################','Binance_Channel')

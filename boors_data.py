import pytse_client as tse

class Boors:
    def __init__(self,symbol="وبملت"):
        self.symbol = symbol

    def get_data(self):
        data = tse.download(symbols=self.symbol, write_to_csv=True)
        self.df = data[self.symbol]
        self.df = self.df.reindex(['date','open','adjClose','high','low','volume','value','count'],axis=1)
        self.df = self.df.rename(columns={"date": "time", "adjClose": "close"})
        return self.df



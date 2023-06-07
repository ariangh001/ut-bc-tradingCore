import sqlite3
import pandas as pd
import sys
import logging

COINS_SYMBOL = ["KAVAUSDT", "RENUSDT" ,"COMPUSDT" ,"DYDXUSDT", "SOLUSDT"]
TIMEFRAMES = ['5m','1h','4h','1d']
RISK_MANAGER_COLS = ['Symbol','Timeframe','SL','TP','TPS','SLS','Leverage','Position_Size']
OPEN_TRADES_COLS = ['Code','Open_Date','Symbol','Timeframe','Type','Open_Price','SL','TP','TPS','SLS','Leverage','In_Money','Status']
CLOSE_TRADES_COLS = ['Code','Open_Date','Close_Date','Symbol','Timeframe','Type','Open_Price','SL','TP','TPS','SLS','Leverage','In_Money','Out_Money','Close_Percent','Profit_Value','Profit_Percent','ProfitOrLoss','Status']
CLOUD_TABLE = 'Cloud'
SIGNAL_CODES_TABLE = 'Signal_Codes'
TRADE_CODES_TABLE = 'Trade_Codes'
OPEN_TRADES_TABLE = 'Open_Trades'
CLOSE_TRADES_TABLE = 'Close_Trades'
RISK_MANAGER_TABLE = 'Risk_Manager'
SIGNAL_CODES_ADDRESS = './last_codes/signal_codes.csv'
TRADE_CODES_ADDRESS = './last_codes/trade_codes.csv'
OPEN_TRADES_ADDRESS = './last_codes/open_trades.csv'
CLOSE_TRADES_ADDRESS = './last_codes/close_trades.csv'
RISK_MANAGER_ADDRESS = './last_codes/risk_manager.csv'

class Database:
    def __init__(self,db_address):
        try:
            logging.basicConfig(filename='Database.log', filemode='a', format='%(levelname)s - %(thread)d - %(asctime)s  - %(relativeCreated)d - %(message)s',level=logging.ERROR)
            self.connection = sqlite3.connect(db_address)
        except Exception as e:
            logging.error('Error in Connecting to Database !!!',exc_info=True)
            print('Error in Connecting to Database !!!')
            print(e)

    def createTable(self,name,columns):
        cursor = self.connection.cursor()
        command = f'CREATE TABLE {name} ('
        for col in columns:
            command += f'{col} TEXT,'
        command = command[:-1]
        command += ');'
        cursor.execute(command)
        self.connection.commit()

    # Table Name, Data(List)
    def addData(self,table,data):
        cursor = self.connection.cursor()
        command = f'INSERT INTO {table} VALUES ('
        for col in data:
            command += f'"{col}", '
        command = command[:-2]
        command += ');'
        cursor.execute(command)
        self.connection.commit()

    def addOpenTrade(self,data):
        self.addData(OPEN_TRADES_TABLE,data)

    def addCloseTrade(self,data):
        self.addData(CLOSE_TRADES_TABLE,data)

    def modifyCloudData(self,tf,data,symbol):
        log_error = 1
        while(True):
            try:
                cursor = self.connection.cursor()
                command = f'UPDATE {CLOUD_TABLE} SET tf_{tf} = "{data}" WHERE symbol = "{symbol}";'
                cursor.execute(command)
                self.connection.commit()
                return
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Modifying Cloud Data !!!',exc_info=True)
                    log_error = 0

    def modifyCodeNumber(self,tf,data,is_signal=True):
        log_error = 1
        while(True):
            try:
                cursor = self.connection.cursor()
                table = SIGNAL_CODES_TABLE
                if(not(is_signal)):
                    table = TRADE_CODES_TABLE
                command = f'UPDATE {table} SET number = "{data}" WHERE tf = "{tf}"; '
                cursor.execute(command)
                self.connection.commit()
                return
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Modifying Codes Data !!!',exc_info=True)
                    log_error = 0

    def readCloudData(self,tf,symbol):
        log_error = 1
        while(True):
            try:
                cursor = self.connection.cursor()
                command = f'SELECT tf_{tf} FROM Cloud WHERE symbol = "{symbol}";'
                ans = cursor.execute(command).fetchall()
                self.connection.commit()
                return float(ans[0][0])
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Reading Cloud Data !!!',exc_info=True)
                    log_error = 0

    def readCodesData(self,tf,is_signal=True):
        log_error = 1
        while(True):
            try:
                table = SIGNAL_CODES_TABLE
                if(not(is_signal)):
                    table = TRADE_CODES_TABLE
                cursor = self.connection.cursor()
                command = f'SELECT number FROM {table} WHERE tf = "{tf}";'
                ans = cursor.execute(command).fetchall()
                self.connection.commit()
                return int(ans[0][0])
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Reading Codes Data !!!',exc_info=True)
                    log_error = 0

    def readOpenTradesData(self,tf,symbol,params=None):
        log_error = 1
        while(True):
            try:
                table = OPEN_TRADES_TABLE
                command = f'SELECT * FROM {table} WHERE Timeframe = "{tf}" AND Symbol = "{symbol}" '
                if params:
                    for key in params:
                        command += f'AND {key} = "{params[key]}" '
                command += ';'
                return pd.read_sql_query(command,self.connection)
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Reading Open Trades Data !!!',exc_info=True)
                    log_error = 0

    def modifyOpenTrade(self,code,params):
        log_error = 1
        while(True):
            try:
                cursor = self.connection.cursor()
                table = OPEN_TRADES_TABLE
                for key in params:
                    command = f'UPDATE {table} SET {key} = "{params[key]}" WHERE Code = "{code}"; '
                    cursor.execute(command)
                self.connection.commit()
                return
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Modifying Open Trades Data !!!',exc_info=True)
                    log_error = 0

    def readCloseTradesData(self,tf,symbol,params=None):
        log_error = 1
        while(True):
            try:
                table = CLOSE_TRADES_TABLE
                command = f'SELECT * FROM {table} WHERE Timeframe = "{tf}" AND Symbol = "{symbol}" '
                if params:
                    for key in params:
                        command += f'AND {key} = "{params[key]}" '
                command += ';'
                return pd.read_sql_query(command,self.connection)
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Reading Close Trades Data !!!',exc_info=True)
                    log_error = 0

    def modifyCloseTrade(self,code,conds,params):
        log_error = 1
        while(True):
            try:
                cursor = self.connection.cursor()
                table = CLOSE_TRADES_TABLE
                for key in params:
                    command = f'UPDATE {table} SET {key} = "{params[key]}" WHERE Code = "{code}" '
                    if conds:
                        for cond in conds:
                            command += f'AND {cond} = "{conds[cond]}" '
                    command += ';'
                    cursor.execute(command)
                self.connection.commit()
                return
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Modifying Close Trades Data !!!',exc_info=True)
                    log_error = 0

    def deleteOpenTrade(self,code,params=None):
        log_error = 1
        while(True):
            try:
                cursor = self.connection.cursor()
                command = f'DELETE FROM {OPEN_TRADES_TABLE} WHERE Code = {code}'
                if params:
                    for key in params:
                        command += f'AND {key} = "{params[key]}" '
                command += ';'
                cursor.execute(command)
                self.connection.commit()
                return
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Delete Open Trades Data !!!',exc_info=True)
                    log_error = 0

    def deleteCloseTrade(self,code,params=None):
        log_error = 1
        while(True):
            try:
                cursor = self.connection.cursor()
                command = f'DELETE FROM {CLOSE_TRADES_TABLE} WHERE Code = {code}'
                if params:
                    for key in params:
                        command += f'AND {key} = "{params[key]}" '
                command += ';'
                cursor.execute(command)
                self.connection.commit()
                return
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Delete Close Trades Data !!!',exc_info=True)
                    log_error = 0

    def readRiskManager(self,symbol,tf):
        log_error = 1
        while(True):
            try:
                table = RISK_MANAGER_TABLE
                command = f'SELECT * FROM {table} WHERE Timeframe = "{tf}" AND Symbol = "{symbol}" ;'
                return pd.read_sql_query(command,self.connection)
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Reading Risk Manager Data !!!',exc_info=True)
                    log_error = 0

    def readSL(self,symbol,tf):
        log_error = 1
        while(True):
            try:
                table = RISK_MANAGER_TABLE
                command = f'SELECT SL FROM {table} WHERE Timeframe = "{tf}" AND Symbol = "{symbol}" ;'
                return pd.read_sql_query(command,self.connection)
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Reading SL !!!',exc_info=True)
                    log_error = 0

    def readTP(self,symbol,tf):
        log_error = 1
        while(True):
            try:
                table = RISK_MANAGER_TABLE
                command = f'SELECT TP FROM {table} WHERE Timeframe = "{tf}" AND Symbol = "{symbol}" ;'
                return pd.read_sql_query(command,self.connection)
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Reading TP !!!',exc_info=True)
                    log_error = 0

    def readSLS(self,symbol,tf):
        log_error = 1
        while(True):
            try:
                table = RISK_MANAGER_TABLE
                command = f'SELECT SLS FROM {table} WHERE Timeframe = "{tf}" AND Symbol = "{symbol}" ;'
                return pd.read_sql_query(command,self.connection)
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Reading SLS !!!',exc_info=True)
                    log_error = 0

    def readTPS(self,symbol,tf):
        log_error = 1
        while(True):
            try:
                table = RISK_MANAGER_TABLE
                command = f'SELECT TPS FROM {table} WHERE Timeframe = "{tf}" AND Symbol = "{symbol}" ;'
                return pd.read_sql_query(command,self.connection)
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Reading TPS !!!',exc_info=True)
                    log_error = 0

    def readLeverage(self,symbol,tf):
        log_error = 1
        while(True):
            try:
                table = RISK_MANAGER_TABLE
                command = f'SELECT Leverage FROM {table} WHERE Timeframe = "{tf}" AND Symbol = "{symbol}" ;'
                return pd.read_sql_query(command,self.connection)
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Reading Leverage !!!',exc_info=True)
                    log_error = 0

    def readPositionSize(self,symbol,tf):
        log_error = 1
        while(True):
            try:
                table = RISK_MANAGER_TABLE
                command = f'SELECT Position_Size FROM {table} WHERE Timeframe = "{tf}" AND Symbol = "{symbol}" ;'
                return pd.read_sql_query(command,self.connection)
            except sqlite3.Error as e:
                if(log_error):
                    logging.error('Error in Reading Position_Size !!!',exc_info=True)
                    log_error = 0

    def saveAllData(self,signal_address=SIGNAL_CODES_ADDRESS,trade_address=TRADE_CODES_ADDRESS,open_trades=OPEN_TRADES_ADDRESS,close_trades=CLOSE_TRADES_ADDRESS,risk_manager=RISK_MANAGER_ADDRESS):
        with open(signal_address,'w+') as f:
            f.write(',tf,number\n'\
                    f'0,"5m",{self.readCodesData("5m")}\n'\
                    f'1,"1h",{self.readCodesData("1h")}\n'\
                    f'2,"4h",{self.readCodesData("4h")}\n'\
                    f'3,"1d",{self.readCodesData("1d")}\n')
            f.close()
        with open(trade_address,'w+') as f:
            f.write(',tf,number\n'\
                    f'0,"all",{self.readCodesData("all",False)}\n')
            f.close()
        open_trades_df = pd.DataFrame(columns=OPEN_TRADES_COLS)
        close_trades_df = pd.DataFrame(columns=CLOSE_TRADES_COLS)
        risk_manager_df = pd.DataFrame(columns=RISK_MANAGER_COLS)
        for symbol in COINS_SYMBOL:
            for tf in TIMEFRAMES:
                open_trades_data = self.readOpenTradesData(tf,symbol)
                for data in open_trades_data:
                    open_append = pd.Series(data, index = OPEN_TRADES_COLS)
                    open_trades_df = open_trades_df.append(open_append,ignore_index=True)
                close_trades_data = self.readCloseTradesData(tf,symbol)
                for data in close_trades_data:
                    close_append = pd.Series(data, index = CLOSE_TRADES_COLS)
                    close_trades_df = close_trades_df.append(close_append,ignore_index=True)
                risk_manager_data = self.readRiskManager(symbol,tf)
                for data in risk_manager_data:
                    trade_append = pd.Series(data, index = RISK_MANAGER_COLS)
                    risk_manager_df = risk_manager_df.append(trade_append,ignore_index=True)
        open_trades_df.to_csv(open_trades)
        close_trades_df.to_csv(close_trades)
        risk_manager_df.to_csv(risk_manager)

    def importData(self,symbols,signal_address=SIGNAL_CODES_ADDRESS,trade_address=TRADE_CODES_ADDRESS,open_trades_address=OPEN_TRADES_ADDRESS,close_trades_address=CLOSE_TRADES_ADDRESS,risk_manager_address=RISK_MANAGER_ADDRESS):
        signals = pd.read_csv(signal_address)
        trades = pd.read_csv(trade_address)
        for coin in symbols:
            self.addData(CLOUD_TABLE,[coin,0,0,0,0])
        self.addData(SIGNAL_CODES_TABLE,['5m',signals['number'].iloc[0]])
        self.addData(SIGNAL_CODES_TABLE,['1h',signals['number'].iloc[1]])
        self.addData(SIGNAL_CODES_TABLE,['4h',signals['number'].iloc[2]])
        self.addData(SIGNAL_CODES_TABLE,['1d',signals['number'].iloc[3]])
        self.addData(TRADE_CODES_TABLE,['all',trades['number'].iloc[0]])
        open_trades = pd.read_csv(open_trades_address)
        close_trades = pd.read_csv(close_trades_address)
        risk_manager = pd.read_csv(risk_manager_address)
        for data in open_trades.values.tolist():
            self.addData(OPEN_TRADES_TABLE,data[1:])
        for data in close_trades.values.tolist():
            self.addData(CLOSE_TRADES_TABLE,data[1:])
        for data in risk_manager.values.tolist():
            self.addData(RISK_MANAGER_TABLE,data[1:])

    def run(self,symbols=COINS_SYMBOL):
        cursor = self.connection.cursor()
        cursor.execute(f''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{CLOUD_TABLE}' ''')
        if cursor.fetchone()[0]==1 :
            return

        self.createTable(CLOUD_TABLE, ['symbol','tf_5m','tf_1h','tf_4h','tf_1d'])
        self.createTable(SIGNAL_CODES_TABLE, ['tf','number'])
        self.createTable(TRADE_CODES_TABLE, ['tf','number'])
        self.createTable(OPEN_TRADES_TABLE, OPEN_TRADES_COLS)
        self.createTable(CLOSE_TRADES_TABLE, CLOSE_TRADES_COLS)
        self.createTable(RISK_MANAGER_TABLE, RISK_MANAGER_COLS)
        # self.addData(RISK_MANAGER_TABLE,['SOLUSDT','5m','0.02','0.04','0.02','0.01',10,0.05])
        self.importData(symbols)
        # self.saveAllData('./New-signal_codes.csv','./New-trade_codes.csv')

if __name__ == "__main__":
    try:
        db = Database('./database.db')
        if(len(sys.argv) < 2):
            db.run(COINS_SYMBOL)
        elif (sys.argv[1] == 'save'):
            db.saveAllData()
    except Exception as error:
        print(error)


# db = Database('./database.db')
# db.run()
# db.importData(COINS_SYMBOL)
# a = db.readTP('SOLUSDT','5m')
# print(list(a.iloc[0]))
# db.addData(RISK_MANAGER_TABLE,['BTCUSDT','1h','0.02','0.04','0.02','0.01',5,0.05])
# db.addData(OPEN_TRADES_TABLE,[2,'1-1-1','BTCUSDT','1h','Long (Buy)',56000,55000,57000,56500,58000,5,1000,'TP1'])
# db.modifyCloseTrade(1,None,{'Close_Date' : '2-2-2','Open_Price':500})
# db.deleteOpenTrade(1)
# db.saveAllData()
# db.createTable(CLOUD_TABLE, ['symbol','tf_5m','tf_1h','tf_4h','tf_1d'])
# db.createTable(SIGNAL_CODES_TABLE, ['tf','number'])
# db.createTable(TRADE_CODES_TABLE, ['tf','number'])
# db.createTable(OPEN_TRADES_TABLE, OPEN_TRADES_COLS)
# db.createTable(CLOSE_TRADES_TABLE, CLOSE_TRADES_COLS)
# db.importData(COINS_SYMBOL)
# db.modifyCloudData('5m','+1','BTCUSDT')
# db.modifyCodeNumber('1h',200)
# db.readCloudData('5m','BTCUSDT')
# a = db.readCodesData('1h')
# db.modifyCodeNumber('1h',a+1)
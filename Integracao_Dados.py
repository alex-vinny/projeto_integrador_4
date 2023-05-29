# -*- coding: utf-8 -*-

# -- GetData --

"""  Importação das bibliotecas necessárias """
import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
import numpy as np
import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base
import os
from datetime import date
from datetime import datetime
from datetime import timedelta
from urllib.parse import quote_plus

def get_connection_string(debug=False):
    str_conn = os.environ["sql_connection"].format(
        os.environ["db_uid"], 
        quote_plus(os.environ["db_pwd"]), 
        os.environ["db_server"], 
        os.environ["db_name"])
    
    if(debug):
        print(str_conn)

    return  str_conn

get_connection_string()

""" Retornar a conexão com o banco de dados """
def get_session():
    str_conn = get_connection_string()

    engine = db.create_engine(str_conn)
    Session = db.orm.sessionmaker(bind=engine)

    session = Session()
    return session

''' Declaração das classes '''
Base = declarative_base()

class Stock(Base):
    """ Representação da classe stocks (ações) """
    __tablename__ = 'stocks'

    id = db.Column("id", db.BIGINT, primary_key=True)
    code = db.Column('code', db.VARCHAR(20))
    description = db.Column('description', db.VARCHAR(200))
    last_integration = db.Column('last_integration', db.DATETIME)
    
    def __str__(self):
        return "Id: {0}\tCode: {1}\tLast Integration: {2}" \
    .format(self.id, self.code, self.last_integration)
    
    def __repr__(self):
        return str(self)

class StockData(Base):
    """ Representação da classe dos dados das ações por data (diariamente) """
    __tablename__ = 'stock_data'

    id = db.Column("id", db.BIGINT, primary_key=True)
    stock_id = db.Column("stock_id",db.BIGINT , db.ForeignKey("stocks.id"))
    stock = db.orm.relationship("Stock", backref="stock_data")
    date = db.Column("stock_date", db.DATE, nullable=False)
    open = db.Column('open_value', db.FLOAT)
    high = db.Column('high_value', db.FLOAT)
    low = db.Column('low_value', db.FLOAT)
    close = db.Column('close_value', db.FLOAT)
    adj = db.Column('adjclose_value', db.FLOAT)
    volume = db.Column('volume_value', db.FLOAT)

    def __init__(self, stock, period, data=None, index=None):
        self.stock_id=stock.id
        self.date=period
        if (data is not None):
            self.open=data['Open'][index]
            self.high=data['High'][index]
            self.low=data['Low'][index]
            self.close=data['Close'][index]
            self.adj=data['Adj Close'][index]
            self.volume=data['Volume'][index]

        self.open = 0 if self.open is None else self.open
        self.high = 0 if self.high is None else self.open
        self.low = 0 if self.low is None else self.open
        self.close = 0 if self.close is None else self.open
        self.adj = 0 if self.adj is None else self.open
        self.volume = 0 if self.volume is None else self.open

    def __str__(self):
        return "Id: {0}, Date: {1}, Open: {2},\tHigh:{3},\t<<Stock: {4}>>" \
    .format(self.id, self.date, self.open, self.high, self.stock)
    
    def __repr__(self):
        return str(self)

class StockCalculation(Base):
    """ Representação da classe stock_calculation (cálculo) """
    __tablename__ = 'stock_calculation'

    id = db.Column("id", db.BIGINT, primary_key=True, autoincrement=True)
    stock_data_id = db.Column("stock_data_id",db.BIGINT , db.ForeignKey("stock_data.id"))
    stockdata = db.orm.relationship("StockData", backref="stock_calculation")
    date = db.Column("stock_date", db.DATE, nullable=False)
    process_date = db.Column('process_date', db.DATETIME)
    results = db.Column('results', db.FLOAT)
    positive = db.Column('positive', db.FLOAT)
    negative = db.Column('negative', db.FLOAT)
    positive_mean = db.Column('positive_mean', db.FLOAT)
    negative_mean = db.Column('negative_mean', db.FLOAT)
    rsi = db.Column('rsi', db.FLOAT)
    opportunity = db.Column('opportunity', db.BOOLEAN)
    to_buy = db.Column('to_buy', db.BOOLEAN)
    to_sell = db.Column('to_sell', db.BOOLEAN)

    @staticmethod
    def create(stockdata_id, period):
        cls = StockCalculation()
        cls.stock_data_id=stockdata_id
        cls.date=period
        cls.process_date=date.today()
        cls.results=0
        cls.positive=0
        cls.negative=0
        cls.positive_mean=0
        cls.negative_mean=0
        cls.rsi=0
        cls.opportunity=False
        cls.to_buy=False
        cls.to_sell=False
        return  cls

    def __str__(self):
        return "Id: {0}, Stock Data: {1}, Date: {2}, Should Buy / Sell: {3}/{4}" \
    .format(self.id, self.stock_data_id, self.date, self.to_buy, self.to_sell)
    
    def __repr__(self):
        return str(self)

""" Recuperar a lista de ações """
def get_stocks(session, debug=False):
    stocks = session.query(Stock).all()

    if(debug):
        print(stocks)

    return stocks

get_stocks(get_session())

""" Retorna conversão do DataFrame para lista de objeto """
def convert_to_model(stock, data):
    stockdata = []
    #columns = data.columns.tolist()

    for i in range(0, len(data)):
        #values = dict(zip(columns, data.values[i]))
        period = data.index[i].date()
        item = StockData(stock=stock, period=period, data=data, index=i)
        stockdata.append(item)

    return stockdata

""" Efetua o sincronismo via integração com API do Yahoo. Este método deverá ser agendado. """
def sync_from_yahoo():
    print('Start')
    with get_session() as session:
        stocks = get_stocks(session)
        # Sempre gravar a integração do dia anterior
        yesterday = date.today() - timedelta(days = 1)

        for stock in stocks:
            print('Running {0}'.format(stock))
            if stock.last_integration.date() < yesterday:
                data = yf.download(stock.code, start=stock.last_integration, end=yesterday, interval='1d')
                try:
                    result = session.bulk_save_objects(convert_to_model(stock, data))
                    stock.last_integration = yesterday                    
                except Exception as e:
                    print('Erro ao efetuar a gravação dos dados para {0} em {1}'.format(stock.code, yesterday))
                    print('Exception: {0}'.format(e))
            else:
                print('Nenhum dado para integração para {0} em {1}.'.format(stock.code, yesterday))
        
        session.commit()
        print('Done')

sync_from_yahoo()

# -- Calculation --

pd.options.mode.chained_assignment = None

""" Filtrar intervalo para cálculo dos dados das ações """
def get_data(stock_id, init_date, end_date):
    str_conn = get_connection_string()
    engine = db.create_engine(str_conn)
    query = """\
SET NOCOUNT ON;
EXEC usp_get_stockdata @stock_id = :stock_id, @initDate = :initDate, @endDate = :endDate;
"""
    data = []
    s = Stock()
    s.id = stock_id

    with engine.begin() as conn:
        result = conn.execute(
            db.text(query), 
            dict(stock_id=stock_id, initDate=init_date,endDate=end_date))
        for row in result:
            stock = StockData(s, row[2]) #['stock_date'])
            stock.id = row[0] #['id']            
            stock.open = row[3]#['open_value']
            stock.high = row[4]#['high_value']
            stock.low = row[5]#['low_value']
            stock.close = row[6]#['close_value']
            stock.adj = row[7]#['adjclose_value']
            stock.volume = row[8]#['volume_value']
            data.append(stock)
    return  data

# Example call
get_data(20, date(2000, 1, 1), date(2000, 3, 1))

""" Recuperar a lista de ações como DataFrame """
def get_stockdata(session, params, debug=False):
    stocks_data = get_data(params['stock_id'], 
        params['data_init'],
        params['data_end'])

    dates = []
    dict={
        'id': list(), 'open': list(), 
        'high': list(), 'close': list(), 
        'adj': list(), 'volume': list()
    }
    for n in stocks_data:
        dates.append(n.date)
        dict['id'].append(n.id)
        dict['open'].append(n.open)
        dict['high'].append(n.high)
        dict['close'].append(n.close)
        dict['adj'].append(n.adj)
        dict['volume'].append(n.volume)

    columns = ['id', 'open', 'high', 'close', 'adj', 'volume']
    #print(dict, columns)
    df = pd.DataFrame(dict, columns=columns, index=dates)
    return df

""" Calcular datas de venda / compra """
def calculate(df):
    if len(df) == 0:
        return df

    df['retornos'] = df['adj'].pct_change().dropna()
    
    df['positivos'] = df['retornos'].apply(lambda x: x if x > 0 else 0)
    df['negativos'] = df['retornos'].apply(lambda x: abs(x) if x < 0 else 0)

    df['media_positivos'] = df['positivos'].rolling(window=22).mean()
    df['media_negativos'] = df['negativos'].rolling(window=22).mean()

    df = df.dropna()

    df['rsi'] = (100 - 100 / (1 + df['media_positivos'] / df['media_negativos']))

    if len(df) == 0:
        return df
    
    df.loc[df['rsi'] < 30, 'oportunidade'] = True
    df.loc[df['rsi'] > 30, 'oportunidade'] = False

    df['comprar'] = False
    df['vender'] = False

    datas_comprar = []
    datas_vender = []

    for i in range(len(df)):
        if df['oportunidade'].iloc[i]:
            if i+1 < len(df):
                datas_comprar.append(df.iloc[i+1].name)                 # porque se deve comprar no preço de abertura do dia anterior

            for j in range(1, 11):                                      # 10 dias de operação
                if i+j+1 < len(df):
                    if df['rsi'].iloc[i+j] > 40:                        # vender se RSI maior de 40
                        datas_vender.append(df.iloc[i + j + 1].name)    # vender no dia seguinte ao atigir mais que 40
                        break
                    elif j == 10:
                        datas_vender.append(df.iloc[i + j + 1].name)

    df.loc[datas_comprar, 'comprar'] = True
    df.loc[datas_vender, 'vender'] = True

    df = df.dropna()

    return df

""" Retorna conversão do DataFrame para lista de objeto """
def convert_calculated_to_model(data):
    calculations = []

    for i in range(0, len(data)):
        item = StockCalculation()
        item.stock_data_id = int(data['id'][i])
        item.date = data.index[i].date()
        item.process_date = date.today()
        item.results=float(data['retornos'][i])
        item.positive=float(data['positivos'][i])
        item.negative=float(data['negativos'][i])
        item.positive_mean=float(data['media_positivos'][i])
        item.negative_mean=float(data['media_negativos'][i])
        item.rsi=float(data['rsi'][i])
        item.opportunity=bool(data['oportunidade'][i])
        item.to_buy=bool(data['comprar'][i])
        item.to_sell=bool(data['vender'][i])
        calculations.append(item)

    return calculations

# Example call
convert_calculated_to_model(
    calculate(
        get_stockdata(get_session(), 
                      dict(stock_id=20, 
                           data_init=date(2000, 1, 1), 
                           data_end=date(2000, 5, 1)))))

""" Persistir o calculado inserindo ou atualizando a informação """
def get_or_add_calculation(calculated, debug=False):
    if debug: print('Starting')
    with get_session() as session:
        for c in calculated:
            query = session.query(StockCalculation).filter(StockCalculation.stock_data_id==int(c.stock_data_id))

            if query.first() is None:           # Inserindo              
                session.add(c)
                if debug: print('Adding', c)
            else:                               # Atualizando
                u = query.first()
                u.process_date = date.today()
                u.results=c.results
                u.positive=c.positive
                u.negative=c.negative
                u.positive_mean=c.positive_mean
                u.negative_mean=c.negative_mean
                u.rsi=c.rsi
                u.opportunity=c.opportunity
                u.to_buy=c.to_buy
                u.to_sell=c.to_sell
                if debug: print('Updating', u)
            session.flush()           
        session.commit()
    if debug: print('Finished')

# Example call
get_or_add_calculation(
    convert_calculated_to_model(
        calculate(
            get_stockdata(get_session(), dict(stock_id=20, data_init=date(2000, 1, 1), data_end=date(2000, 5, 1))))), True)

""" Função auxiliar para geração de meses seqüênciais em formato de iterator """
def month_year_iter(start_month, start_year, end_month, end_year ):
    ym_start= 12*start_year + start_month - 1
    ym_end= 12*end_year + end_month - 1
    for ym in range( ym_start, ym_end ):
        y, m = divmod( ym, 12 )
        if m+1==1:
            yield y, m+1
        else:
            continue

""" Função principal que executa o cálculo """
def process_calculation(init:date, end:date):
    print('Starting')
    stocks = get_stocks(get_session())
    for stock in stocks:
        print(stock.code)        
        #if init.year == datetime.now().year and month > datetime.now().month:
        #    break
        item=dict(stock_id=stock.id, data_init=init, data_end=end)

        get_or_add_calculation(
            convert_calculated_to_model(
                calculate(
                    get_stockdata(get_session(), item))))
    print('Finished')

#[m for m in month_year_iter(1, 2000, 12, 2023)]
process_calculation(date(2023, 5, 1), date(2023, 6, 1))


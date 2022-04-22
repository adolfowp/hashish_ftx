from datetime import datetime
from numpy import integer
import pandas as pd
import ccxt
import time
from os.path import exists
import database as db
import csv

from config import FTX_API_KEY, FTX_SECRET

from bots_common import fetch_bars, get_perpetual_markets, crossed_above, crossed_below, get_price, get_balance_in_usd
from ta_functions import heikin_ashi, populate_ema

blacklisted_pairs = []
if exists('blacklist.csv'):
    with open('blacklist.csv', 'r') as read_obj:
        csv_reader = csv.reader(read_obj)
        blacklisted_pairs = [item[0] for item in list(csv_reader)]

MAX_OPEN_TRADES = 5
RISK = 0.01

exchange = ccxt.ftx({
    'apiKey' : FTX_API_KEY,
    'secret' : FTX_SECRET,
    'enableRateLimit' : True,
})

def calculate_amount(open_price, stoploss, balance, risk=0.1):
    pips = abs(stoploss-open_price)
    atl = balance*risk
    return atl/pips

def is_position_open(symbol):
    positions = exchange.fetch_positions()
    ps = [p for p in positions if (float(p['info']['size']) > 0 and p['info']['future'] == symbol)]
    if len(ps) > 0:
        return True
    
    return False

def count_open_positions():
    positions = exchange.fetch_positions()
    ps = [p for p in positions if float(p['info']['size']) > 0]
    
    return len(ps)

def close_phantom_trades():
    for trade in db.get_open_trades():
        if not is_position_open(trade[0]):
            db.close_trade(trade[0], trade[3])

def trade_logic(df: ccxt.Exchange, symbol):
    
    # df['raw_buy']=False
    df.loc[
        (
            ((df['crossed_above']) | # Controllo se la chiusura è maggiore dell'apertura
            (df['uptrend'] & # Se è in uptrend
            (df['low'].shift(1) <= df['ema20']) & 
            ((df['body'] == 1) & 
            (df['no_lower_wick']) & 
            (df['volume'] > df['volume'].shift(1)) &
            ((df['body'].shift(1) == -1) | 
            (df['body'].shift(2) == -1)))))
        ),
        'buy'] = 1
    
    # df['buy'] = df['raw_buy'] & (df['raw_buy'].shift(1) == False)
    
    df.loc[
        (
            ((df['crossed_below']) | # Controllo se la chiusura è maggiore dell'apertura
            (df['uptrend'] & # Se è in uptrend
            ((df['body'] == -1) & 
            (df['no_upper_wick']))))
        ),
        'close_buy'] = 1
    
    # buy = 1 if df['buy'][-1] else 0
    
    return (df['buy'][-1], df['close_buy'][-1], symbol, df['stoploss'][-1])

def run(symbol):
    bars = fetch_bars(exchange, symbol, tf='1d')
    if len(bars) > 100:
        df = heikin_ashi(bars[:-1])
        populate_ema(df, 10)
        populate_ema(df, 20)
        
        df['crossed_above'] = crossed_above(df['ema10'], df['ema20'])
        df['crossed_below'] = crossed_below(df['ema10'], df['ema20'])
        
        df['no_lower_wick'] = ((df['low'] == df['open']) | (df['low'] == df['close']))
        df['uptrend'] = df['ema10'] > df['ema20']
        df['no_upper_wick'] = ((df['high'] == df['open']) | (df['high'] == df['close']))
        
        df['stoploss'] = df['low'].rolling(5).min()
        
        df.loc[
            (
                (df['close'] > df['open']) # Controllo se la chiusura è maggiore dell'apertura
            ),
            'body'] = 1
        
        df.loc[
            (
                (df['close'] <= df['open']) # Controllo se la chiusura è maggiore dell'apertura
            ),
            'body'] = -1
            
        print(df)
        trade_info = trade_logic(df, symbol)
        
        if (trade_info[0] == 1) or (trade_info[1] == 1):
            return trade_info

def run_all(balance):
    markets = get_perpetual_markets(exchange)
    
    # controllo se qualche ordine ha preso stop
    if db.count_open_trades() > count_open_positions():
        close_phantom_trades()
        
    results = []
    
    for symbol in [f for f in markets if f not in blacklisted_pairs]:
        
        results.append(run(symbol))
        
    close_result = [r for r in results if r != None and r[1] == 1]
    buy_result = [r for r in results if r != None and r[0] == 1]
    
    # chiudo posizioni
    for result in close_result:
        trade = db.get_trade(result[2])
        if trade != None:
            if is_position_open(symbol):
                trade = db.get_trade(symbol)
                exchange.create_market_sell_order(symbol, trade[7], params={'reduceOnly': True})
                exchange.cancel_order(str(trade[6]), symbol, params={'type': 'stop'})
            db.close_trade(symbol, price)
    
    for result in buy_result:
        stoploss = result[3]
        
        # calculate amount
        price = get_price(exchange, result[2])
        
        if (db.count_open_trades() < MAX_OPEN_TRADES) and (db.get_trade(symbol) == None):
        
            # calculate amount
            amount = calculate_amount(price, stoploss, balance, RISK)
            try:
                exchange.create_market_buy_order(symbol, amount)
                sl_order = exchange.create_order(symbol, 'stop', 'sell', amount, price=stoploss, params={'stopPrice':stoploss, 'reduceOnly': True})
                
                #add to db
                db.create_trade(symbol=symbol, timestamp=datetime.now(), open_price=price, stoploss=stoploss, sl_order=sl_order['id'], amount=amount)
            except Exception as e:
                print(e)
        
        
    # if result != None:
    #     # symbol = result[2]
    #     buy = True if result[0] == 1 else False
    #     close_buy = True if result[1] == 1 else False
    #     stoploss = result[3]
        
    #     # calculate amount
    #     price = get_price(exchange, symbol)
        
    #     if (buy and (db.count_open_trades() < MAX_OPEN_TRADES) and (db.get_trade(symbol) == None)):
            
    #         # calculate amount
    #         amount = calculate_amount(price, stoploss, balance, RISK)
    #         try:
    #             exchange.create_market_buy_order(symbol, amount)
    #             sl_order = exchange.create_order(symbol, 'stop', 'sell', amount, price=stoploss, params={'stopPrice':stoploss, 'reduceOnly': True})
                
    #             #add to db
    #             db.create_trade(symbol=symbol, timestamp=datetime.now(), open_price=price, stoploss=stoploss, sl_order=sl_order['id'], amount=amount)
    #         except Exception as e:
    #             print(e)
            
    #     elif close_buy:
    #         trade = db.get_trade(symbol)
    #         if trade != None:
    #             if is_position_open(symbol):
    #                 trade = db.get_trade(symbol)
    #                 exchange.create_market_sell_order(symbol, trade[7], params={'reduceOnly': True})
    #                 exchange.cancel_order(str(trade[6]), symbol, params={'type': 'stop'})
    #             db.close_trade(symbol, price)
                
                

if __name__ == "__main__":
    if not exists('trades.db'):
        db.create_sqlite_database()
        
    run_all(get_balance_in_usd(exchange))
    exit()
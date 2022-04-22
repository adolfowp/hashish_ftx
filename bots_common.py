import ccxt
import time
import pandas as pd

def crossed_above(serie1: pd.Series, serie2: pd.Series):
    current = serie1 > serie2
    
    previus = current.shift(1)
    trigger = (current != previus) & (current == True)
    return trigger

def crossed_below(serie1: pd.Series, serie2: pd.Series):
    current = serie1 < serie2
    
    previus = current.shift(1)
    trigger = (current != previus) & (current == True)
    return trigger

def get_price(exchange: ccxt.Exchange, symbol: str) -> float:
    ticker =  exchange.fetch_ticker(symbol)['last']
    return ticker

# Import bars function
def fetch_bars(exchange, symbol, tf='1m', limit=300):

    while True:
        try:
        # recupero le info sulle barre
            bars = exchange.fetch_ohlcv(symbol, limit=limit, timeframe=tf)
            
        except ccxt.RequestTimeout:
            time.sleep(10)
            continue
        
        except ccxt.NetworkError:
            time.sleep(10)
            continue
        
        
        break

    # inizializzo dataframe con panda
    df = pd.DataFrame(bars, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.index = pd.DatetimeIndex(df['timestamp'])

    return df

def get_perpetual_markets(exchange: ccxt.Exchange):
    exchange.load_markets()
    markets = list()
    whole_markets = exchange.markets_by_id

    for m in whole_markets:
        if m.endswith('PERP'):
            markets.append((m, float(whole_markets[m]['info']['volumeUsd24h'])))
            
    markets.sort(key=lambda y: y[1], reverse=True)
        
    return [m[0] for m in markets]

def get_balance_in_usd(exchange: ccxt.Exchange):
    
    balance_usd = 0
    balances = exchange.fetch_total_balance()
    
    for b in balances:
        if balances[b] != 0:
            if b == 'USD' or b == 'USDT':
                balance_usd += balances[b]
                
            else:
                balance_usd += balances[b] * get_price(exchange, b+'-PERP')
    
    return balance_usd
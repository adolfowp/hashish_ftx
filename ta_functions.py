from numpy import NaN, float64
import pandas as pd

from ta.momentum import AwesomeOscillatorIndicator, StochRSIIndicator
from ta.trend import ADXIndicator, EMAIndicator
from ta.volatility import DonchianChannel


def populate_adx(df: pd.DataFrame, p: int, di: int):
    
    adx_indicator = ADXIndicator(df['high'], df['low'], df['close'], p)
    df['adx'] = adx_indicator.adx()
    df['+di'] = adx_indicator.adx_pos()
    df['-di'] = adx_indicator.adx_neg()
    
    return df

def populate_ao(df: pd.DataFrame):
    
    ao_indicator = AwesomeOscillatorIndicator(df['high'], df['low'])
    df['ao'] = ao_indicator.awesome_oscillator()
    
    return df

def populate_stochastic(df: pd.DataFrame, p: int = 14, s1: int = 7, s2: int = 3):
    
    stoch = StochRSIIndicator(df['close'], window=p, smooth1=s1, smooth2=s2)
    df['%D'] = stoch.stochrsi_d()
    df['%K'] = stoch.stochrsi_k()
    
    return df

def populate_ema(df: pd.DataFrame, p: int = 21):
    
    df[f'ema{p}'] = EMAIndicator(df['close'], p).ema_indicator()
    return df

def populate_mcginley(df: pd.DataFrame, p: int = 14):
    
    prev = df['close'].shift(1)
    
    ema_indicator = EMAIndicator(df['close'], p)
    df[f'ema{p}'] = ema_indicator.ema_indicator()
    
    df[f'mg{p}'] = df['close'].copy()
    
    for i in range(1, len(df.index)):
        
        if df[f'mg{p}'][i-1] is NaN:
            
            df[f'mg{p}'][i] = df[f'ema{p}'][i]
            
        else:
            
            df[f'mg{p}'][i] = df[f'mg{p}'][i-1] + (df['close'][i] - df[f'mg{p}'][i-1]) / (p * (df['close'][i]/df[f'mg{p}'][i-1])**4)
            
    return df

def populate_donchain(df: pd.DataFrame, p: int):
    
    dc = DonchianChannel(df['high'], df['low'], df['close'], p)
    
    df['upper'] = dc.donchian_channel_hband()
    df['lower'] = dc.donchian_channel_lband()
        
    return df
    
def heikin_ashi(df: pd.DataFrame):
    heikin_ashi_df = pd.DataFrame(index=df.index.values, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    
    heikin_ashi_df['close'] = (df['open'] + df['high'] + df['low'] + df['close']) / 4
    heikin_ashi_df['timestamp'] = df['timestamp']
    heikin_ashi_df['volume'] = df['volume']
    
    for i in range(len(df)):
        if i == 0:
            heikin_ashi_df.iat[0, 1] = df['open'].iloc[0]
        else:
            heikin_ashi_df.iat[i, 1] = (heikin_ashi_df.iat[i-1, 1] + heikin_ashi_df.iat[i-1, 4]) / 2
        
    heikin_ashi_df['high'] = heikin_ashi_df.loc[:, ['open', 'close']].join(df['high']).max(axis=1)
    
    heikin_ashi_df['low'] = heikin_ashi_df.loc[:, ['open', 'close']].join(df['low']).min(axis=1)
    
    heikin_ashi_df['open'] = heikin_ashi_df['open'].astype(float64)
    
    return heikin_ashi_df
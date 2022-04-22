import sqlite3

def create_sqlite_database():
    conn = sqlite3.connect('trades.db') #Opens Connection to SQLite database file.
    conn.execute('''CREATE TABLE trades
                (
                    SYMBOL      TEXT NOT NULL,
                    TIMESTAMP   BLOB NOT NULL,
                    OPEN_PRICE  REAL,
                    STOPLOSS    REAL,
                    CLOSE_PRICE REAL,
                    IS_OPEN     INTEGER NOT NULL,
                    SL_ORDER    INTEGER,
                    AMOUNT      REAL NOT NULL
                );''') #Creates the table
    conn.commit() # Commits the entries to the database
    conn.close()
    
def create_trade(symbol, timestamp, amount, open_price, stoploss, sl_order):
    conn = sqlite3.connect('trades.db')
    cursor = conn.cursor()
    params = (symbol, timestamp, open_price, stoploss, sl_order, amount)
    cursor.execute("INSERT INTO trades VALUES (?,?,?,?, NULL, 1, ?, ?)",params)
    conn.commit()
    print('Trade Creation Successful')
    conn.close()
    
def get_trade(symbol):
    conn = sqlite3.connect('trades.db')
    cur = conn.cursor()
    cur.execute("SELECT * FROM trades WHERE SYMBOL=:symbol AND IS_OPEN=1", {'symbol' : symbol})
    trade = cur.fetchone()
    conn.close()
    return trade
    
def count_open_trades():
    ot = len(get_open_trades())
    return ot

def close_trade(symbol, price):
    conn = sqlite3.connect('trades.db')
    cur = conn.cursor()
    
    cur.execute("SELECT * FROM trades WHERE SYMBOL=:symbol AND IS_OPEN=1", {'symbol' : symbol})
    record = cur.fetchone()
    if record == None:
        print('No record found')
        return
    
    cur.execute("""UPDATE trades SET CLOSE_PRICE = :CLOSE_PRICE, IS_OPEN = 0 WHERE SYMBOL =:SYMBOL """,{'CLOSE_PRICE':price,'SYMBOL':symbol})
    
    print("Update Successful")
    conn.commit()
    conn.close()
    
def get_sl_order_id(symbol):
    conn = sqlite3.connect('trades.db')
    cur = conn.cursor()
    cur.execute("SELECT SL_ORDER FROM trades WHERE IS_OPEN=1 AND SYMBOL=:SYMBOL", {'SYMBOL':symbol})
    r = cur.fetchone()
    conn.close()
    if r != None:
        return r[0]
    
def get_open_trades():
    conn = sqlite3.connect('trades.db')
    cur = conn.cursor()
    cur.execute("SELECT * FROM trades WHERE IS_OPEN=1")
    open_trades = cur.fetchall()
    conn.close()
    return open_trades

def __data_update(symbol):
    conn = sqlite3.connect('trades.db')
    cur = conn.cursor()
    
    # cur.execute("""UPDATE trades SET PASSWORD = :PASSWORD WHERE NAME =:NAME """,{'PASSWORD':password,'NAME':username})
    print("Update Successful")
    conn.commit()
    conn.close()
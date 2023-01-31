#!/usr/bin/env python
# coding: utf-8

# In[1]:


from datetime import datetime
import json
import socketio
from Connect import XTSConnect
from MarketDataSocketClient import MDSocket_io
from Connect import XTSCommon
import pandas as pd
import threading
import sqlite3


# In[2]:


# conn = sqlite3.connect('ticks.db') 
# c = conn.cursor()
# c.execute('''CREATE TABLE IF NOT EXISTS market_data_xts (timestamp TEXT, instrument_id TEXT, bid REAL, ask REAL, ltp REAL)''')  


# In[ ]:


#composite
API_KEY = "xxxxx"
API_SECRET = "xxxxx"
source = "WEBAPI"


# Initialise
xt = XTSConnect(API_KEY, API_SECRET, source,"","")
response = xt.marketdata_login()
print("Login: ", response)

set_marketDataToken =  response['result']['token']


set_muserID = response['result']['userID']
# Connecting to Marketdata socket
soc = MDSocket_io(set_marketDataToken, set_muserID)



# Instruments for subscribing
Instruments = [
              
                {'exchangeSegment': 1, 'exchangeInstrumentID':  18011},
    {'exchangeSegment': 2, 'exchangeInstrumentID': 125431}
             
               ]

# Callback for connection
def on_connect():
    """Connect from the socket."""
    print('Market Data Socket connected successfully!')

    # Subscribe to instruments
    print('Sending subscription request for Instruments - \n' + str(Instruments))
    response = xt.send_subscription(Instruments, 1501)
    print(datetime.now(),'  Sent Subscription request!')
    
on_connect()

# Callback on receiving message
def on_message(data):
    print('I received a message!')
    
    
# Callback for message code 1501 FULL
def on_message1501_json_full(data):
    print('I received a 1501 Touchline message!' + json.dumps(data))  # Convert dictionary to string before concatenating
    ms = json.loads(data)
    timestamp = ms["Touchline"]["LastTradedTime"]
    print(f"Last_Traded_Time: {timestamp}")
    instrument_id = ms["ExchangeInstrumentID"]
    print(f"INST_ID: {instrument_id}")
    volume = ms["Touchline"]["TotalTradedQuantity"]
    print(f"VOLUME:{volume}")
    bid =ms["Touchline"]["BidInfo"]["Price"]
    print(f"BID: {bid}")
    ask = ms["Touchline"]["AskInfo"]["Price"]
    print(f"ASK: {ask}")
    ltp = (bid + ask)/2
    print(f"Calculated ltp: {ltp}")
    df = pd.DataFrame({'Timestamp': [timestamp],
                       'Instrument ID': [instrument_id],
                       'Bid': [bid],
                       'Ask': [ask],
                      'LTP' : [ltp]})
    df
    conn = sqlite3.connect('xts_test.db') 
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS xts_live_tick_data (timestamp TEXT,instrument_id TEXT,volume REAL,bid REAL,ask REAL,ltp REAL,current_time TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S', 'now')))''')
    c.execute("INSERT INTO xts_live_tick_data (timestamp, instrument_id, volume ,bid, ask, ltp, current_time) VALUES (?,?,?,?,?,?, strftime('%Y-%m-%d %H:%M:%S', 'now'))", (timestamp, instrument_id, volume ,bid, ask, ltp))
    conn.commit()

    
    
def on_message1502_json_full(data):
    print('I received a 1502 Market depth message!' + data)


# Callback for message code 1501 PARTIAL
def on_message1501_json_partial(data):
    print('I received a 1501, Touchline Event message!' + data)

# Callback for message code 1502 PARTIAL
def on_message1502_json_partial(data):
    print('I received a 1502 Market depth message!' + data)


# Callback for message code 1502 PARTIAL
def on_message1505_json_partial(data):
    print(datetime.now() ,'I received a 1505 Candle message!' + data)


# Callback for disconnection
def on_disconnect():
    print('Market Data Socket disconnected!')


# Callback for error
def on_error(data):
    """Error from the socket."""
    print('Market Data Error', data)


# Assign the callbacks.
soc.on_connect = on_connect
soc.on_message = on_message
soc.on_message1502_json_full = on_message1502_json_full

soc.on_message1501_json_full = on_message1501_json_full
soc.on_message1502_json_partial = on_message1502_json_partial
soc.on_message1501_json_partial = on_message1501_json_partial
soc.on_message1505_json_partial = on_message1505_json_partial

soc.on_disconnect = on_disconnect
soc.on_error = on_error


# Event listener
el = soc.get_emitter()
el.on('connect', on_connect)
el.on('1501-json-full', on_message1501_json_full)
el.on('1502-json-full', on_message1502_json_full)
el.on('1505-json-partial', on_message1505_json_partial)


# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
if __name__ == "__main__":
    soc.connect() 


# In[ ]:





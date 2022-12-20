#!/usr/bin/env python
# coding: utf-8

# In[1]:


import logging
import sqlite3
from kiteconnect import KiteTicker

logging.basicConfig(level=logging.DEBUG)

# Initialize the KiteTicker
kws = KiteTicker('API_KEY', 'ACCESS_TOKEN')

# Connect to the SQLite database
db = sqlite3.connect('kite_data.db')
cur = db.cursor()

# Create a table for the ticks data
cur.execute('CREATE TABLE db_bday (instrument_token INTEGER, last_price NUMERIC)')

def on_ticks(ws, ticks):
    # Callback to receive ticks
    logging.debug("Ticks: {}".format(ticks))

    # Insert the ticks into the database
    for tick in ticks:
        cur.execute('INSERT INTO db_bday VALUES (?,?)', (tick['instrument_token'], tick['last_price']))
        db.commit()

def on_connect(ws, response):
    ws.subscribe([62285063,16078850,17600514,16079106])
    ws.set_mode(ws.MODE_LTP, [62285063,16078850,17600514,16079106])

def on_close(ws, code, reason):
    ws.stop()


kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

kws.connect()


# In[ ]:





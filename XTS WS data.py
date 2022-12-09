#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from Connect import XTSConnect
from InteractiveSocketClient import OrderSocket_io

# Interactive API Credentials
API_KEY = "f6bdb692c9a05029a09113"
API_SECRET = "Txey033@2q"
source = "WEBAPI"




"""
{
  "secretKey":"Txey033@2q",
  "appKey": "f6bdb692c9a05029a09113",
  "source": "WEBAPI"
}

"""

# Initialise
xt = XTSConnect(API_KEY, API_SECRET, source)

# Login for authorization token
response = xt.interactive_login()

# Store the token and userid
set_interactiveToken = response['result']['token']
set_iuserID = response['result']['userID']
print("Login: ", response)

# Connecting to Interactive socket
soc = OrderSocket_io(set_interactiveToken, set_iuserID)


# Callback for connection
def on_connect():
    """Connect from the socket."""
    print('Interactive socket connected successfully!')


# Callback for receiving message
def on_message():
    print('I received a message!')


# Callback for joined event
def on_joined(data):
    print('Interactive socket joined successfully!' + data)


# Callback for error
def on_error(data):
    print('Interactive socket error!' + data)


# Callback for order
def on_order(data):
    print("Order placed!" + data)


# Callback for trade
def on_trade(data):
    print("Trade Received!" + data)


# Callback for position
def on_position(data):
    print("Position Retrieved!" + data)


# Callback for trade conversion event
def on_tradeconversion(data):
    print("Trade Conversion Received!" + data)


# Callback for message logout
def on_messagelogout(data):
    print("User logged out!" + data)


# Callback for disconnection
def on_disconnect():
    print('Interactive Socket disconnected!')


# Assign the callbacks.
soc.on_connect = on_connect
soc.on_message = on_message
soc.on_joined = on_joined
soc.on_error = on_error
soc.on_order = on_order
soc.on_trade = on_trade
soc.on_position = on_position
soc.on_tradeconversion = on_tradeconversion
soc.on_messagelogout = on_messagelogout
soc.on_disconnect = on_disconnect

# Event listener
el = soc.get_emitter()
el.on('connect', on_connect)
el.on('order', on_order)
el.on('trade', on_trade)
el.on('position', on_position)
el.on('tradeConversion', on_tradeconversion)

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
soc.connect()


# In[ ]:


soc.on_connect()


# In[ ]:


import sqlite3
conn = sqlite3.connect('mydatabase.db')

cursor = conn.cursor()

cursor.execute('CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, data TEXT)')

cursor.execute('INSERT INTO orders (data) VALUES (?)', (data,))

conn.commit()

conn.close()


# In[ ]:


get_ipython().system('pip install python-engineio==3.13.0')
get_ipython().system('pip install python-socketio==4.6.0')
get_ipython().system('pip install websocket-client==0.57.0')


# In[ ]:


cursor.execute('SELECT * FROM orders')
result = cur.fetchall()
for r in result:
        print(r)


# In[ ]:





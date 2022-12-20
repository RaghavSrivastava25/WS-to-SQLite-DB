#!/usr/bin/env python
# coding: utf-8

# In[1]:


import logging
import sqlite3
import time
from kiteconnect import KiteTicker


logging.basicConfig(level=logging.DEBUG)

# Initialize the KiteTicker
kws = KiteTicker('API_KEY', 'ACCESS_TOKEN')

# Connect to the SQLite database
db = sqlite3.connect('kite_data.db')
cur = db.cursor()


# In[2]:


cur.execute('SELECT * FROM db_bday LIMIT 4')
result = cur.fetchall()


# In[3]:


for r in result:
        print(r)


# In[ ]:





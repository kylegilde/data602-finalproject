# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 07:42:55 2017

@author: niki
"""



from pymongo import MongoClient


client = MongoClient("mongodb://data602:data602@tradingcluster-shard-00-00-8ceuz.mongodb.net:27017,tradingcluster-shard-00-01-8ceuz.mongodb.net:27017,tradingcluster-shard-00-02-8ceuz.mongodb.net:27017/test?ssl=true&replicaSet=TradingCluster-shard-0&authSource=admin")


db = client.TradeData
db.blotter.drop()
blotterItem = {'Side': 'N/A',
           'Ticker': 'N/A',
           'Price': 0,
           'Quantity': 0,
           'Date': 'N/A',
           'Cash': 10000000
        }
result = db.blotter.insert_one(blotterItem)

initialPL = {'Ticker': 'N/A',
             'RPL':0.0}
db.RealPL.drop()
result = db.RealPL.insert_one(initialPL)
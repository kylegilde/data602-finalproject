# -*- coding: utf-8 -*-
"""
Created on Sat Oct  7 18:38:06 2017

@author: niki
"""

import c3pyo as c3
from flask import Flask, render_template, request
from bs4 import BeautifulSoup as bs
import requests as url
from datetime import datetime
import pandas as pd
from pymongo import MongoClient

app = Flask(__name__)
#app.config["TEMPLATES_AUTO_RELOAD"] = True

##MongoDB Setup
client = MongoClient("mongodb://data602:data602@tradingcluster-shard-00-00-8ceuz.mongodb.net:27017,tradingcluster-shard-00-01-8ceuz.mongodb.net:27017,tradingcluster-shard-00-02-8ceuz.mongodb.net:27017/test?ssl=true&replicaSet=TradingCluster-shard-0&authSource=admin")

db = client.TradeData

TickerData = pd.read_csv('http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download')


##Takes in a ticker symbol and reads the price.  If price not found it return 0.
def ReadQuote(stock):
    try:
        StockPage = url.get('https://finance.yahoo.com/quote/'+stock +'?p=' + stock)
        StockSoup = bs(StockPage.text, "html.parser")
        #print(StockSoup)
        price = StockSoup.find('span', attrs={'class':'Trsdu(0.3s) Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(b)'})
        StockPrice = price.text
        StockPrice = StockPrice.replace(',','')
    except Exception:
        return 0
    else:
        return float(StockPrice)


#This function takes a ticker symbol and scrapes the last hundred trades from Nasdaq's website.
#Page 1 is scraped and 2 only if Page 1 had 50 entries.  It should be noted that whether or 
#not the site has the relevant info for the symbol the read is a success.  However, if info for
#the symbol does not exist the time is not in the correct place.
def lastHundred(tickerValue):
    try:
        data = pd.read_html('http://www.nasdaq.com/symbol/' + tickerValue +'/time-sales')
        
    except ValueError:
        print('Error somewhere 1')
        data3 = None
    else:
        if (data[5].shape[0] == 50):
            data2 = pd.read_html('http://www.nasdaq.com/symbol/' + tickerValue 
                                 +'/time-sales?pageno=2')
    
            data3 = (data[5].append(data2[5]))
            data3 = data3.reset_index(drop=True)
        else:
            data3 = data[5]
        try:
            data3['NLS Time (ET)'] = pd.to_datetime(data3['NLS Time (ET)'])
        except (ValueError, TypeError, KeyError) as e:
            print('Error somewhere 2')
            data3 = None
        else:
            data3['NLS Price'] = data3['NLS Price'].str.replace(',', '')
            data3['NLS Price'] = data3['NLS Price'].str.replace('$', '')
            data3['NLS Price'] = data3['NLS Price'].str.strip()
        try:
            data3['NLS Price'] = pd.to_numeric(data3['NLS Price'])
        except (ValueError, TypeError) as e:
            print('Error somewhere 3')
            data3 = None
   # print(data3)    
    return data3


#function to chart a single time series
def get_timeSeries_single(x_axis, y_axis, bound):
    # data
    

#    # chart
    chart = c3.LineChart()
    chart.plot(x_axis, y_axis, label=bound)
#    chart.plot(x, y2, label='Series 2')
     
    chart.bind_to(bound)

    return chart.json()

#function to chart two time series on the same graph
def get_timeSeries_double(x_axis, y1_axis, y1_title, y2_axis, y2_title, bound):
    # data
    

#    # chart
    chart = c3.LineChart()
    chart.plot(x_axis, y1_axis, label=y1_title)
    chart.plot(x_axis, y2_axis, label=y2_title)
       
    chart.bind_to(bound)
    

    return chart.json()
  
#This function gets the data for the last 100 days of the stock symbol entered from
#Nasdaq's website.  If no data is found None value is returned      

def get_hundred_days(tickerValue):
    try:
        data = pd.read_html('http://www.nasdaq.com/symbol/' + tickerValue +'/historical')
    except Exception:
        data3 = None
    else:
        data2 = data[5]
        data3 = data2.iloc[1:,:]
        data3.columns = ['Date','Open','High','Low','Close','Volume']
        data3['Date'] = pd.to_datetime(data3['Date'])
    return data3

#gets the last record in the blotter.  Mainly for updating the new Cash value
def getLastRecord():
    
    lastOne = list(db.blotter.find().skip(db.blotter.count() - 1))
    return lastOne

#appends a transation onto the blotter
def addToBlotter(cash, date, price, qty,
                                   side, ticker):
    blotterItem = {'Side': side,
           'Ticker': ticker,
           'Price': price,
           'Quantity': qty,
           'Date': date,
           'Cash': cash
        }
    try:
        db.blotter.insert_one(blotterItem)
        return "Success"
    except Exception:
        return "Database write error"
        

#This is used to get the total stock in hand of a particular ticker symbol.  Used to
#calculate values in the P and L.
def countStock(stock):
    allStockBuys = pd.DataFrame(list(db.blotter.find({'Ticker':stock, 'Side':'buy'})))
    
    if allStockBuys.empty:
        totalStock = 0
    else:
        allStockSells = pd.DataFrame(list(db.blotter.find({'Ticker':stock, 'Side':'sell'})))
        if allStockSells.empty:
            totalStock = int(allStockBuys['Quantity'].sum())
        else:
            totalStockBuys = allStockBuys['Quantity'].sum()
            totalStockSells = allStockSells['Quantity'].sum()
            totalStock = int(totalStockBuys) - int(totalStockSells)
    
    return totalStock

#Function to calculate WAP
def calcWAP(stock):
    allStock = pd.DataFrame(list(db.blotter.find({'Ticker':stock})))
    if allStock.empty:
        WAP = 0
    else:
        allStock['Total Cost'] = allStock['Quantity'] * allStock['Price']
        totalCost = allStock['Total Cost'].sum()
        totalQuantity = allStock['Quantity'].sum()
        WAP = totalCost/totalQuantity
    return WAP

##Get stock position of symbol 
#def getPositions(stock):
#    allStock = pd.DataFrame(list(db.blotter.find({'Ticker':stock})))
#    position = allStock['Quantity'].sum()
#    return position

#To calculate share allocation
def getshareallocation(stock):
    tickeralloc = pd.DataFrame(list(db.blotter.find({'Ticker':stock})))
    tickerallocAmt = tickeralloc['Quantity'].sum()
    
    totalQuantity = pd.DataFrame(list(db.blotter.find()))
    totalQuantityAmt = totalQuantity['Quantity'].sum()
    
    allocation = tickerallocAmt/totalQuantityAmt
    return allocation






@app.route("/")
@app.route("/main")
def index():
    return render_template('index.html')


@app.route("/reset")
def reset():
    try:
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
        Message = 'Reset Successful'
    except Exception:
        Message = 'Cannot reset at this time'
    return render_template('reset.html', status = Message)
 
@app.route("/tradeStock")
def tradeStock():
    return render_template('trade.html', ticker=TickerData.iloc[:,0])
 
@app.route("/showBlotter")
def showBlotter():
    blotterData = pd.DataFrame(list(db.blotter.find()))
    return render_template('blotter.html', blotterTable = blotterData.to_html())
 
@app.route("/showPL")
def showPL():
    positions = []
    markets = []
    waps = []
    upls = []
    rpls = []
    shareallocations = []
    dollarallocations = []
    tickers = db.blotter.distinct('Ticker')
    alltickers = tickers[1:]
    for i in alltickers:
        pos = countStock(i)
        positions.append(pos)
        market = ReadQuote(i)
        markets.append(market)
        wap = calcWAP(i)
        waps.append(wap)
        upl = pos * (market - wap)
        upls.append(upl)
        rpl = pd.DataFrame(list(db.RealPL.find({'Ticker':i})))
        if rpl.empty:
            currentRPL = 0
        else:
            currentRPL = float(rpl['RPL'])
        print(str(currentRPL))
        rpls.append(currentRPL)
        shareallocations.append(getshareallocation(i))
    
    PandL = pd.DataFrame(
                {'Ticker' : alltickers,
                 'Position': positions,
                 'Market':markets,
                 'WAP': waps,
                 'UPL': upls,
                 'RPL': rpls,
                 'Allocation by Shares': shareallocations})
    
    costs = PandL['Position'] * PandL['WAP']
    totalcosts = sum(costs)
    PandL['Allocation by Dollars'] = (PandL['Position'] * PandL['WAP'])/totalcosts
        
    return render_template('pl.html', PLTable = PandL.to_html())

@app.route("/conductTrade", methods = ['POST', 'GET'])
def conductTrade():
    #Read in Form values
    stockToTrade = request.form.get('stockChosen')
#    tradeSide = request.form.get('sideChosen')
#    tradeQuantity= request.form.get('quantity')
    #Read stock price
    tradePrice = ReadQuote(stockToTrade)
    #get Last hundred sales
    tradeHistory =lastHundred(stockToTrade)
    #print(tradeHistory)
    if (tradeHistory is not None):
        hundredTradesChart = get_timeSeries_single(tradeHistory['NLS Time (ET)'], 
                                        tradeHistory['NLS Price'],'time_series_price')
        Message = "Success Reading Hundred Trades Data"
    else:
        Message = "Error with Hundred Trades Chart"
        hundredTradesChart = None
    #print(hundredChart)
    hundredDays = get_hundred_days(stockToTrade)
    
    if (hundredDays is not None):
      
        hundredDaysClose = get_timeSeries_double(hundredDays['Date'], 
                                                          hundredDays['Close'],'Close',
                                                          hundredDays['Open'],'Open',
                                                          'time_series_open_close')
        
        Max = hundredDays['High'].max()
        Min = hundredDays['Low'].min()
        hundredDays['MidPrice'] = (hundredDays['High']+hundredDays['Low'])/2
        Mean = hundredDays['MidPrice'].mean()
        Std = hundredDays['MidPrice'].std()
    
        return render_template('hundred.html', resultStock=stockToTrade, 
                                    resultPrice = tradePrice,
                                    resultMax = Max, resultMin = Min,
                                    resultMean = Mean, resultStd = Std,
                                    jsonChartTrade100 = hundredTradesChart,
                                    jsonChartClose100 = hundredDaysClose, 
                                    status1 = Message, status2 = "Success Reading 100 Days Data")
                                
    else:
         hundredDaysClose = None
         return render_template('hundred.html', resultStock=stockToTrade, 
                                    resultPrice = tradePrice,jsonChartTrade100 = hundredTradesChart,
                                    jsonChartClose100 = hundredDaysClose,status1 = Message, 
                                    status2 = "Problems reading Hundred Day Data")

    
@app.route("/tradeConfirm", methods = ['POST', 'GET'])
def tradeConfirm():
  stockToTrade = request.form.get('stock')
  return render_template('details.html', resultStock=stockToTrade)

@app.route("/concludeTrade", methods = ['POST', 'GET'])
def concludeTrade():
    stockToTrade = request.form.get('stockChosen')
    tradeSide = request.form.get('sideChosen')
    tradeQuantity= int(request.form.get('quantity'))
    tradePrice = ReadQuote(stockToTrade)
    if tradePrice == 0:
        message = 'Error Reading Price'
    else:
        
        tradeDate = str(datetime.now())
        
        TransactionCost = tradeQuantity * tradePrice
        lastRecord = getLastRecord()
        cashAvailable = lastRecord[0]['Cash']
        if tradeSide == "buy":
            if cashAvailable >= TransactionCost:
                cashAvailable = cashAvailable - TransactionCost
                status = 0
            else:
                status = 1
        else:
            stockRemaining = countStock(stockToTrade)
            if stockRemaining >= tradeQuantity:
                cashAvailable = cashAvailable + TransactionCost
                status = 0
            else:
                status = 2
        
        if status == 0:
            message = addToBlotter(cashAvailable, tradeDate, tradePrice, tradeQuantity,
                                       tradeSide, stockToTrade)
            if tradeSide=="sell":
                oldRPLValue = pd.DataFrame(list(db.RealPL.find({'Ticker':stockToTrade})))
                
                if oldRPLValue.empty:
                    OldRPL=0
                else:
                    OldRPL = oldRPLValue['RPL']
                    
                CurrentWAP = calcWAP(stockToTrade)
                PL = TransactionCost - (CurrentWAP * tradeQuantity)
                print(str(PL))
                newRPL = OldRPL + PL
                if OldRPL != 0:
                    db.RealPL.update_one({
                          '_id': oldRPLValue['_id']
                        },{
                          '$set': {
                            'RPL': newRPL
                          }
                        }, upsert=False)
                else:
                    PLItem = {'Ticker':stockToTrade,'RPL': newRPL}
                    db.RealPL.insert_one(PLItem)
        elif status == 1:
            message = "Buy Error.  You do not have enough cash"
        else:
            message = "Sell Error.  You do not have enough stocks to sell that quantity"
    
    
    
    return render_template('confirmTrade.html', resultSide = tradeSide,
                                          resultStock = stockToTrade,
                                          resultQty = tradeQuantity,
                                          resultPrice = tradePrice,
                                           resultMessage = message)

if __name__ == "__main__":
    app.run(host='0.0.0.0')
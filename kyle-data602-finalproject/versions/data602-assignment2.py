#!/usr/bin/env/python3
"""
Created on Oct 15 19:49:43 2017
@author: kyleg
"""
# Resources:
# https://code.tutsplus.com/tutorials/creating-a-web-app-from-scratch-using-python-flask-and-mysql--cms-22972
# https://www.w3schools.com/bootstrap/default.asp
# https://www.w3schools.com/bootstrap/bootstrap_buttons.asp
# kyle1234!

# http://pandas-datareader.readthedocs.io/en/latest/remote_data.html#remote-data-nasdaq-symbols
# ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt


#The range is the highest high and the lowest low, the range the stock moved up and down in over the past 100 days...
#  hi - low / 2 is the average. You can use high / low or you sum(all trade prices) / volume of trades
#Daily standard deviation can be based on all trades for that day, you would find the mean and then the variance, etc..



import datetime
import math
from flask import Flask, render_template, request
import re
import urllib.request as req
import numpy as np
import scipy as sp
import pandas as pd
import matplotlib as mp
#import plotly
from bs4 import BeautifulSoup
from pymongo import MongoClient
import json

app = Flask(__name__)

@app.route("/")
def show_main_page():
    return render_template('main.html')

@app.route("/trade")
def show_trade_screen():
    return render_template('trade.html')

@app.route("/profile/<username>")
def profile(username):
    return "<h2>Hey, there %s<h2>" % username

@app.route("/blotter")
def show_blotter():
    return render_template('blotter.html')
#
@app.route("/pl")
def show_pl():
    return render_template('pl.html')

@app.route("/submitTrade",methods=['POST'])
def execute_trade():
    symbol = request.form['symbol']
    price = get_quote(symbol)
    return "You traded at $" + str(price)

#if __name__ == "__main__":
#    app.run() # host='0.0.0.0' needed for docker


#replicaSet=data602-shard-0&

"""Support Functions""" 
def get_quote(symbol):
    """Scrapes the current market price from Nasdaq!"""    
    url = 'http://www.nasdaq.com/symbol/' + symbol
    i = 0
    while True:
        try:
            page = req.urlopen(url)
            soup = BeautifulSoup(page, 'html.parser')
            price_box = soup.find('div', attrs={'class':'qwidget-dollar'})
            price = float(re.findall("\d+\.\d+",price_box.text)[0])
#            break
        except:
            if i == 1: 
                price = None
                break
            i += 1
        else: 
            return price

def menu_selection(menu, print_menu = True):
    if print_menu:
        for idx, option in enumerate(menu):
            print(str(idx + 1) + " - " + option) 
    while True:
        try:       
            selected_int = int(input(">")) - 1      
            math.sqrt(selected_int)
            selected_action = menu[selected_int]
        except (TypeError, IndexError, ValueError):
            print("\nThat's not a valid selection... \nPlease try again!\n") 
        else:
            return selected_action

def get_90_day_history(symbol):
    """Retrieves metrics for the last 90 days"""
    url = 'http://www.nasdaq.com/symbol/%s/historical' % symbol
    try:
        table = pd.read_html(url, parse_dates=True)[5].dropna()
    except Exception as e:
        print(e)
    else:
        table.columns = ["Date", "Open", "High", "Low", "Close / Last", "Volume"]
    return table

def get_last_100_trades(symbol):
    """Retrieves metrics for the last 90 days"""
    url1 = 'http://www.nasdaq.com/symbol/%s/time-sales' % symbol
    url2 = url1 + "?time=0&pageno=2"
    try:
        table1 = pd.read_html(url1, parse_dates=True)[5].dropna()
        table2 = pd.read_html(url2, parse_dates=True)[5].dropna()
    except Exception as e:
        print(e)
    else:    
        final_table = table1.append(table2, ignore_index=True)
        final_table.columns = ["Time", "Price", "Volume"]
    return final_table

""" Main Menu Functions """
def trade(pl_df, blotter_df, portfolio, equities, db, max_shares_exp10 = 7):  
    """Receives trade inputs and updates the blotter and P&L"""

    actions = ("Buy", "Sell", "Stop Trading")
    yes_no = ("Yes", "No")
    quantity = [i + 1 for i in range(10**max_shares_exp10)]
    
    while True:
        """4 questions and inputs"""
        #1: Main Trading Menu
        print("\nDo you want to Buy, Sell or Stop Trading?\n")        
        selected_action = menu_selection(actions)
        print(selected_action)
        if selected_action == "Stop Trading": break
        else:      
            #2: which equity
            print("\nWhich equity do you want to %s?\n" % selected_action.lower()) 
            if selected_action == "Buy":
                selected_ticker = menu_selection(equities["Symbol"][:10])
            else:
                try: 
                    current_equities = pl_df.index.tolist()
                    1/len(current_equities)
                    selected_ticker = menu_selection(current_equities)
                except:
                    print("You don't have any equities to sell yet!") 
                    trade(pl_df, blotter_df, portfolio, equities, db)
             
            #load or initialize Series for transaction                    
            try: 
                current_pl_values = pl_df.loc[selected_ticker]
            except:                          
                initiate_row = np.zeros(len(pl_df.columns))                   
                current_pl_values = pd.Series(data = initiate_row,
                                              index = pl_df.columns)    
            #validate selected action            
            if selected_action == "Sell" and int(current_pl_values["Position"]) == 0:
                print("\nYou don't any units to sell!\n")                           
            elif selected_action == "Buy" and portfolio <= 0:
                print("\nYou don't have any money left. Try to sell something!\n")                    
            else:                                    
                try:
                    current_price = get_quote(selected_ticker)
                    current_price / 1
#                            history_90days = get_90_day_history(selected_ticker)
#                            history_100trades = get_last_100_trades(selected_ticker)
                except:
                    print("Could not retrieve quote! Please try again.")   
#                            break
                else:
                #print(history_df)
                    #3: How many?
                    print("\nHow many shares do you want to %s?\n(Max is 1 million)" % selected_action.lower())
                    selected_quantity = menu_selection(quantity, print_menu = False)
                    current_transaction_amt = current_price * selected_quantity  
                    
                    #validation against position and portfolio
                    if selected_action == "Sell" and selected_quantity > int(current_pl_values["Position"]):
                        print("\nYou don't have that many units in your current position."
                              "Please try again!\n")
                        trade(pl_df, blotter_df, portfolio, equities, db)
                    elif selected_action == "Buy" and current_transaction_amt > portfolio:
                        print("\nYou don't have enough cash on hand for this purchase.\n")
                        trade(pl_df, blotter_df, portfolio, equities, db)
                    else:    
                        #4: Confirm Transaction
                        print("\nPlease confirm that you want to %s %d shares of %s at $%g, a total of $%s?\n"
                              % (selected_action.lower(), selected_quantity, selected_ticker, 
                                 current_price, "{0:,.2f}".format(current_transaction_amt))) 
                        confirm = menu_selection(yes_no)     
                        if confirm == "No": 
                            trade(pl_df, blotter_df, portfolio, equities, db)
                        #execute transaction
                        try:
                            executed_price = get_quote(selected_ticker)
                            transaction_dt = str(datetime.datetime.now())[0:19]
                            transaction_amount = executed_price * selected_quantity
                        except:
                            print("Could not retrieve quote! Please try again.")   
                            trade(pl_df, blotter_df, portfolio, equities, db)                                      
                        else:
                            if selected_action == "Buy" and transaction_amount > portfolio:
                                print("\nThe market price has changed, and you no longer have enough to make this purchase\n.")
                            else:                                      
                                ###Calculate new P&L values###
                                #update based upon action
                                if selected_action == "Sell": 
                                    current_pl_values["Position"] -= selected_quantity
                                    portfolio += transaction_amount
                                    
                                    current_pl_values["RPL"] += selected_quantity * (executed_price - current_pl_values["WAP"])
                                    if current_pl_values["Position"] == 0:
                                        current_pl_values["WAP"] = 0
                                else:
                                    #calculate new WAP
                                    total_price = current_pl_values["WAP"] * current_pl_values["Position"]
                                    new_numerator = transaction_amount + total_price
                                    new_denomenator = selected_quantity + current_pl_values["Position"]
                                    current_pl_values["WAP"] = new_numerator / new_denomenator
                                    #other calcs
                                    current_pl_values["Position"] = current_pl_values["Position"] + selected_quantity
                                    portfolio -= transaction_amount              
            
                                current_pl_values["Market Price"] = executed_price  
                                current_pl_values["UPL"] = current_pl_values["Position"] * executed_price
                                current_pl_values["Total P/L"] = current_pl_values["RPL"] + current_pl_values["UPL"]
                                 
                                #Append new values to P/L
                                pl_df.loc[selected_ticker] = current_pl_values
                                #update blodder
                                new_row = pd.Series([transaction_dt, selected_action, selected_ticker, 
                                                  selected_quantity, executed_price, transaction_amount, portfolio],
                                                  index = blotter_df.columns.tolist())
                                
                                blotter_df = blotter_df.append(new_row, ignore_index = True)  
        #                        print(blotter.to_string())
                                
                                # write to DB            
                                try:
                                    #insert into blotter
                                    new_blotter_doc = new_row.to_dict()    
                                    print(type(new_blotter_doc))
                                    print(new_blotter_doc)
                                    for key, value in new_blotter_doc.items():
                                        print(type(value))                                                            
    
                                    db.blotter.insert_one(new_blotter_doc)                      
                                    cursor = db.blotter.find()
                                    for document in cursor:
                                          print(document)   
                                          
                                    #upsert P&L row      
                                    query = {"_id" : selected_ticker}
                                    new_pl_doc = pd.Series(query).append(current_pl_values).to_dict()  
                                    pl_update = { '$set' : new_pl_doc}
                                    db.pl_table.update_one(query,
                                                         pl_update,
                                                         upsert=True)            
                                    
                                    cursor = db.pl_table.find()                     
                                    for document in cursor:
                                          print(document)                                      
                                except:
                                    print("Couldn't update P&L table in DB")
                                                
                                      
                                print("Transaction completed:\n %d shares of %s at $%g, a total of $%s.\n"
                                      % (selected_quantity, selected_ticker, executed_price, "{0:,.2f}".format(transaction_amount)))           
                
    print("Thank you for trading.\nYour blotter and P & L have been updated accordingly.")    
    print(pl_df.to_string())
    return pl_df, blotter_df, portfolio

def show_pl(pl_df, portfolio):
    """Scrapes latest market prices, calculates UPL & prints P&L in table format"""
    try:
        1/pl_df.shape[0]
        print("Pulling the latest market prices...\n")
        
        #Update price, UPL & Total P/L
        indices = pl_df.index.values.tolist()      
        for idx in indices:
            try:
                latest_quote = get_quote(idx)
                pl_df.loc[idx, "Market Price"] = latest_quote
                pl_df.loc[idx, "UPL"] = latest_quote * pl_df.loc[idx, "Position"]
                pl_df.loc[idx, "Total P/L"] = pl_df.loc[idx, "RPL"] + pl_df.loc[idx, "UPL"]                      
            except:
                print("Problem with updating market prices. Your patience is appreciated.")
                show_pl(pl_df, portfolio)
        
        #Update Allocations
        total_position = pl_df["Position"].sum()
        pl_df["Allocation by Shares"] = pl_df["Position"] / total_position
        
        total_UPL = pl_df["UPL"].sum()
        pl_df["Allocation by UPL"] = np.divide(pl_df["UPL"], total_UPL) 
        
        
        costs = pl_df["WAP"] * pl_df["Position"]
        total_cost = costs.sum()  
        pl_df["Allocation by Cost"] = np.divide(costs, total_cost)  
                
        print(pl_df.to_string())
        print("Total cash on hand: $" + str("{0:,.2f}".format(portfolio)))
      
    except:
        print("Let's try conducting some trades first!")
    return pl_df

    
def show_blotter(blotter_df):
    """Prints the blotter list object"""
    try:
        1/blotter_df.shape[0]        
        print(blotter_df.to_string())
    except:
        print('\nYOUR BLOTTER IS EMPTY... please conduct some trades first!')

def start_app():    
    """Starts the app and controls its flow"""
    print("\nWelcome to your Trading Console!")
    
    try:
        #retrieve all NASDAQ tickers
        nasdaq_csv = "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download"
        nasdaq_df = pd.read_csv(req.urlopen(nasdaq_csv), error_bad_lines=False).sort_values("Symbol").reset_index(drop = False)
        equities = nasdaq_df[["Symbol", "Name"]]
    except:
        print("Couldn't access NASDASQ CSV.")                
    else:        
        try:
            #connect to trading_db
            client = MongoClient("mongodb://kylegilde:kyle1234!@data602-shard-00-00-lfo48.mongodb.net:27017,data602-shard-00-01-lfo48.mongodb.net:27017,data602-shard-00-02-lfo48.mongodb.net:27017/admin?ssl=true&authSource=admin")
            db = client.trading_db
            
            #load or initialize blotter
            blotter_attributes = ("Transaction Date", "Side", "Ticker", "Quantity",
                          "Executed Price", "Transaction Amount", "Cash")
            
            try:
                blotter_df = pd.DataFrame(list(db.blotter.find())) 
                blotter_df = blotter_df[[*blotter_attributes]]
            except:
                pl_df = pd.DataFrame(columns = blotter_attributes)
            
#            blotter_df = pd.DataFrame(list(db.blotter.find()),
#                           columns = blotter_attributes)
            print(blotter_df.to_string())     
            
            #load or initialize P&L
            pl_attributes = ("WAP", "Position", "Market Price", "UPL", "RPL", 
                             "Total P/L", "Allocation by Shares", "Allocation by Cost", "Allocation by UPL")  
            
            try:
                pl_df = pd.DataFrame(list(db.pl_table.find())).set_index("_id")  
                pl_df.index.names = ['Ticker']
                pl_df = pl_df[[*pl_attributes]]
             
            except:
                pl_df = pd.DataFrame(columns = pl_attributes,
                        dtype = np.float64)
                pl_df.index.names = ['Ticker'] 
                    
            print(pl_df.to_string())

            #load or initialize portfolio value
            try: 
                portfolio = float(blotter_df.iat[-1, 6])
                portfolio/1
            except TypeError:    
                portfolio = 10000000    
            print(portfolio)           
        except:
            print("Couldn't connect to database.")  
                
        else:        
            # Control flow
            menu = ('Trade','Show P/L','Show Blotter','Quit')
            while True:
                print('\nPlease select the number corresponding to one of the menu options:\n')
                selection = menu_selection(menu)
                if selection == 'Trade':
                    pl_df, blotter_df, portfolio = trade(pl_df, blotter_df, portfolio, equities, db)  
                elif selection == 'Show P/L':
                    pl_df = show_pl(pl_df, portfolio)            
                elif selection == 'Show Blotter':
                    show_blotter(blotter_df)           
                elif selection == 'Quit':
                    print("Thank you for trading... Have a great day!")   
                    break   
           
start_app() 

            #reset
#            db.blotter.drop()
#            db.pl_table.drop()
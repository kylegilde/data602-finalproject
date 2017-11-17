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
import datetime
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
            break
        except:
            if i == 1: 
                price = None
                break
            i += 1
    return price

def menu_selection(menu, print_menu = True):
    if print_menu:
        for idx, option in enumerate(menu):
            print(str(idx + 1) + " - " + option) 
    while True:
        try:       
            selected_action = menu[int(input(">")) - 1]
            break
        except:
            print("\nThat's not a valid selection... \nPlease try again!\n") 
    return selected_action

def get_historicals(symbol):
    """Retrieves metrics for the last 90 days"""
    url = 'http://www.nasdaq.com/symbol/%s/historical' % symbol
    table = pd.read_html(url, parse_dates=True)[5].dropna()
    new_columns = ["Date", "Open", "High", "Low", "Close / Last", "Volume"]
    table.columns = new_columns
    return table

""" Main Menu Functions """
def trade(pl_table, blotter, portfolio, equities, db, max_shares_exp10 = 7):  
    """Receives trade inputs and updates the blotter and P&L"""

    actions = ("Buy", "Sell", "Stop Trading")
    yes_no = ("Yes", "No")
    quantity = [i + 1 for i in range(10**max_shares_exp10)]
    
    while True:
        """4 questions and inputs"""
        print("\nDo you want to Buy, Sell or Stop Trading?\n")        
        selected_action = menu_selection(actions)
        if selected_action == "Stop Trading": 
            break
        else:
            print("\nWhich equity do you want to %s?\n" % selected_action.lower()) 
            if selected_action == "Buy":
                selected_ticker = menu_selection(equities["Symbol"][:10])
            else:
                while True:
                    try: 
                        current_equities = pl_table.index.tolist()
                        1/len(current_equities)
                        selected_ticker = menu_selection(current_equities)
                        break
                    except:
                        print("You don't have any equities to sell yet!") 
                        trade(pl_table, blotter, portfolio, equities, db)
                        
            try: 
                current_pl_values = pl_table.loc[selected_ticker]
            except:                          
                initiate_row = np.zeros(len(pl_table.columns))                   
                current_pl_values = pd.Series(data = initiate_row,
                                              index = pl_table.columns)    
    
            #validate selected action            
            if selected_action == "Sell" and int(current_pl_values["Position"]) == 0:
                print("\nYou don't any units to sell!\n")                           
            elif selected_action == "Buy" and portfolio <= 0:
                print("\nYou don't have any money left. Try to sell something!\n")                    
            else:                                    
                try:
                    current_price = get_quote(selected_ticker)
                    current_price / 1
                    history_df = get_historicals(selected_ticker)
                except:
                    print("Could not retrieve quote! Please try again.")   
                    break
                
                #print(history_df)
                print("\nHow many shares do you want to %s?\n(Max is 1 million)" % selected_action.lower())
                selected_quantity = menu_selection(quantity, print_menu = False)
                current_transaction_amt = current_price * selected_quantity  
                
                #validation against position and portfolio
                if selected_action == "Sell" and selected_quantity > int(current_pl_values["Position"]):
                    print("\nYou don't have that many units in your current position."
                          "Please try again!\n")
                elif selected_action == "Buy" and current_transaction_amt > portfolio:
                    print("\nYou don't have enough cash on hand for this purchase.\n")
                else:    
                    #confirm transaction
                    print("\nPlease confirm that you want to %s %d shares of %s at $%g, a total of $%s?\n"
                          % (selected_action.lower(), selected_quantity, selected_ticker, 
                             current_price, "{0:,.2f}".format(current_transaction_amt))) 
                    confirm = menu_selection(yes_no)      
                    if confirm == "No": break
                    
                    #execute transaction
                    try:
                        executed_price = get_quote(selected_ticker)
                        transaction_dt = str(datetime.datetime.now())[0:19]
                        executed_price / 1
                    except:
                        print("Could not retrieve quote! Please try again.")   
                        break                                      
                    
                    transaction_amount = executed_price * selected_quantity
                    
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
                        pl_table.loc[selected_ticker] = current_pl_values
                        #update blodder
                        new_row = pd.Series([transaction_dt, selected_action, selected_ticker, 
                                          selected_quantity, executed_price, transaction_amount, portfolio],
                                          index = blotter.columns.tolist())
#                        new_row = [transaction_dt, selected_action, selected_ticker, 
#                                   selected_quantity, executed_price, transaction_amount, portfolio]
                        
                        blotter = blotter.append(new_row, ignore_index = True)  
                        print(blotter.to_string())
                        
                        #db_blotter = 
                        row_insert = new_row.to_dict()
                        db.blotter.insert_one(row_insert)                      
                        cursor = db.blotter.find()
                        for document in cursor:
                              print(document)
                              
                        print("Transaction completed:\n %d shares of %s at $%g, a total of $%s.\n"
                              % (selected_quantity, selected_ticker, executed_price, "{0:,.2f}".format(transaction_amount)))           
            
        print("Thank you for trading.\nYour blotter and P & L have been updated accordingly.")
        
        print(pl_table.to_string())
        return pl_table, blotter, portfolio

def show_pl(pl_table, portfolio):
    """Scrapes latest market prices, calculates UPL & prints P&L in table format"""
    try:
        1/pl_table.shape[0]
        print("Pulling the latest market prices...\n")
        
        #Update price, UPL & Total P/L
        indices = pl_table.index.values.tolist()      
        for idx in indices:
            try:
                latest_quote = get_quote(idx)
                pl_table.loc[idx, "Market Price"] = latest_quote
                pl_table.loc[idx, "UPL"] = latest_quote * pl_table.loc[idx, "Position"]
                pl_table.loc[idx, "Total P/L"] = pl_table.loc[idx, "RPL"] + pl_table.loc[idx, "UPL"]                      
            except:
                print("Problem with updating market prices. Your patience is appreciated.")
                show_pl(pl_table, portfolio)
        
        #Update Allocations
        total_position = pl_table["Position"].sum()
        pl_table["Allocation by Shares"] = pl_table["Position"] / total_position
        
        total_UPL = pl_table["UPL"].sum()
        pl_table["Allocation by UPL"] = np.divide(pl_table["UPL"], total_UPL) 
                 
        total_cost = np.multiply(pl_table["WAP"], pl_table["Position"]).sum()
        pl_table["Allocation by Cost"] = np.divide(pl_table["UPL"], total_cost)  
                
        print(pl_table.to_string())
        print("Total cash on hand: $" + str("{0:,.2f}".format(portfolio)))
      
    except:
        print("Let's try conducting some trades first!")
    return pl_table

    
def show_blotter(blotter):
    """Prints the blotter list object"""
    try:
        1/blotter.shape[0]        
        print(blotter.to_string())
    except:
        print('\nYOUR BLOTTER IS EMPTY... please conduct some trades first!')




#client = MongoClient("mongodb://kylegilde:kyle1234!@data602-shard-00-00-lfo48.mongodb.net:27017,data602-shard-00-01-lfo48.mongodb.net:27017,data602-shard-00-02-lfo48.mongodb.net:27017/admin?ssl=true&authSource=admin")
#db = client.trading_db
#db.collection_names()
#df = pd.DataFrame.from_dict({'A': {1: datetime.datetime.now()}})
#records = json.loads(df.T.to_json()).values()
#test = db.test
#test.insert_many(records)
#cursor = test.find({})
#for document in cursor:
#      print(document)
#df2 = pd.DataFrame(list(test.find({})))
#
#pl_table = db.pl_table
#records = json.loads(df.T.to_json()).values()


def start_app():    
    """Starts the app and controls its flow"""
    print("\nWelcome to your Trading Console!")
    
    #initialize variables
    menu = ('Trade','Show P/L','Show Blotter','Quit')
    
    nasdaq_csv = "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download"
    nasdaq_df = pd.read_csv(req.urlopen(nasdaq_csv)).sort_values("Symbol").reset_index(drop = False)
    equities = nasdaq_df[["Symbol", "Name"]]
    
    #initialize P&L 
    pl_attributes = ("Ticker", "WAP", "Position", "Market Price", "UPL", "RPL", 
                     "Total P/L", "Allocation by Shares", "Allocation by Cost", "Allocation by UPL")  
    pl_table = pd.DataFrame(columns = pl_attributes,
                            dtype = np.float64)
    pl_table = pl_table.set_index("Ticker")
    
    # Initialize Blotter & P/L
    try:
        client = MongoClient("mongodb://kylegilde:kyle1234!@data602-shard-00-00-lfo48.mongodb.net:27017,data602-shard-00-01-lfo48.mongodb.net:27017,data602-shard-00-02-lfo48.mongodb.net:27017/admin?ssl=true&authSource=admin")
        db = client.trading_db 
        blotter_attributes = ("Transaction Date", "Side", "Ticker", "Quantity",
                      "Executed Price", "Transaction Amount", "Cash")
        blotter = pd.DataFrame(list(db.blotter.find()),
                       columns = blotter_attributes)
        print(blotter.to_string())
    except:
        print("Couldn't connect to database. Please quit the app and start again.")

    try: 
        portfolio = blotter.iat[-1, 6]
        portfolio/1
    except:    
        portfolio = 10000000    
    print(portfolio)
    
    # Control flow
    while True:
        print('\nPlease select the number corresponding to one of the menu options:\n')
        selection = menu_selection(menu)
        if selection == 'Trade':
            pl_table, blotter, portfolio = trade(pl_table, blotter, portfolio, equities, db)  
        elif selection == 'Show P/L':
            pl_table = show_pl(pl_table, portfolio)            
        elif selection == 'Show Blotter':
            show_blotter(blotter)           
        elif selection == 'Quit':
            break   
    print("Thank you for trading... Have a great day!")        
    
start_app() 

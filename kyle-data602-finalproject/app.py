#!/usr/bin/env/python3
"""
Created on Oct 15 19:49:43 2017
@author: kyleg
"""
import datetime
import math
from flask import Flask, render_template, request, redirect, url_for, session, make_response
import re
import urllib.request as req
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from io import BytesIO
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from bs4 import BeautifulSoup
from pymongo import MongoClient

app = Flask(__name__)
        
@app.route("/")
def show_main_page():
    return render_template('main.html')

"""Main Menu Functions & Pages"""
@app.route("/trade")
def show_trade_screen():
    symbols = list(NASDAQ_SYMBOLS["Symbol"])
    return render_template('trade.html', symbols = symbols)

@app.route("/blotter")
def show_blotter():
    """Loads & prints blotter"""
    blotter_attributes = ("Transaction Date", "Side", "Ticker", "Quantity",
                  "Executed Price", "Transaction Amount", "Cash")        
    try:
        blotter_df = pd.DataFrame(list(db.blotter.find())) 
        blotter_df = blotter_df[[*blotter_attributes]]
    except:
        blotter_df = pd.DataFrame(columns = blotter_attributes)
    blotter_df = blotter_df.sort_values("Transaction Date", ascending = False)     
    return render_template('blotter.html', table = blotter_df.to_html())

@app.route("/pl")
def show_pl():
    """Retrieve or initialize P&L"""
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
    #Update price, UPL & Total P/L
    indices = pl_df.index.values.tolist()      
    for idx in indices:
        try:
            latest_quote = get_quote(idx)
            pl_df.loc[idx, "Market Price"] = latest_quote
            pl_df.loc[idx, "UPL"] = latest_quote * pl_df.loc[idx, "Position"]
            pl_df.loc[idx, "Total P/L"] = pl_df.loc[idx, "RPL"] + pl_df.loc[idx, "UPL"]                      
        except Exception as e:
            print(e)
    
    #Update Allocations
    total_position = pl_df["Position"].sum()
    pl_df["Allocation by Shares"] = pl_df["Position"] / total_position
    
    total_UPL = pl_df["UPL"].sum()
    pl_df["Allocation by UPL"] = np.divide(pl_df["UPL"], total_UPL) 
    
    costs = pl_df["WAP"] * pl_df["Position"]
    total_cost = costs.sum()  
    pl_df["Allocation by Cost"] = np.divide(costs, total_cost)  
    #Grand Total the columns
    pl_df = pl_df.append(pd.DataFrame(pl_df.sum(), columns = ['Grand Total']).T)
    nulls = ["WAP", "Market Price"]
    pl_df.loc['Grand Total', nulls] = ""
         
    cash = "Total cash on hand: $" + "{0:,.2f}".format(get_portfolio())  
    return render_template('pl.html', table = pl_df.to_html(), cash = cash)

"""Trading Desk Functions & Pages"""
@app.route("/confirmation", methods=['POST'])
def confirmation():
    try:
        selected_quantity = int(request.form['quantity'])           
    except:
        return redirect(url_for('invalid_input'))
    else: 
        try:
            symbol = request.form['symbol']
            side = request.form['side']        
            price = get_quote(symbol)
            transaction_amount = selected_quantity * price
            portfolio = get_portfolio()
        except:
            return redirect(url_for('retrieval_failure'))
        else:
            #validate selected action    
            try:
                if side == "sell":
                    1 / get_position(symbol)                      
                else:
                    1 / portfolio
                    math.sqrt(portfolio - transaction_amount)                  
            except (ZeroDivisionError, ValueError):
                if side == "sell":
                    return redirect(url_for('no_units_to_sell'))                     
                else:
                    return redirect(url_for('no_money_left'))
            else:
                #analytics & confirm transaction
                try:
                   stats_100trades, session['x_100trades'], session['y_100trades']  = get_last_100_trades(symbol) #, df_100trades
                   stats_90days, session['date_90days'], session['price_90days'] = get_90_day_history(symbol)
                   
                except:
                    return redirect(url_for('retrieval_failure'))
                else:
                    session['side'], session['symbol'], session['selected_quantity'], session['portfolio'] = side, symbol, selected_quantity, portfolio
                    confirm = "Please confirm that you want to %s %d share(s) of %s at $%s, a total of $%s?\n" % (side, selected_quantity, symbol, "{0:,.2f}".format(price), "{0:,.2f}".format(transaction_amount))                                 
                    stats_tables = [stats_100trades, stats_90days]
                    charts = ["/chart-price-100trades.png","/chart-avgprice-90days.png"]
                    titles = ['Last 100 Trades', 'Last 90 Days']
                return render_template('confirmation.html', confirm = confirm, tables = stats_tables, titles = titles, charts = charts)

@app.route("/submitTrade", methods=['POST'])
def execute_trade():
    try:
        submitted_value = request.form['confirm']        
    except:
        return redirect(url_for('invalid_input'))
    else: 
        if submitted_value == 'no':
            return redirect('/')
        else:
            try:
                side, symbol, selected_quantity, portfolio = session.get('side'), session.get('symbol'), session.get('selected_quantity'), session.get('portfolio')
            except:
                return redirect(url_for('retrieval_failure'))
            else:
                current_pl_values = get_pl_row(symbol)
                #execute transaction
                try:
                    executed_price = get_quote(symbol)
                    transaction_dt = str(datetime.datetime.now())[0:19]
                    transaction_amount = executed_price * selected_quantity
                except:
                    return redirect(url_for('retrieval_failure'))                                    
                else:
                    #validate selected action    
                    try:
                        if side == "buy":
                            math.sqrt(portfolio - transaction_amount)                                                                 
                    except (ValueError):
                        return redirect(url_for('no_money_left'))
                    else:                                      
                        ###Calculate new P&L values###
                        #update based upon action
                        if side == "sell": 
                            current_pl_values["Position"] -= selected_quantity
                            new_portfolio = portfolio + transaction_amount
                            
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
                            new_portfolio = portfolio - transaction_amount             
    
                        current_pl_values["Market Price"] = executed_price  
                        current_pl_values["UPL"] = current_pl_values["Position"] * executed_price
                        current_pl_values["Total P/L"] = current_pl_values["RPL"] + current_pl_values["UPL"]                       

                        #Create New Blotter Document
                        blotter_attributes = ("Transaction Date", "Side", "Ticker", "Quantity",
                                      "Executed Price", "Transaction Amount", "Cash")         
                        new_transaction = [transaction_dt, side, symbol, 
                                          selected_quantity, executed_price, transaction_amount, new_portfolio]
                        new_blotter_doc = dict(zip(blotter_attributes, new_transaction))
                        # write to DB            
                        try:
                            #insert into blotter
                            db.blotter.insert_one(new_blotter_doc)                      
                                
                            #upsert P&L row      
                            query = {"_id" : symbol}
                            new_pl_doc = pd.Series(query).append(current_pl_values).to_dict()  
                            pl_update = { '$set' : new_pl_doc}
                            db.pl_table.update_one(query,
                                                 pl_update,
                                                 upsert=True)                                              
                        except:
                            return redirect(url_for('retrieval_failure'))
    transaction_type = {'buy':"Purchase", 'sell':"Sale" }      
    summary = "%d share(s) of %s at $%s, a total of $%s.\n" % (selected_quantity, symbol, "{0:,.2f}".format(executed_price), "{0:,.2f}".format(transaction_amount))   
    return render_template('submitTrade.html', transaction_type = transaction_type[side], summary = summary)

"""Support Functions"""
@app.route("/chart-avgprice-90days.png") 
def chart_avgprice_90days():
    fig=Figure()
    ax=fig.add_subplot(111)   
    x, y = session['date_90days'], session['price_90days']
    ax.plot(x, y, "go--", label='Price')
    fig.autofmt_xdate()
    canvas=FigureCanvas(fig)
    png_output = BytesIO()
    canvas.print_png(png_output)
    response = make_response(png_output.getvalue())
    response.headers['Content-Type'] = 'image/png'
    return response    
 
@app.route("/chart-price-100trades.png") 
def chart_price_100trades():
    fig=Figure()
    ax=fig.add_subplot(111)   
    x, y = session['x_100trades'], session['y_100trades'] 
    ax.plot(x, y, "go--", label='Price')
    fig.autofmt_xdate()
    canvas=FigureCanvas(fig)
    png_output = BytesIO()
    canvas.print_png(png_output)
    response = make_response(png_output.getvalue())
    response.headers['Content-Type'] = 'image/png'
    return response

def get_last_100_trades(symbol):
    """Retrieves metrics for the last 100 trades"""
    url1 = 'http://www.nasdaq.com/symbol/%s/time-sales' % symbol
    url2 = url1 + "?time=0&pageno=2"
    try:
        page = req.urlopen(url1)
        soup = BeautifulSoup(page, 'html.parser')
        date_box = soup.find('span', attrs={'id':'qwidget_markettime'})
        date = date_box.text
        
        table1 = pd.read_html(url1, parse_dates = True, flavor='html5lib')[5].dropna()
        table2 = pd.read_html(url2, parse_dates = True, flavor='html5lib')[5].dropna()        
    except Exception as e:
        print(e)
    else:    
        final_table = table1.append(table2, ignore_index=True)
        final_table.columns = ["DateTime", "Price", "Volume"]
        final_table["Price"] = final_table["Price"].str.extract('(\d+,?\d+\.?\d+)')
        final_table["Price"] = final_table["Price"].str.replace(',', '').astype('float')
        final_table["DateTime"] = pd.to_datetime(date+final_table["DateTime"])
        #stats
        mean_100trades, max_100trades, min_100trades, std_100trades = final_table["Price"].mean(), final_table["Price"].max(), final_table["Price"].min(), final_table["Price"].std()
        stats = pd.Series([mean_100trades, max_100trades, min_100trades, std_100trades],
                  index = ["mean", "max", "min", "sd"], name = "")
        stats_df = pd.DataFrame(stats).transpose()
        x_100trades = list(final_table["DateTime"])
        y_100trades = list(final_table["Price"])
    return stats_df.to_html(classes = '100trades'), x_100trades, y_100trades
      
def get_90_day_history(symbol):
    """Retrieves metrics for the last 90 days"""
    url = 'http://www.nasdaq.com/symbol/%s/historical' % symbol
    try:
        table = pd.read_html(url, parse_dates = True, flavor='html5lib')[5].dropna()
    except Exception as e:
        print(e)
    else:
        table.columns = ["Date", "Open", "High", "Low", "Close / Last", "Volume"]
        table["Daily Average"] = (table["High"] + table["Low"]) / 2
        table["Date"] = pd.to_datetime(table["Date"])
        mean_90days, max_90days, min_90days, std_90days = table["Daily Average"].mean(), table["High"].max(), table["Low"].min(), table["Daily Average"].std()
        stats = pd.Series([mean_90days, max_90days, min_90days, std_90days],
                  index = ["mean", "max", "min", "sd"], name = "")
        stats_df = pd.DataFrame(stats).transpose()
        date_90days = list(table["Date"])
        price_90days = list(table["Daily Average"])
    return stats_df.to_html(classes = '90days'),date_90days, price_90days

def get_quote(symbol):
    """Makes 3 attempts to scrape the current market price from Nasdaq"""    
    url = 'http://www.nasdaq.com/symbol/' + symbol
    i = 0
    while True:
        try:
            page = req.urlopen(url)
            soup = BeautifulSoup(page, 'html.parser')
            price_box = soup.find('div', attrs={'class':'qwidget-dollar'})
            price = float(re.findall("\d+\.?\d*",price_box.text)[0])
        except:
            if i == 2: 
                price = None
                break
            i += 1
        else: 
            return price
       
def get_portfolio():
    """Loads or initializes current portfolio value"""    
    try:
        blotter_df = pd.DataFrame(list(db.blotter.find()))
        1 / len(blotter_df)
        portfolio = float(blotter_df.iat[-1, 0])
        portfolio / 1
    except:
        portfolio = 10000000 
    return portfolio

def get_pl_row(symbol):
    """Retrieves or initializes P&L row"""
    pl_attributes = ("WAP", "Position", "Market Price", "UPL", "RPL", 
                 "Total P/L", "Allocation by Shares", "Allocation by Cost", "Allocation by UPL")     
    try:
        query = {"_id" : symbol}
        current_pl_values = db.pl_table.find_one(query)
        del current_pl_values["_id"]
        current_pl_values = pd.Series(current_pl_values)
    except:                          
        initiate_row = np.zeros(len(pl_attributes))                   
        current_pl_values = pd.Series(data = initiate_row,
                                      index = pl_attributes) 
    return current_pl_values                
 
def get_position(symbol):
    """Retrieves position or initializes a position of 0"""
    try:
        pl_df = pd.DataFrame(list(db.pl_table.find())).set_index("_id") 
        position = int(pl_df.loc[symbol, "Position"])
    except (KeyError, ValueError, TypeError):
        position = 0
    return position        

"""Exception Handling"""
@app.route("/invalid_input")
def invalid_input():
    return render_template('invalid_input.html')

@app.route("/no_money_left")
def no_money_left():
    return render_template('no_money_left.html')

@app.route("/no_units_to_sell")
def no_units_to_sell():
    return render_template('no_units_to_sell.html')

@app.route("/retrieval_failure")
def retrieval_failure():
    return render_template('retrieval_failure.html')

"""initialize connections & DataFrames"""
try:
    #Connect to trading_db
    client = MongoClient("mongodb://kylegilde:kyle1234!@data602-shard-00-00-lfo48.mongodb.net:27017,data602-shard-00-01-lfo48.mongodb.net:27017,data602-shard-00-02-lfo48.mongodb.net:27017/admin?ssl=true&authSource=admin")
    db = client.trading_db
except:
    print("Couldn't connect to database.")  
else:    
    #retrieve NASDAQ tickers from DB or website
    try:
        NASDAQ_SYMBOLS = pd.DataFrame(list(db.nasdaq.find()))
        1 / len(NASDAQ_SYMBOLS)
    except:
        nasdaq_csv = "http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=NASDAQ&render=download"
        nasdaq_df = pd.read_csv(req.urlopen(nasdaq_csv), error_bad_lines=False).sort_values("Symbol").reset_index(drop = False)
        NASDAQ_SYMBOLS = nasdaq_df[["Symbol", "Name"]]    
        db.nasdaq.insert_many(NASDAQ_SYMBOLS.to_dict("records"))          
    if __name__ == "__main__":
        app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'
        app.run(host='0.0.0.0') # host='0.0.0.0' #needed for docker 
#        app.run()
#        docker-machine ip default   
#        docker run -p 81:5000 -it myapp
#        http://192.168.99.100:81
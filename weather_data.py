# -*- coding: utf-8 -*-
"""
Created on Mon Nov 20 14:17:39 2017

@author: kyleg
"""

import datetime as dt
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from pymongo import MongoClient
import time

url1 = 'http://www.zipcodestogo.com/New%20York/'
zip_table = pd.read_html(url1, flavor='html5lib', attrs={'class':'inner_table'})[0]
zip_table = zip_table[[0, 1, 2]]
zip_codes = zip_table.loc[zip_table[2] == 'New York', 0]


#100xx–104xx, 11004–05, 111xx–114xx, 116xx
#zip_codes = np.arange(10001, 10005)
#zip_codes = np.arange(10001, 10499)
#url = 'https://www.wunderground.com/history/airport/KNYC/2014/11/19/DailyHistory.html?reqdb.zip=10001'
try:
    #Connect to trading_db
    client = MongoClient("mongodb://kylegilde:kyle1234!@data602-shard-00-00-lfo48.mongodb.net:27017,data602-shard-00-01-lfo48.mongodb.net:27017,data602-shard-00-02-lfo48.mongodb.net:27017/admin?ssl=true&authSource=admin")
    db = client.trading_db
except:
    print("Couldn't connect to database.")  
else:  
    try:
        weather_collection = pd.DataFrame(list(db.weather_collection.find()))
        weather_collection = weather_collection.set_index(['Date', 'Zip Code'])
        start_date = np.max(weather_collection['Date'])
    except:
        start_date = dt.date(2014, 9, 17)
        
    end_date = start_date + dt.timedelta(days=3) #dt.date.today()
    date_list =  pd.period_range(start_date, end_date, freq='D')
    
    
    zip_url = 'http://www.zipcodestogo.com/New%20York/'
    zip_table = pd.read_html(zip_url, flavor='html5lib', attrs={'class':'inner_table'})[0]
    zip_table = zip_table[[0, 1, 2]]
    zip_codes = zip_table.loc[zip_table[2] == 'New York', 0]
    url = 'https://www.wunderground.com/history/airport/KNYC/%d/%d/%d/DailyHistory.html?reqdb.zip=%s'
    start = time.time()
    
    raw_weather = pd.DataFrame()
    i = 0
    for a_date in date_list:
        for zip_code in zip_codes:
            year, month, day = a_date.year, a_date.month, a_date.day
            url_instance = url % (year, month, day, zip_code)
            table_instance = pd.read_html(url_instance, flavor='html5lib', attrs={'id':'historyTable'})[0]
            table_instance["Date"], table_instance["Zip Code"] = a_date, zip_code        
            raw_weather = table_instance.append(raw_weather, ignore_index = True)
            i += 1
    end = time.time()
    lapsed = end - start
    print(i)
    print(lapsed/60)
    print(lapsed/i)
    
    #Tidying
    tidy_weather = raw_weather.reset_index(drop = False)
    tidy_weather = tidy_weather.rename(columns = {'\xa0':'Attributes', 'Average ':'Average'})
    tidy_weather = tidy_weather[["Date", "Zip Code","Attributes", "Actual", "Average"]]
    tidy_weather["Actual"] = tidy_weather["Actual"].str.extract('(\d*\.?\d*)').astype(float)
    tidy_weather["Average"] = tidy_weather["Average"].str.extract('(\d*\.?\d*)').astype(float)
    
    # Pivot the long-form data
    weather_actuals = pd.pivot_table(tidy_weather, values='Actual', index = ["Date", "Zip Code"], columns = 'Attributes')
    weather_averages = pd.pivot_table(tidy_weather, values='Average', index = ["Date", "Zip Code"], columns = 'Attributes')
    weather_averages = weather_averages[['Precipitation', "Mean Temperature"]].dropna()
    weather_averages = weather_averages.rename(columns = {'Precipitation':'Average Precipitation', 'Mean Temperature':'Average Temperature'})
    
    # Join actuals & averages, new and old data
    weather_df = weather_actuals.join(other = weather_averages, how = 'left')    
    weather_df = weather_collection.append(weather_df)
    
    # Extreme Temperature Index
    temp_delta = np.abs(weather_df["Mean Temperature"] - weather_df['Average Temperature'])
    min_temp_delta = np.min(temp_delta)
    max_temp_delta = np.max(temp_delta)
    weather_df['Extreme Temperature Index'] = (temp_delta - min_temp_delta) / max_temp_delta
    
    # Extreme Precipitation Index
    precip_delta = np.abs(weather_df["Precipitation"] - weather_df['Average Precipitation'])
    min_precip_delta = np.min(precip_delta)
    max_precip_delta = np.max(precip_delta)
    weather_df['Extreme Precipitation Index'] = (precip_delta - min_precip_delta) / max_precip_delta
    
    # Extreme Wind Index
    min_wind = np.min(weather_df["Max Wind Speed"])
    max_wind = np.max(weather_df["Max Wind Speed"])
    weather_df['Extreme Wind Index'] = (weather_df["Max Wind Speed"] - min_wind) / max_wind
    
    # Extreme Visibility Index
    min_visibilitv = np.min(weather_df["Visibility"])
    max_visibilitv = np.max(weather_df["Visibility"])
    weather_df['Extreme Visibility Index'] = 1 - (weather_df["Visibility"] - min_visibilitv) / max_visibilitv
    
    # Extreme Weather Index
    weather_df['Extreme Weather Index'] = weather_df[['Extreme Temperature Index', 'Extreme Precipitation Index', 'Extreme Wind Index', 'Extreme Visibility Index']].mean(axis=1)
    
    db.weather_collection.drop()
    db.weather_collection.insert_many(weather_df.to_dict("records")) 


#except Exception as e:
print(e)


#url = 'https://tools.usps.com/go/ZipLookupResultsAction!input.action?items=100&page=1&companyName=&address1=&address2=&city=New+York+City&state=NY&zip=#'
#url = 'https://www.unitedstateszipcodes.org/ny/'
#headers = {
#    'User-Agent': 'Kyle Gilde, sps.cuny.edu',
#    'From': 'Kyle.Gilde@spsmail.cuny.edu'
#}
#
#url1 = 'http://www.zipcodestogo.com/New%20York/'
#a = pd.read_html(url1, parse_dates = True) , flavor='html5lib'


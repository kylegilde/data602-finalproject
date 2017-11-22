# -*- coding: utf-8 -*-
"""
Created on Mon Nov 20 14:17:39 2017

@author: kyleg
"""

import datetime as dt
import numpy as np
import pandas as pd
from pymongo import MongoClient
import time

try:
    atlas = "mongodb://kylegilde:kyle1234!@data602-shard-00-00-lfo48.mongodb.net:27017,data602-shard-00-01-lfo48.mongodb.net:27017,data602-shard-00-02-lfo48.mongodb.net:27017/admin?ssl=true&authSource=admin"
    client = MongoClient(atlas)
    db = client.trading_db
except:
    print("Couldn't connect to database.")
else:
    try:
        # db.weather_collection.drop()
        weather_collection = pd.DataFrame(list(db.weather_collection.find()))
        start_date = np.max(weather_collection['Date'])
    except KeyError:
        start_date = dt.date(2014, 9, 17)
    print(start_date)

    end_date = start_date + dt.timedelta(days=3) #dt.date(2014, 12, 31)# dt.date.today() #
    date_list = pd.date_range(start_date, end_date)
    # type(date_list[0])
    zip_url = 'http://www.zipcodestogo.com/New%20York/'
    zip_table = pd.read_html(zip_url, flavor='html5lib', attrs={'class': 'inner_table'})[0]
    zip_table = zip_table[[0, 1, 2]]
    zip_codes = zip_table.loc[zip_table[1] == 'New York', 0]

    zips = len(zip_codes)
    dates = len(date_list)
    print(zips, dates)
    print(dates*zips)
    print(dates*zips/60/60 *.8)
#https://www.wunderground.com/history/airport/KJRB/2017/11/5/DailyHistory.html?req_city=Brooklyn&req_state=NY
    url = 'https://www.wunderground.com/history/airport/KNYC/%d/%d/%d/DailyHistory.html?reqdb.zip=%s'
    start = time.time()
    #initialize DF
    raw_weather = pd.DataFrame()
    i = 0
    for a_date in date_list:
        for zip_code in zip_codes:
            year, month, day = a_date.year, a_date.month, a_date.day
            url_instance = url % (year, month, day, zip_code)
            try:
                table_instance = pd.read_html(url_instance, flavor='html5lib', attrs={'id': 'historyTable'})[0]
            except Exception as e:
                print(e)
            else:
                table_instance["Date"], table_instance["Zip Code"] = a_date, zip_code
                raw_weather = table_instance.append(raw_weather, ignore_index=True)
        i += 1
        end = time.time()
        lapsed = end - start
        per_loop = lapsed / i
        remaining_loops = dates - i
        minutes_remaining = per_loop * remaining_loops/60
        print(i, remaining_loops, dates, minutes_remaining)

    end = time.time()
    lapsed = end - start
    print(i)
    print(lapsed / 60)
    print(lapsed / i/ zips)

    # Tidying
    tidy_weather = raw_weather.rename(columns={'\xa0': 'Attributes', 'Average ': 'Average'})
    tidy_weather = tidy_weather[["Date", "Zip Code", "Attributes", "Actual", "Average"]]
    tidy_weather["Actual"] = tidy_weather["Actual"].str.extract('(\d*\.?\d*)').astype(float)
    tidy_weather["Average"] = tidy_weather["Average"].str.extract('(\d*\.?\d*)').astype(float)

    # Pivot Actuals & Averages
    weather_actuals = pd.pivot_table(tidy_weather, index=["Date", "Zip Code"], values='Actual', columns='Attributes')
    weather_averages = pd.pivot_table(tidy_weather, values='Average', index=["Date", "Zip Code"], columns='Attributes')
    weather_averages = weather_averages[['Precipitation', "Mean Temperature"]].dropna()
    weather_averages = weather_averages.rename(columns={'Precipitation': 'Average Precipitation',
                                                        'Mean Temperature': 'Average Temperature'})
    # Join Actuals & Averages
    weather_df = weather_actuals.join(other=weather_averages, how='left')
    # Combine Existing & New Data if necessary
    try:
        weather_collection = weather_collection.set_index(['Date', 'Zip Code'])
        weather_df = weather_collection.append(weather_df)
    except KeyError:
        print('Do nothing')
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
    weather_df['Extreme Weather Index'] = weather_df[['Extreme Temperature Index', 'Extreme Precipitation Index',
                                                      'Extreme Wind Index', 'Extreme Visibility Index']].mean(axis=1)
    # Insert into MongoDB
    weather_df = weather_df.reset_index()
    #db.weather_collection.drop()

    db.weather_collection.insert_many(weather_df.to_dict("records"))
    weather_collection = pd.DataFrame(list(db.weather_collection.find()))

    # weather_df.columns
    # weather_df.index
    # print(weather_df.head(20).to_string())
    # print(weather_df.tail(20).to_string())
    weather_collection.columns
    weather_collection.index
    weather_collection.shape
    print(weather_collection.head(20).to_string())
    print(weather_collection.tail(20).to_string())
    #


current_year = dt.date.today().year
i_year = current_year - 10
raw_csv = pd.DataFrame()
while i_year <= current_year:
    url = 'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/%d.csv.gz'
    url_instance = url % i_year
    table_instance = pd.read_csv(csv_url, compression='gzip',
                         names = ['Station', 'Date', 'Attribute', 'Value', 'x', 'y', 'z'],
                         index_col = False)
    table_instance = csv_df[['Station', 'Date', 'Attribute', 'Value']]
    table_instance = csv_df[csv_df['Station'].str.contains('US1NY')]
    raw_csv = table_instance.append(raw_csv, ignore_index=True)
    i_year += 1
    print(i_year)

station_url = 'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt'
stations_df = pd.read_table(station_url, names = ['Station', 'Latitude', 'Longitude', 'Elevation', 'State', 'Station Name', 'y','z'],
                         index_col = False)
stations_df.shape
stations_df.head()
stations_df.tail()
np.max(csv_df["Date"])

raw_csv.columns
raw_csv.shape
raw_csv.index
raw_csv.head()
raw_csv.tail()


df['ids'].str.contains('ball', na = False)
csv_df.rename
zip_codes = zip_table.loc[zip_table[1] == 'New York', 0]




    # type(weather_df['Date'][0])
    # weather_df['Date'] = weather_df['Date'].astype(np.datetime64)
    # weather_df['Date'] = np.datetime64(weather_df['Date'])
    #
    #
    # db.weather_collection.drop()
    # type(weather_collection['Date'][0])
    #
    #
    # weather_collection.index
    # weather_collection.columns
    # weather_df.columns
    # weather_df.index
    # pd.MultiIndex.from_tuples(list(zip(tidy_weather['Date'], tidy_weather['Zip Code'])), names = ['Date', 'Zip Code'])
    # Pivot the long-form data
   # type(tidy_weather['Date'][0])
    # tidy_weather = tidy_weather.set_index(['Date', 'Zip Code'])#
   # tidy_weather.index
    #tidy_weather.pivot(index=["Date", "Zip Code"], columns='Attributes', values='Actual')

# except Exception as e:
# print(e)


# url = 'https://tools.usps.com/go/ZipLookupResultsAction!input.action?items=100&page=1&companyName=&address1=&address2=&city=New+York+City&state=NY&zip=#'
# url = 'https://www.unitedstateszipcodes.org/ny/'
# headers = {
#    'User-Agent': 'Kyle Gilde, sps.cuny.edu',
#    'From': 'Kyle.Gilde@spsmail.cuny.edu'
# }
#
# url1 = 'http://www.zipcodestogo.com/New%20York/'
# a = pd.read_html(url1, parse_dates = True) , flavor='html5lib'

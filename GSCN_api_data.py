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
from pygeocoder import Geocoder
import requests as req



#Date Range
start_date = dt.date(1997, 1, 1).strftime('%Y-%m-%d')
# end_date = dt.date.today() #dt.timedelta(days=3)  # dt.date(2014, 12, 31)#start_date +
# date_list = pd.date_range(start_date, end_date)

#Variables to get
variables = ['PRCP', 'SNWD', 'TMAX', 'AWND']

#Zip Codes to query
zip_codes = NY_stations['Zip Code'].unique()
#len(zip_codes)
#header
headers = {'token': 'OSsaciGRPogGjCrRixolTOLVWLoUpReF'}

api_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&datatypeid=%s&locationid=ZIP:%s&startdate=%s'

raw_weather_data = pd.DataFrame()
variable = 'AWND'
zip_code = '10023'
for zip_code in zip_codes:
    for variable in varibles:
        url_instance = api_url % (varible, zip_code, start_date)
        response = req.get(url_instance, headers = headers)
responseTMAX = responseTMAX.json()
pd.DataFrame(responseTMIN['results'])
all_csvs = csv_instance.append(all_csvs, ignore_index = True)



# &enddate=2017-10-31&limit=500
# replace 'myToken' with the actual token, below



responseTMIN = req.get(urlTMIN, headers = headers)
responseTMIN = responseTMIN.json()

responseAWND = req.get(urlAWND, headers = headers)
responseAWND = responseAWND.json()
pd.read_json(type(responseTMIN))
pd.io.json.json_normalize(responseTMIN)



def reverse_geocode_weather_stations(NY_stations):
    ### Gets Zip Code and City for the weather stations ###
    NY_stations = NY_stations.set_index("Station")
    remaining_stations = NY_stations[pd.isnull(NY_stations['Zip Code'])]
    if len(remaining_stations) > 0:
        for station in remaining_stations.index.tolist():
            try:
                results = Geocoder.reverse_geocode(NY_stations.loc[station, 'Latitude'],
                                                   NY_stations.loc[station, 'Longitude'])
                # NY_stations.loc[station, 'Full Address'] = str(results)
                # NY_stations.loc[station, 'City'] = results.city
                NY_stations.loc[station, 'Zip Code'] = results.postal_code
                # NY_stations.loc[station, 'County'] = results.administrative_area_level_2
            except Exception as e:
                print(e)
    NY_stations = NY_stations.reset_index()
    NY_stations = NY_stations[['Station', 'Zip Code']]
    return NY_stations

def get_station_metadata():
    ### Get NY Weather Station Metadata ###
    try:
        NY_stations = pd.DataFrame(list(db.dim_station.find()))
        test = NY_stations['Station']
    except Exception as e:
        print(e)
        # print("Couldn't connect to DB. Getting the data from the source. This will take a few minutes.")
        station_url = 'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt'
        stations_df = pd.read_fwf(station_url,
                                  colspecs=[[0, 11], [12, 19], [21, 29], [31, 36], [37, 40], [41, 70], [72, 74],
                                            [76, 78], [80, 84], ],
                                  names=['Station', 'Latitude', 'Longitude', 'Elevation', 'State', 'Station Name',
                                         'GSN FLAG', 'HCN/CRN FLAG', 'WMO ID'],
                                  index_col=False)
        #Subset to NY State
        NY_stations = stations_df[stations_df['State'] == 'NY']
        # Remove & Add columns
        NY_stations = NY_stations[['Station', 'Latitude', 'Longitude']]
        NY_stations['Zip Code'] = np.nan #NY_stations['Full Address'], NY_stations['City'],, NY_stations['County'] , np.nan, np.nan, np.nan
    # Gets zip code and city
    NY_stations = reverse_geocode_weather_stations(NY_stations)
    # Insert into DB
    try:
        db.dim_station.drop()
        db.dim_station.insert_many(NY_stations.to_dict("records"))
    except Exception as e:
        print(e)
    return NY_stations

try:
    atlas = "mongodb://kylegilde:kyle1234!@data602-shard-00-00-lfo48.mongodb.net:27017,data602-shard-00-01-lfo48.mongodb.net:27017,data602-shard-00-02-lfo48.mongodb.net:27017/admin?ssl=true&authSource=admin"
    client = MongoClient(atlas)
    db = client.MTA_weather
except Exception as e:
    print("Couldn't connect to database.", e)
else:
    #Get historical weather data
    try:
        all_csvs = pd.DataFrame(list(db.all_csvs.find()))
        test = all_csvs['Station']
    except Exception as e:
        print(e)
        start = time.time()
        i = 0
        #Initialize variables
        n_years = 2
        current_year = dt.date.today().year
        i_year = current_year - n_years + 1
        all_csvs = pd.DataFrame()
        url = 'ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/%d.csv.gz'
        NY_stations = get_station_metadata()
        while i_year <= current_year:
            try:
                url_instance = url % i_year
                csv_instance = pd.read_csv(url_instance, compression='gzip',
                                     names = ['Station', 'Date', 'Attribute', 'Value', 'x', 'y', 'z'],
                                     index_col = False)
                #Add zip code & subset to NY
                csv_instance = csv_instance.merge(NY_stations, on='Station')
            except Exception as e:
                print(e)
            else:
                #Append DF
                all_csvs = csv_instance.append(all_csvs, ignore_index = True)
            #Timing
            i_year += 1
            i += 1
            lapsed = time.time() - start
            per_loop = lapsed / i
            print(i, lapsed/60, per_loop/60)
        lapsed = time.time() - start
        print(lapsed/60)
        ### Tidy ###
        #Keep only the relevant columns
        weather_df = all_csvs[['Station', 'Date', 'Zip Code', 'Attribute', 'Value']]
        # Variables to keep
        variables = ['PRCP', 'SNWD', 'TMAX', 'AWND']
        weather_df = weather_df[weather_df['Attribute'].isin(variables)]
        #Remove missing values
        weather_df = weather_df[weather_df['Value'] != 9999]
        #Convert int to date
        weather_df['Date'] = pd.to_datetime(weather_df['Date'].astype(str), format='%Y%m%d')
        #Pivot and take the mean by zip code
        weather_df = pd.pivot_table(weather_df, index=['Zip Code', 'Date'], values='Value',
                                      columns='Attribute')
        weather_df = weather_df.reset_index()
        # Add, rename & sort the columns
        weather_df['Day'], weather_df['Month'], weather_df['Year'] = weather_df['Date'].dt.day, \
                                                                     weather_df['Date'].dt.month,\
                                                                     weather_df['Date'].dt.year
        weather_df = weather_df.rename(columns={'PRCP': 'Precipitation',
                        'TMAX': 'Max Temperature',
                        'AWND': 'Average Wind Speed',
                        'SNWD': 'Snow Depth'})
        weather_df = weather_df[['Zip Code', 'Date', 'Year', 'Month', 'Day', 'Precipitation',
                                 'Max Temperature', 'Average Wind Speed', 'Snow Depth']]
        np.sum(pd.isnull(weather_df))
        len(weather_df)
        #Insert long-form data into DB
        try:
            db.weather_df.drop()
            db.weather_df.insert_many(weather_df.to_dict("records"))
        except Exception as e:
            print(e)
        #Calculate the extreme indices



    NY_stations.index
    weather_df.columns
    weather_df.shape
    weather_df.index
    weather_df.head()
    weather_df.tail()







    min_visibilitv = np.min(weather_df["Visibility"])
    max_visibilitv = np.max(weather_df["Visibility"])
    weather_df['Extreme Visibility Index'] = 1 - (weather_df["Visibility"] - min_visibilitv) / max_visibilitv

    all_csvs['Attribute'].value_counts()


all_csvs['Attribute'].value_counts()
all_csvs['Value'].value_counts()

np.sum(pd.isnull(all_csvs['Value']))
# Tidying
# tidy_weather["Average"] = tidy_weather["Average"].str.extract('(\d*\.?\d*)').astype(float)




NY_stations.columns
NY_stations['Station'].value_counts()





NY_stations.drop(['index', 'level_0'], axis=1, inplace = True)


NY_stations.shape

NY_stations.head()
NY_stations.tail()
np.max(csv_df["Date"])

db.weather_collection.insert_many(weather_df.to_dict("records"))
len('USR0000MCYC')

df['ids'].str.contains('ball', na = False)
csv_df.rename
zip_codes = zip_table.loc[zip_table[1] == 'New York', 0]



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

# !/usr/bin/env/python3
"""
Created on Mon Nov 20 14:17:39 2017
CUNY DATA602 Final Project
@author: Nkasi Nedd, Michael Muller & Kyle Gilde
"""
import os
import urllib.request
import datetime as dt
import numpy as np
import pandas as pd
from pymongo import MongoClient
from pygeocoder import Geocoder
import requests as req
import math
#stats packages
from sklearn import linear_model
import statsmodels.api as sm
import statsmodels.formula.api as smf
from sklearn.model_selection import train_test_split

import plotly
import plotly.plotly as py
import plotly.graph_objs as go
import cufflinks as cf

from matplotlib import pyplot as plt
import random

def get_ridership_data():
    """Loads or creates the ridership data by day, station and zip code"""
    try:
        # Attempt to load from MongoDB instance
        ridership_data = pd.DataFrame(list(db.ridership_data.find()))
        ridership_data = ridership_data[['Date', 'Station', 'Zip Code', 'Zip Code - 3 Digits', 'Entries', 'Exits',
                                         'Total Traffic']]
    except Exception as e:
        print(e, ': Getting new data')
        date = dt.datetime(2015, 1, 10)
        cwd = os.getcwd()
        # datetime.timedelta(days=1)
        url = 'http://web.mta.info/developers/data/nyct/turnstile/turnstile_'

        for attempt in range(99999):
            try:
                fileID = date.isoformat()[2:4] + date.isoformat()[5:7] + date.isoformat()[8:10]
                link = url + fileID + '.txt'
                urllib.request.urlretrieve(link, './turnstileData/turnstile_' + fileID + '.csv')
                date = date + dt.timedelta(weeks=1)
                print('Downloaded ' + str(fileID))
            except Exception as e:
                print(e)
                print(link)
                break
        files = os.listdir(cwd + '/turnstileData')
        masterDF = pd.DataFrame(columns=['DATE', 'STATION', 'ENTRIES', 'EXITS'])
        for dataFile in files[1:]:
            currentWeek = pd.read_csv(cwd + '/turnstileData/' + dataFile,
                                      names=['C/A', 'UNIT', 'SCP', 'STATION', 'LINENAME', 'DIVISION', 'DATE', 'TIME',
                                             'DESC', 'ENTRIES', 'EXITS'])
        datesInFile = currentWeek['DATE'].unique()
        stationsInFileToUse = currentWeek.loc[currentWeek['LINENAME'].isin(['2', '3', '4', '5', '6'])]
        # uniqueStations = list(stationsInFileToUse['STATION'].unique())
        print('All Dates in file are ')
        print(datesInFile)
        for dates in datesInFile[1:]:
            print(dates)
            narrowFrame = stationsInFileToUse.loc[stationsInFileToUse['DATE'] == dates]
            for station in stationsInFileToUse['STATION'].unique():
                narrowFrame2 = narrowFrame.loc[narrowFrame['STATION'] == station]
            stationEntries = []
            stationExits = []
            for scp in narrowFrame2['SCP'].unique():
                narrowerFrame = narrowFrame2.loc[narrowFrame2['SCP'] == scp]
                entries = narrowerFrame['ENTRIES'].max() - narrowerFrame['ENTRIES'].min()
                exits = narrowerFrame['EXITS'].max() - narrowerFrame['EXITS'].min()
                stationEntries.append(entries)
                stationExits.append(exits)
            stationEntries = pd.Series(stationEntries).sum()
            stationExits = pd.Series(stationExits).sum()
            row = pd.Series({'DATE': dates, 'STATION': station, 'ENTRIES': stationEntries, 'EXITS': stationExits})
            masterDF = masterDF.append(row, ignore_index=True)

        masterDF.to_csv('preliminary.csv')
        mix = pd.read_csv('zipcodes.csv', names=['STATION', 'Zip Code'], header=0)

        masterDF = pd.read_csv('preliminary.csv').drop(['Unnamed: 0'], axis=1)

        masterDF['STATION'].loc[masterDF['STATION'] == '148 ST-LENOX'] = 'HARLEM 148 ST'
        masterDF['STATION'].loc[masterDF['STATION'] == '138 ST-3 AVE'] = '3 AV 138 ST'
        masterDF['STATION'].loc[masterDF['STATION'] == 'E 143 ST'] = 'E 143/ST MARY\'S'
        masterDF['STATION'].loc[masterDF['STATION'] == 'E 177 ST-PARKCH'] = 'PARKCHESTER'
        masterDF['STATION'].loc[masterDF['STATION'] == 'DYRE AVE'] = 'EASTCHSTER/DYRE'

        differentNames = list(masterDF['STATION'].unique())

        for name in differentNames:
            print(name)
            mix['similarity'] = mix['STATION'].apply(lambda x: SequenceMatcher(None, name, x).ratio())
            consistentName = list(mix['STATION'].loc[mix['similarity'].max() == mix['similarity']])[0]
            masterDF['STATION'].loc[masterDF['STATION'] == name] = consistentName
        # print(list(mix['STATION'].loc[mix['similarity'].max()==mix['similarity']]))

        mix = mix.set_index('STATION')['Zip Code'].to_dict()
        masterDF['Zip Code'] = masterDF['STATION'].map(mix)
        masterDF.to_csv('Fixed.csv', index=False)
        ### End of turnstile acquisition and tidy :: All info in 'Fixed.csv'
        ###############################################################################################################
        # Read and append the 2 CSVs
        ridership_data = pd.read_csv('Fixed.csv', index_col=False)
        ridership_data.columns = ['Date', 'Station', 'Entries', 'Exits', 'Zip Code']
        ridership_data2 = pd.read_csv('2015Data.csv', index_col=False)
        ridership_data2 = ridership_data2[['DATE', 'STATION', 'ENTRIES', 'EXITS', 'Zip Code']]
        ridership_data2.columns = ['Date', 'Station', 'Entries', 'Exits', 'Zip Code']
        ridership_data = ridership_data.append(ridership_data2, ignore_index=True)
        #Munge data
        ridership_data['Total Traffic'] = ridership_data['Entries'] + ridership_data['Exits']
        ridership_data['Date'] = pd.to_datetime(ridership_data['Date'])
        ridership_data['Zip Code'] = ridership_data['Zip Code'].astype(str)
        ridership_data['Zip Code'].replace('4064', '04064', inplace=True)
        ridership_data['Zip Code - 3 Digits'] = ridership_data['Zip Code'].str[:3]
        # Remove bad data
        ridership_data = ridership_data[(ridership_data['Entries'] < 100000) & (ridership_data['Exits'] < 100000)]
        try:
            db.ridership_data.drop()
            db.ridership_data.insert_many(ridership_data.to_dict("records"))
        except Exception as e:
            print(e)
    return ridership_data

def reverse_geocode_zip_codes(NY_weather_stations):
    """Gets Zip Code and City for the weather stations"""
    NY_weather_stations = NY_weather_stations.set_index('Station ID')
    remaining_stations = NY_weather_stations[pd.isnull(NY_weather_stations['Zip Code'])]
    if len(remaining_stations) > 0:
        for station in remaining_stations.index.tolist():
            try:
                results = Geocoder.reverse_geocode(NY_weather_stations.loc[station, 'Latitude'],
                                                   NY_weather_stations.loc[station, 'Longitude'])
                # NY_weather_stations.loc[station, 'City'] = results.city
                NY_weather_stations.loc[station, 'Zip Code'] = results.postal_code
            except Exception as e:
                print(e)
    NY_weather_stations = NY_weather_stations.reset_index()
    return NY_weather_stations


def get_weather_station_metadata(get_new_data=False):
    """Get NY Weather Station Metadata"""
    try:
        if get_new_data:
            a = 1 / 0
        # Attempt to load from MongoDB instance
        NY_weather_stations = pd.DataFrame(list(db.dim_station.find()))
        test = NY_weather_stations['Station ID']
    except Exception as e:
        print(e, ': Getting new data')
        ### Initialize or recreate this data set. Write to MongoDB instance ###
        station_url = 'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt'
        stations_df = pd.read_fwf(station_url,
                                  colspecs=[[0, 11], [12, 19], [21, 29], [31, 36], [37, 40], [41, 70], [72, 74],
                                            [76, 78], [80, 84], ],
                                  names=['Station ID', 'Latitude', 'Longitude', 'Elevation', 'State', 'Station Name',
                                         'GSN FLAG', 'HCN/CRN FLAG', 'WMO ID'],
                                  index_col=False)
        # Subset to NY State
        NY_weather_stations = stations_df[stations_df['State'] == 'NY']
        # Remove & Add columns
        NY_weather_stations = NY_weather_stations[['Station ID', 'Latitude', 'Longitude']]
        NY_weather_stations['Zip Code'], NY_weather_stations['City'] = np.nan, np.nan
        # Gets zip code and city
        NY_weather_stations = reverse_geocode_zip_codes(NY_weather_stations)
        NY_weather_stations['Zip Code - 3 Digits'] = NY_weather_stations['Zip Code'].str[:3]
        # Insert into DB
        try:
            db.dim_station.drop()
            db.dim_station.insert_many(NY_weather_stations.to_dict("records"))
        except Exception as e:
            print(e)
    return NY_weather_stations


def create_MTA_weather_df(get_new_data=False):
    """" Pulls weather data from API and combines with MTA ridership data """
    try:
        if get_new_data:
            a = 1 / 0
        MTA_weather_df = pd.DataFrame(list(db.MTA_weather_df.find()))
        MTA_weather_df = MTA_weather_df[
            ['Date', 'Station', 'Zip Code', 'Year', 'Month', 'Day', 'Day of Week', 'Is Weekday',
             'Entries', 'Exits', 'Total Traffic', 'Max Temperature (C)', 'Precipitation (mm)',
             'Snow Depth (mm)', '# Max Temp STDs', '# Precipitation STDs', '# Snow Depth STDs',
             'Mean # of Absolute STDs']]
        # Transform to categoricals
        MTA_weather_df['Day'], MTA_weather_df['Month'], MTA_weather_df['Year'], MTA_weather_df['Day of Week'], \
        MTA_weather_df['Is Weekday'] = \
            pd.Categorical(MTA_weather_df['Day'], ordered=True), \
            pd.Categorical(MTA_weather_df['Month'], ordered=True), \
            pd.Categorical(MTA_weather_df['Year'], ordered=True), \
            pd.Categorical(MTA_weather_df['Day of Week'], ordered=True), \
            pd.Categorical(MTA_weather_df['Is Weekday'], ordered=True)

    except Exception as e:
        print(e, ': Getting new data')

        ridership_data = get_ridership_data()
        NY_weather_stations = get_weather_station_metadata()
        # Initialize some constants & API parameters
        n_years = 30
        api_limit_per_call = 1000
        variables = ['PRCP', 'SNWD', 'TMAX']  # Variables to get
        # Initialize API parameters
        unique_zip_codes = ridership_data['Zip Code - 3 Digits'].unique()
        needed_weather_stations = NY_weather_stations[NY_weather_stations['Zip Code - 3 Digits'].isin(unique_zip_codes)]
        days_per_api_call = math.floor(api_limit_per_call / len(variables)) - 1  # max per call
        time_periods = int(math.ceil(n_years * 365 / days_per_api_call))
        start_date = dt.date.today() - dt.timedelta(days=n_years * 365)
        end_date = min(start_date + dt.timedelta(days=days_per_api_call), dt.date.today() - dt.timedelta(days=6))
        str_start_date, str_end_date = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
        headers = {'token': 'ljMPWeEPzzUNldzSpRogHqqEgkTFeVYf'}
        api_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&datatypeid=%s&datatypeid=%s&datatypeid=%s&stationid=GHCND:%s&startdate=%s&enddate=%s&limit=%d'
        raw_weather = pd.DataFrame()

        # Takes 5-10 minutes to execute
        for time_period in range(time_periods):
            for idx in needed_weather_stations.index.tolist():
                try:
                    parameters = (
                        *variables, needed_weather_stations.loc[idx, "Station ID"], str_start_date, str_end_date,
                        api_limit_per_call)
                    api_call = api_url % parameters
                    get_response = req.get(api_call, headers=headers)
                    response_to_json = get_response.json()
                    df_instance = pd.DataFrame(response_to_json['results'])
                    df_instance['Zip Code - 3 Digits'] = needed_weather_stations.loc[idx, 'Zip Code - 3 Digits']
                except Exception as e:
                    print('No results:', e)
                else:
                    raw_weather = df_instance.append(raw_weather, ignore_index=True)
            start_date = end_date + dt.timedelta(1)
            end_date = min(start_date + dt.timedelta(days=days_per_api_call), dt.date.today() - dt.timedelta(days=6))
            str_start_date, str_end_date = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
        # Pivot and take the mean values
        raw_weather['Date'] = pd.to_datetime(raw_weather['date'])
        weather_df = pd.pivot_table(raw_weather, index=['Zip Code - 3 Digits', 'Date'], values='value',
                                    columns='datatype').dropna()
        weather_df = weather_df.reset_index()
        # Add, rename & transform columns
        weather_df['Day'], weather_df['Month'], weather_df['Year'] = pd.Categorical(weather_df['Date'].dt.day,
                                                                                    ordered=True), \
                                                                     pd.Categorical(weather_df['Date'].dt.month,
                                                                                    ordered=True), \
                                                                     pd.Categorical(weather_df['Date'].dt.year,
                                                                                    ordered=True)
        weather_df['Day of Week'] = pd.Categorical(weather_df['Date'].dt.dayofweek + 1, ordered=True)
        weather_df['Is Weekday'] = pd.Categorical((weather_df['Day of Week'] < 6).astype(int))

        # From tenths of degrees C to degrees C
        weather_df['Max Temperature (C)'] = weather_df['TMAX'] / 10
        # From tenths of mm to mm
        weather_df['Precipitation (mm)'] = weather_df['PRCP'] / 10
        weather_df = weather_df.rename(columns={'SNWD': 'Snow Depth (mm)'})

        # Calculate Means by Calendar Day
        weather_df['Max Temperature Calendar-Day Mean'] = weather_df.groupby(['Month', 'Day'])[
            'Max Temperature (C)'].transform('mean')
        weather_df['Precipitation Calendar-Day Mean'] = weather_df.groupby(['Month', 'Day'])[
            'Precipitation (mm)'].transform('mean')
        weather_df['Snow Depth Calendar-Day Mean'] = weather_df.groupby(['Month', 'Day'])['Snow Depth (mm)'].transform(
            'mean')
        # Calculate the STDs by Calendar Day
        weather_df['Max Temperature Calendar-Day STD'] = weather_df.groupby(['Month', 'Day'])[
            'Max Temperature (C)'].transform('std')
        weather_df['Precipitation Calendar-Day STD'] = weather_df.groupby(['Month', 'Day'])[
            'Precipitation (mm)'].transform('std')
        weather_df['Snow Depth Calendar-Day STD'] = weather_df.groupby(['Month', 'Day'])['Snow Depth (mm)'].transform(
            'std')
        # Normalize metrics by calculating the # of STDs
        weather_df['# Max Temp STDs'] = (weather_df['Max Temperature (C)'] - weather_df[
            'Max Temperature Calendar-Day Mean']) / weather_df['Max Temperature Calendar-Day STD']
        weather_df['# Precipitation STDs'] = (weather_df['Precipitation (mm)'] - weather_df[
            'Precipitation Calendar-Day Mean']) / weather_df['Precipitation Calendar-Day STD']
        weather_df['# Snow Depth STDs'] = (
            (weather_df['Snow Depth (mm)'] - weather_df['Snow Depth Calendar-Day Mean']) / weather_df[
                'Snow Depth Calendar-Day STD']).fillna(0)
        weather_df['Mean # of Absolute STDs'] = abs(weather_df['# Max Temp STDs']) + abs(
            weather_df['# Precipitation STDs']) + abs(weather_df['# Snow Depth STDs'])
        # Merge to create final DF
        MTA_weather_df = ridership_data.merge(weather_df, on=['Zip Code - 3 Digits', 'Date'])
        # Drop & re-order some of the columns
        MTA_weather_df = MTA_weather_df[
            ['Date', 'Station', 'Zip Code', 'Year', 'Month', 'Day', 'Day of Week', 'Is Weekday',
             'Entries', 'Exits', 'Total Traffic', 'Max Temperature (C)', 'Precipitation (mm)',
             'Snow Depth (mm)', '# Max Temp STDs', '# Precipitation STDs', '# Snow Depth STDs',
             'Mean # of Absolute STDs']]
        # Insert into DB
        try:
            db.MTA_weather_df.drop()
            db.MTA_weather_df.insert_many(MTA_weather_df.to_dict("records"))
        except Exception as e:
            print(e)
    return MTA_weather_df


try:
    atlas = "mongodb://kylegilde:kyle1234!@data602-shard-00-00-lfo48.mongodb.net:27017,data602-shard-00-01-lfo48.mongodb.net:27017,data602-shard-00-02-lfo48.mongodb.net:27017/admin?ssl=true&authSource=admin"
    client = MongoClient(atlas)
    db = client.MTA_weather
except Exception as e:
    print("Couldn't connect to database:", e)
else:
    MTA_weather_df = create_MTA_weather_df()
    MTA_weather_df.info()
    random.seed(888)
    X = MTA_weather_df[['Month', 'Is Weekday', 'Max Temperature (C)', 'Precipitation (mm)', 'Snow Depth (mm)']]
    y = MTA_weather_df[['Total Traffic']]
    X.columns = ['Month', 'Is_Weekday', 'Max_Temperature_C', 'Precipitation_mm', 'Snow_Depth_mm']
    y.columns = ['Total_Traffic']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
    df_train = pd.concat([X_train, y_train], axis=1)
#  These variables weren't statistically significant: Snow_Depth_mm +  C(Month)
    sm_model = smf.ols(formula="Total_Traffic ~ Precipitation_mm + C(Is_Weekday)  + Max_Temperature_C",
                       data=df_train).fit()
    sm_model.summary()
    predictions = sm_model.predict(X_test)  # make the predictions by the model
    RMSE = np.sqrt(np.sum((predictions - y_test['Total_Traffic']) ** 2) / len(y_test))
    print('Root Mean Squared Error:', RMSE)

    plt.scatter(y_test, predictions)
    plt.xlabel('True Values')
    plt.ylabel('Predictions')

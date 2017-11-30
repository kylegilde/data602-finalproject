# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 18:54:56 2017

@author: Exped
"""

import pandas as pd
import os

cwd = os.getcwd()
files = os.listdir(cwd)


#DataFrames = ats(actual train movement) / stops(info on locations) / gtfs(actual scheduled arrival/departure times)

stops = pd.read_csv('stops17.txt').drop(['stop_code','stop_desc','parent_station','location_type','stop_url','zone_id'],axis=1)
ats = pd.read_csv('ATS-Data_2011-05-21.csv').drop(['track_id'],axis=1)

gtfs = pd.read_csv('stop_times.txt').drop(['shape_dist_traveled','drop_off_type','pickup_type','stop_headsign'],axis=1)
gtfs['train'] = gtfs['trip_id'].astype(str).str[20]
gtfs = gtfs.loc[gtfs['train'].isin(['2','3','4','5','6'])]
#gtfs = gtfs.loc[gtfs['train'].isin(['1'])]

#ats.loc[ats['route_id'].isin(['1'])]


for theDataFile in files:
    if 'ATS-Data_2011-05-01' in theDataFile:
        theDate = theDataFile[9:19] #Actual date; not included in timestamp
        atsTemp = pd.read_csv(theDataFile)
        pass
        
        
#All the schedules should be the same

#split ats by trackID


ourStops = gtfs['stop_id'].unique()

stopsInQuestion = stops.loc[stops['stop_id'].isin(list(map(str,ourStops)))]
stopsInQuestion['Zip Code'] = None
stopsInQuestion = stopsInQuestion.rename(columns={'stop_id':'Station ID','stop_lat':'Latitude','stop_lon':'Longitude'})
stopsInQuestion['stop_name'] = stopsInQuestion['stop_name'].str.upper()
from pygeocoder import Geocoder
def reverse_geocode_zip_codes(NY_stations):
    ### Gets Zip Code and City for the weather stations ###
    NY_stations = NY_stations.set_index('Station ID')
    remaining_stations = NY_stations[pd.isnull(NY_stations['Zip Code'])]
    if len(remaining_stations) > 0:
        for station in remaining_stations.index.tolist():
            try:
                results = Geocoder.reverse_geocode(NY_stations.loc[station, 'Latitude'],
                                                   NY_stations.loc[station, 'Longitude'])
                NY_stations.loc[station, 'City'] = results.city
                NY_stations.loc[station, 'Zip Code'] = results.postal_code
            except Exception as e:
                print(e)
    NY_stations = NY_stations.reset_index()
    return NY_stations

files = os.listdir(cwd+'/dataDownloads')
#sumting = Geocoder.geocode(list(exp['STATION'].unique())[1]) # Use this to put zipcodes on master DF afterwards
exp= pd.read_csv(cwd+'/dataDownloads/turnstile_160109.csv')
exp = exp.loc[exp['LINENAME'].isin(['2','3','4','5','6'])]

#list(stopsInQuestion['stop_name'])
#list(exp['STATION'].unique())
masterDF = pd.DataFrame(columns=['DATE','STATION','ENTRIES','EXITS'])
for dataFile in files[1:]:
    currentWeek = pd.read_csv(cwd+'/dataDownloads/'+dataFile,names=['C/A', 'UNIT', 'SCP', 'STATION', 'LINENAME', 'DIVISION', 'DATE', 'TIME',
       'DESC', 'ENTRIES', 'EXITS'])
    datesInFile = currentWeek['DATE'].unique()
    stationsInFileToUse = currentWeek.loc[currentWeek['LINENAME'].isin(['2','3','4','5','6'])]
    #uniqueStations = list(stationsInFileToUse['STATION'].unique())
    print('All Dates in file are ')
    print(datesInFile)
    for dates in datesInFile[1:]:
        for station in stationsInFileToUse['STATION'].unique():
            narrowFrame = stationsInFileToUse.loc[stationsInFileToUse['STATION']==station]
            stationEntries = []
            stationExits = []
            for scp in narrowFrame['SCP'].unique():
                narrowerFrame = narrowFrame.loc[narrowFrame['SCP']==scp]
                entries =narrowerFrame['ENTRIES'].max() - narrowerFrame['ENTRIES'].min()
                exits = narrowerFrame['EXITS'].max() - narrowerFrame['EXITS'].min()
                stationEntries.append(entries)
                stationExits.append(exits)
            stationEntries = pd.Series(stationEntries).sum()
            stationExits = pd.Series(stationExits).sum()
            row = pd.Series({'DATE':dates,'STATION':station,'ENTRIES':stationEntries,'EXITS':stationExits})
            masterDF = masterDF.append(row,ignore_index=True)
                
                
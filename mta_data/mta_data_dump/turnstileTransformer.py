# -*- coding: utf-8 -*-
"""
Created on Wed Nov 29 13:42:02 2017

@author: Exped
"""

from pygeocoder import Geocoder
import pandas as pd
import os
import urllib
import json

#sumting = Geocoder.geocode(list(exp['STATION'].unique())[1]) # Use this to put zipcodes on master DF afterwards
cwd = os.getcwd
files = os.listdir(os.getcwd())

ats = pd.read_csv('preliminary.csv')

geoinfo = pd.read_csv('geoinfo.csv')

#ats['Zip Code'] = Geocoder.geocode(ats['STATION']).postal_code

listOfAddresses = list(ats['STATION'].unique())

finalZipcodes = pd.DataFrame(geoinfo['input_string'])
finalZipcodes['Zip Code'] = geoinfo['postcode']
finalZipcodes.ix[22,'Zip Code'] = 10467


mix = pd.read_csv('zipcodes.csv',names=['STATION','Zip Code'],header=0)
mix = mix.set_index('STATION')['Zip Code'].to_dict()

ats['Zip Code'] = ats['STATION'].map(mix)
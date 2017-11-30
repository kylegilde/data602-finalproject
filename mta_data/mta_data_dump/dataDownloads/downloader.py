# -*- coding: utf-8 -*-
"""
Created on Wed Nov 29 13:05:14 2017

@author: Exped
"""
#http://web.mta.info/developers/data/nyct/turnstile/turnstile_160423.txt
#Saturday, April 23, 2016
import datetime
import urllib.request
date = datetime.datetime(2016,4,23)
#datetime.timedelta(days=1)
url = 'http://web.mta.info/developers/data/nyct/turnstile/turnstile_'
for attempt in range(99999):
    try:
        fileID = date.isoformat()[2:4]+date.isoformat()[5:7]+date.isoformat()[8:10]
        link = url+fileID+'.txt'
        urllib.request.urlretrieve(link, 'turnstile_'+fileID+'.csv')
        date = date + datetime.timedelta(weeks=1)
    except Exception as e:
        print(e)
        print(link)
        break
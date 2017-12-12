# data602-finalproject
CUNY Data Science
Nkasi, Michael &amp; Kyle's Advanced Programming Final Project

# Introduction
Our project takes a look at the age-old question of how weather affects the number of people trying to get from one place to another. Specifically, we will be looking for statistical evidence of whether 3 types of conditions - precipitation, temperature and snow depth - are predictive of the amount of ridership occurring on the North America’s largest transportation network, the Metropolitan Transportation Authority (MTA).

# Overview of Functions

get_ridership_data()
"""Loads or creates the ridership data by day, station and zip code"""

reverse_geocode_zip_codes(NY_weather_stations)
"""Gets Zip Code and City for the weather stations"""

get_weather_station_metadata(get_new_data=False)
"""
It first trys to pull the already-processed data from the MongoDB.
Otherwise, it gets NY Weather Station Metadata from text file.
"""

create_MTA_weather_df(get_new_data=False)
"""
It first trys to pull the already-processed data from the MongoDB.
If get_new_data=True or if the DB connection fails, then it pulls weather data from API and combines with MTA ridership data.
"""




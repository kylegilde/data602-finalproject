# data602-finalproject
CUNY Data Science
Nkasi, Michael &amp; Kyle's Advanced Programming Final Project

## Introduction
Our project takes a look at the age-old question of how weather affects the number of people trying to get from one place to another. Specifically, we will be looking for statistical evidence of whether 3 types of conditions - precipitation, temperature and snow depth - are predictive of the amount of ridership occurring on the North America’s largest transportation network, the Metropolitan Transportation Authority (MTA).

## Overview of Functions

### get_ridership_data()
Loads or creates the ridership data by day, station and zip code

### reverse_geocode_zip_codes(NY_weather_stations)
Gets Zip Code and City for the weather stations

### get_weather_station_metadata(get_new_data=False)
It first trys to pull the already-processed data from the MongoDB.
If get_new_data=True or if the DB connection fails, it gets NY Weather Station Metadata from text file.

### create_MTA_weather_df(get_new_data=False)
It first trys to pull the already-processed data from the MongoDB.
If get_new_data=True or if the DB connection fails, then it pulls weather data from API and combines with MTA ridership data.

## Accessing the project

The project can be accessed in the following ways:

1.  Via hosted environment on EC2 - http://54.84.167.234:80

The password to be used is: 3525113c943c470643c92905d81d3fcbae8b309e0d52d3e9

2.  Via Docker image found here: https://hub.docker.com/r/parastyle/finalbook/

This image can be pulled from within Docker with the command: docker pull parastyle/finalbook:firsttry
Then run with the command: sudo docker run -p 80:8888 parastyle/finalbook:firsttry
This will launch jupyter notebook.  To access the notebook you would then need to use the brower to access the address of the docker environment and enter the token provided.

3.  Via building and runing the docker image

First the repo must be cloned, then the docker image built where the current directory is the cloned repository directory.  The image built can then be run with a similar command to the one used in (2) above, replacing 'parastyle/finalbook:firsttry' with the name of the image built.


# data602-Assignment2

# CUNYMSDA-DATA602 Assignment 1

## Submitted by:

**N. Nedd** 

# Web Trader 3.1

This project seeks to implement a web based trading system that allows for the purchase of shares, sale of shares, Profit/Loss tracking of shares, calculation of Weighted Average Price of Shares and the Share Blotter.

The system allows for the trading of all shares specified by NASDAQ

## Getting Started


### Prerequisites

To run the software you need to have docker on your system, or you can use the Amazon EC2 AMI instance which includes docker.


### Installing

To install simply pull the docker image from dockerhub (https://hub.docker.com/r/nnedd/trader-web3.1/).  In docker enter the following:

```
docker pull nnedd/trader-web3.1
```

or if in Linux

```
sudo docker pull nnedd/trader-web3.1
```

Then run the docker image

```
docker run -p 5000:5000 nnedd/trader-web3.1
```

or

```
sudo docker run -p 5000:5000 nnedd/trader-web3.1
```




## Deployment

The docker hub image was built using Docker Toolbox on Windows 10 with the following command run while in the directory containing the DockerFile (in this repository)
Please note that to build the docker image, Python 3 will also need to installed on the system.

```
docker build --no-cache -t trader-web3.1
```

then retagged to reflect the username with:

```
docker tag trader-web.0 nnedd/trader-web3.1
```

then pushed to docker hub (after loggin in using docker login):

```
docker push nnedd/trader-web3.1
```

The above commands should also work for linux machines if the sudo keyword is placed before each command.
Please note that in order to complete the image creation I temporarily made this github repository public

References:

https://benalexkeen.github.io/C3PyO/
http://c3js.org/


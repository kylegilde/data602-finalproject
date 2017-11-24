from bs4 import BeautifulSoup
import urllib.request as req
url = 'ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/all/USR0000MCYC.dly'
page = req.urlopen(url)
soup = BeautifulSoup(page, 'html.parser')
date_box = soup.find('span', attrs={'id': 'qwidget_markettime'})
date = date_box.text
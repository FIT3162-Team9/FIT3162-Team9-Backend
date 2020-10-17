import requests
import csv
from bs4 import BeautifulSoup


"""
Extract zip download link from BoM site 
"""

#link variable
stationcodemax = '1'
staioncodemin = '2'

"""
Return URL_LINK for all weather stations
"""
def url_link(stationcode, temperaturetype):
    result = requests.get(
        "http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=" + temperaturetype + "&p_display_type=dailyDataFile&p_startYear=&p_c=&p_stn_num=" + stationcode)

    src = result.content
    soup = BeautifulSoup(src)
    links = soup.find_all('a')

    for link in links:
        if "All years" in link.text:
            return(link.attrs['href'])

def writecsv(filename,reader):
    with open('url_' + filename[:-4] + '.csv', 'w', newline='') as f:
        fieldnames = ['url_' + filename[:-4]]
        thewriter= csv.DictWriter(f, fieldnames=fieldnames)
        thewriter.writeheader()

        for row in reader:
            thewriter.writerow({"url_" + filename[:-4]:  url_link(row[0], '122')})
            thewriter.writerow({"url_" + filename[:-4]: url_link(row[0], '123')})

def readcsv(filename):
    with open(filename) as file:
        reader = csv.reader(file)
        writecsv(filename, reader)



readcsv('NSW.csv')

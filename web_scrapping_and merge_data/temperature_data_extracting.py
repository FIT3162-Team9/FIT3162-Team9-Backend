import requests, zipfile, io, os, csv

"""
Download and extract zip file containing temperature data of all years with url link input
"""
state = "NSW"

directory = 'C:/Users/Jacky/PycharmProjects/untitled/WeatherStation/' + state

def extract(f):
    stations = []
    for line in f:

        list = line.split()
        if len(list) == 0:
                break
        download(list[0][:-1])
        #trigger link to download zip file

    return stations

def download(url_link):

    r = requests.get(url_link)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall("C:/Users/Jacky/PycharmProjects/untitled/WeatherStation/" + state)

for filename in os.listdir(directory):
    if filename.endswith(".csv"):
        f = open(filename, 'r')

        filename = filename[:-4]

        extract(f)

        f.close()
        continue
    else:
        continue

quit()



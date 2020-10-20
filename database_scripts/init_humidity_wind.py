import firebase_admin
import csv
import datetime

from firebase_admin import credentials, auth, firestore

cred = credentials.Certificate("fit3162-team9-firebase-adminsdk-e9u3s-54f8dacbb5.json")
firebase_admin.initialize_app(cred)

db = firebase_admin.firestore.client(app=None)

lga_data = {}
limit = 20000
lim_count = 0


def to_snake_case(string):
    if string == "":
        return 'none'

    return "_".join(string.replace('/', "_").lower().split(' '))


with open('../data/processed_humidity_windspeed_data.csv') as file:
    reader = csv.reader(file, delimiter=",")
    for i, row in enumerate(reader):
        if i == 0:
            print(i, row)
        else:
            if lim_count % 1000 == 0:
                print('----- added {} rows -----'.format(lim_count))

            location = row[6]
            location_str = to_snake_case(location)

            print(location_str, end=" ")

            if row[2] == "" or row[3] == "" or row[4] == "":
                continue

            day = int(float(row[0]))
            month = int(float(row[1]))
            year = int(float(row[2]))
            humidity = float(row[3])
            windspeed = float(row[7])

            lim_count += 1

            year_str = '{:04d}'.format(year)
            month_str = "{:02d}".format(month)
            day_str = '{:02d}'.format(day)
            date_str = '{}{}{}'.format(year_str, month_str, day_str)

            element = datetime.datetime(int(year), int(month), int(float(day)))
            timestamp = int(datetime.datetime.timestamp(element))
            # print('{} - {} {} {} ({}) : {} {}'.format(station_num, year_str, month_str,
            #                                           day_str, timestamp, max_temp, min_temp))

            entry = {'day': day, 'month': month, 'year': year, 'timestamp': timestamp, 'humidity': humidity, 'windspeed': windspeed}
            # print(station_num, date_str, entry)
            # temperature_data[station_num] = entry

            if location_str in lga_data:
                lga_data[location_str][date_str] = entry
            else:
                lga_data[location_str] = {date_str: entry}

                # temperature_data[station_num] = {date_str: entry}
            # print(i, row)
            # if row[5] != '':
            # date_formatted = '{}-{}-{}'.format(row[2], row[3], row[4])
            # max_temp[date] = {'value': float(row[5]), 'date': date_formatted}

for lga, all_entries in lga_data.items():
    lga_ref = db.collection('data').document('humidity_wind').collection(lga)

    print(lga)
    for date_str, entry in all_entries.items():
        doc_ref = lga_ref.document(date_str)
        doc_ref.set(entry)
        print(lga, date_str, entry)

# for date, value in max_temp.items():
#     print(date, value)
#     doc_ref = station_ref.document(date)
#     doc_ref.set(value)

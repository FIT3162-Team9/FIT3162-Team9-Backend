import firebase_admin
import csv
import datetime

from firebase_admin import credentials, auth, firestore

cred = credentials.Certificate("fit3162-team9-firebase-adminsdk-e9u3s-54f8dacbb5.json")
firebase_admin.initialize_app(cred)

db = firebase_admin.firestore.client(app=None)

temperature_data = {}
limit = 20000

existing_stations = [76031, 76047, 76064]

with open('../temperature_data/vic_temperature_data.csv') as file:
    reader = csv.reader(file, delimiter=",")
    for i, row in enumerate(reader):
        if i == 0:
            print(i, row)
        else:
            if i == limit:
                break

            station_num = int(float(row[1]))
            year = int(float(row[2]))
            month = int(float(row[3]))
            day = int(float(row[4]))
            max_temp = float(row[5])
            min_temp = float(row[9])

            station_str = str(station_num)
            year_str = '{:04d}'.format(year)
            month_str = "{:02d}".format(month)
            day_str = '{:02d}'.format(day)
            date_str = '{}{}{}'.format(year_str, month_str, day_str)

            element = datetime.datetime(int(year), int(month), int(float(day)))
            timestamp = int(datetime.datetime.timestamp(element))
            # print('{} - {} {} {} ({}) : {} {}'.format(station_num, year_str, month_str,
            #                                           day_str, timestamp, max_temp, min_temp))

            entry = {'day': day, 'month': month, 'year': year, 'timestamp': timestamp, 'min': min_temp, 'max': max_temp}
            # print(station_num, date_str, entry)
            # temperature_data[station_num] = entry

            if station_str in temperature_data:
                temperature_data[station_str][date_str] = entry
            else:
                temperature_data[station_str] = {date_str: entry}

                # temperature_data[station_num] = {date_str: entry}
            # print(i, row)
            # if row[5] != '':
            # date_formatted = '{}-{}-{}'.format(row[2], row[3], row[4])
            # max_temp[date] = {'value': float(row[5]), 'date': date_formatted}


for station, all_entries in temperature_data.items():
    station_ref = db.collection('data').document('temperature').collection(station)

    print(station)
    for date_str, entry in all_entries.items():
        doc_ref = station_ref.document(date_str)
        doc_ref.set(entry)
        # print(station, date_str, entry)


# for date, value in max_temp.items():
#     print(date, value)
#     doc_ref = station_ref.document(date)
#     doc_ref.set(value)

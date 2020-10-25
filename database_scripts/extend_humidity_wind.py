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


base_day = 26
base_month = 9
base_year = 2019


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

            # print(location_str, end=" ")

            if row[2] == "" or row[3] == "" or row[4] == "":
                continue

            day = int(float(row[0]))
            month = int(float(row[1]))
            year = int(float(row[2]))
            humidity = float(row[3])
            windspeed = float(row[7])

            lim_count += 1

            entry_datetime = datetime.datetime(int(year), int(month), int(float(day)))

            extension_datetime_base = datetime.datetime(int(base_year), int(base_month), int(base_day))

            if entry_datetime > extension_datetime_base:

                year += 1   # OVERWRITE YEAR
                print(int(year), int(month), int(float(day)))
                try:
                    new_datetime = datetime.datetime(int(year), int(month), int(float(day)))
                except ValueError as e:
                    print('ERROR ---', e)
                    continue

                year_str = '{:04d}'.format(year)
                month_str = "{:02d}".format(month)
                day_str = '{:02d}'.format(day)
                date_str = '{}{}{}'.format(year_str, month_str, day_str)

                timestamp = int(datetime.datetime.timestamp(new_datetime))

                entry = {'day': day, 'month': month, 'year': year, 'timestamp': timestamp, 'humidity': humidity, 'windspeed': windspeed}

                if location_str in lga_data:
                    lga_data[location_str][date_str] = entry
                else:
                    lga_data[location_str] = {date_str: entry}



for lga, all_entries in lga_data.items():
    lga_ref = db.collection('data').document('humidity_wind').collection(lga)

    print(lga)
    for date_str, entry in all_entries.items():
        doc_ref = lga_ref.document(date_str)
        doc_ref.set(entry)
        # print(lga, date_str, entry)


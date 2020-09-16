import firebase_admin
import csv
import datetime

from firebase_admin import credentials, auth, firestore

cred = credentials.Certificate("fit3162-team9-firebase-adminsdk-e9u3s-54f8dacbb5.json")
firebase_admin.initialize_app(cred)
db = firebase_admin.firestore.client(app=None)

station_details = {}
lga_station_mapping = {}


def to_snake_case(string):
    if string == "":
        return 'none'

    return "_".join(string.replace('/', "_").lower().split(' '))


def write_state_data(state):

    with open('../temperature_data/{}_temperature_data.csv'.format(state)) as file:
        reader = csv.reader(file, delimiter=",")
        for i, row in enumerate(reader):
            if i == 0:
                print(i, row)
            else:
                station_num = int(float(row[1]))
                station_str = str(station_num)

                name = row[12].strip()
                lga = row[13].strip()
                latitude = float(row[14])
                longitude = float(row[15])

                if station_str not in station_details:
                    station_details[station_str] = {
                        'name': name.title(),
                        'lga': to_snake_case(lga),
                        'number': station_str,
                        'latitude': latitude,
                        'longitude': longitude,
                    }

    for key, value in station_details.items():
        print(value)
        lga = value['lga']
        details = dict(value)
        del details['lga']

        if lga in lga_station_mapping:
            lga_station_mapping[lga].append(details)
        else:
            lga_station_mapping[lga] = [details]

    for lga, stations in lga_station_mapping.items():
        print(lga, stations)
        try:
            lga_ref = db.collection('lga').document(state).collection('lga_to_stations').document(lga)
            lga_ref.set({'array': stations})
        except Exception as e:
            print(e)


if __name__ == '__main__':
    state_array = ['vic', 'nsw', 'qld', 'sa', 'nt', 'wa', 'tas']
    for state_str in state_array:
        print(state_str, '-----------------------')
        write_state_data(state_str)

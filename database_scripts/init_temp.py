import firebase_admin
import csv

from firebase_admin import credentials, auth, firestore

cred = credentials.Certificate("fit3162-team9-firebase-adminsdk-e9u3s-54f8dacbb5.json")
firebase_admin.initialize_app(cred)

db = firebase_admin.firestore.client(app=None)
station_ref = db.collection('temperature').document('max').collection('IDCJAC0010')

max_temp = {}

with open('IDCJAC0010_1006_1800_Data.csv') as file:
    reader = csv.reader(file, delimiter=",")
    for i, row in enumerate(reader):
        if i == 0:
            print(i, row)
        else:
            if row[5] != '':
                # print(i, '{}{}{}: {}'.format(row[2], row[3], row[4], row[5]))
                date = '{}{}{}'.format(row[2], row[3], row[4])
                date_formatted = '{}-{}-{}'.format(row[2], row[3], row[4])
                max_temp[date] = {'value': float(row[5]), 'date': date_formatted}

print(max_temp)

for date, value in max_temp.items():
    print(date, value)
    doc_ref = station_ref.document(date)
    doc_ref.set(value)

import sys
from tracemalloc import Snapshot
sys.path.append(".")

from lib import apicredentials
import firebase_admin
from firebase_admin import credentials,firestore,db

import pandas as pd


import time
import json

cred = credentials.Certificate('.//keys//key-firebase.json')
# Initialize the app with a service account, granting admin privileges
firebase_admin.initialize_app(cred, {
    'databaseURL': "https://twitter-e939f-default-rtdb.firebaseio.com/"
})


ref=db.reference('object')
docs= ref.get()
# print(dir(ref.get()))
print(type(docs.items()))
df = pd.DataFrame.from_dict(docs.items())
# for i in docs.items():
#     print(i)
#     print('----------------------')
df




# bearer_token = apicredentials.BEARER_TOKEN
# ref = db.reference('object')
# ref_doc = db.collections('object')
# def listener(event):
#     # print(event.event_type)  # can be 'put' or 'patch'
#     # print(event.path)  # relative to the reference, it seems
#     # print(event.data)
#     #
#     # print("-----------------")
#     # new data at /reference/event.path. None if deleted
#     print(event.event_type)  # can be 'put' or 'patch'
#     print(event.path)  # relative to the reference, it seems
#     print(event.data)  # new data at /reference/event.path. None if deleted



# # Create an app with listening data from event. If data be push into real-time database, it can be listen by listener envent
# ref.get()

# db.reference('/').listen(listener)
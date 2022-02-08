import json
from bson import ObjectId

import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client['study']

collection = db['users']
doc = {
"name" : "Khal Drogo",
"email":"khaldrogo@gmail.com",
"password" : "$2b$12$7tgpVkBxUqQiYFkHjZyoMuEzFU5BSI.FYkOXHu4zCRlRsa15sHQo6"

}

rec = collection.insert_one(doc)
print(rec)

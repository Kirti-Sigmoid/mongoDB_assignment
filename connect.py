import json
from bson import ObjectId

import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")


db = client['study']

collection = db['comments']
item_list = []

with open('comments.json') as f:
    for json_obj in f:
        if json_obj:
            my_dict = json.loads(json_obj)
            my_dict["_id"] = ObjectId(my_dict["_id"]["$oid"])
            item_list.append(my_dict)

#collection.insert_many(item_list)
print(collection.find_one())
print(item_list[3])

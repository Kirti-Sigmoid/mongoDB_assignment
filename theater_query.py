import json
from bson import ObjectId

import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
def theatres_nearby_given_coordinates(collections,coord):
    dic={}
    for i in collections.find():

        cord_data = i['location']['geo']['coordinates']
        x = float(coord[0]) - float(cord_data[0]["$numberDouble"])
        y = float(coord[1]) - float(cord_data[1]["$numberDouble"])
        x = round(x*x + y*y,5)
        if dic.get(x):
            dic[x].append(i['theaterId'])
        else:
            dic[x]=[]
            dic[x].append(i['theaterId'])
    a = dict(sorted(dic.items()))
    ans = []
    for k,v in a.items():
        ans += v
        if len(ans)+len(v)>10:
            x=10-len(ans)
            ans += v[0:x]
        else:
            ans += v
        if len(ans)>=10:
            break
    return ans
db = client['study']
collection = db['theaters']
# Top 10 cities with the maximum number of theatres
res = collection.aggregate(
    [
        {"$group": {"_id": "$location.address.city","total": {"$sum": 1}}},
        {"$sort": {"total": -1}},
       {"$limit": 10}
    ]
)
for i in res:
    print(i)
# top 10 theatres nearby given coordinates
# cor1=input("Enter Coordinate 1")
# core2=input("Enter coordinate 2")
nearby_theatre = theatres_nearby_given_coordinates(collection,['-93.24565', '44.85466'])
print("Top 10 theatres nearby given coordinates eg: ['-93.24565', '44.85466']")
print(nearby_theatre)

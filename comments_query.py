import json
from bson import ObjectId
from datetime import datetime

import pymongo

def Print(result):
    for movie in result:
        print(movie)
def maxNumberComment(colleaction):
    result = collection.aggregate([
        {"$group": {"_id": {"email": "$email"},
                    "name": {"$first": "$name"},
                    "total": {"$sum": 1}}},
        {"$sort": {"total": -1}},
        {"$limit": 10}])
    Print(result)
def mostCommented(collection,collection2):
    res2 = collection.aggregate([
        {"$group": {
            "_id": "$movie_id",
            "total": {"$sum": 1}
        }
        },
        {"$sort": {"total": -1}},
        {"$limit": 10},
    ])
    List=list(res2)
    movie=[]
    for i in range(0,len(List)):
        obj=ObjectId(List[i]['_id']['$oid'])
        movie.append(obj)
    for i in movie:
        val=collection2.find({"_id":i},{"_id":0,"title":1})
        Print(val)
def total_number_of_comment_in_year(collection,given_year):
    dic= {  "01":0,"02":0,  "03": 0, "04": 0,"05": 0,"06": 0,"07": 0,"08": 0, "09": 0,"10": 0,"11": 0,"12": 0
    }
    for i in collection.find():
        dte = (i['date']['$date']['$numberLong'])
        datetime_obj = datetime.strptime(dte,  "%d%m%y%H%M")

        date = datetime_obj.date()

        x = str(date)
        yr = x[0:4]
        mo = x[5:7]
        if(yr==given_year):
            dic[mo] +=1




client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client['study']

collection = db['comments']
collection2 = db['movies']
maxNumberComment(collection)
# Find top 10 movies with most comments
mostCommented(collection,collection2)
# Allcomment in the given year
year=input("Enter year :")
print("All comment in the year",year)
total_number_of_comment_in_year(collection,year)


# top_10= collection.aggregate([
#
#     {"$group": {"_id": { "movies_id": "$movie_id"},"total_comment":{"$sum":1}}},
#     {"$sort": {"total_comment": -1}},
#     {"$project": {"movies_id": 1,"total_comment":1}},
#     {"$limit": 10}
# ])
#
# mov=list(top_10)
# mov_id=[]
# for i in range(0,len(mov)):
#     oid=mov[i]['_id']['movies_id']['$oid']
#     obj=ObjectId(oid)
#     mov_id.append(obj)
#
# collection1=db.movies
#
# for id1 in mov_id:
#     val=collection1.find({"_id":id1},{"_id":0,"title":1})
#     for title in val:
#         print(title)
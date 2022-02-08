import json
from bson import ObjectId

import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client['study']

def Print(result):
    for movie in result:
        print(movie)

def highestIMDBrating(n, collection):
    res = collection.find({"imdb.rating": {"$ne": ""}}, {"_id": 0, "title": 1, "imdb.rating": 1}).sort("imdb.rating",-1).limit(n)
    Print(res)

def highestIMDBinYear(collection,year,n):
    result = collection.find({"year.$numberInt": {"$eq": year}},
                             {"_id": 0, "title": 1, "year": 1, "imdb.rating": 1}).sort("imdb.rating", -1).limit(n)
    Print(result)

def numberofVotesgreater(collection,n):
    result = collection.find({"imdb.votes.$numberInt": {"$gte": 1000}}, {"_id": 0, "title": 1, 'imdb.votes': 1}).sort("imdb.votes",
                                                                                                          -1).limit(n)
    Print(result)

def titlematching(collection,patteren,n):
    res = collection.find({"title": {"$regex": patteren}},
                          {"_id": 0, "title": 1, "year": 1, "tomatoes.viewer.rating": 1}).sort("tomatoes.viewer.rating",
                                                                                               -1).limit(n)
    Print(res)
def maxMovies(collection,n):
    result2 = collection.aggregate([
            {"$unwind":"$directors"},
            {"$group":{"_id":{"director":"$directors"},"num_of_movies":{"$sum":1}}},
            {"$sort":{"num_of_movies":-1}},
            {"$limit":n}
         ])
    Print(result2)
def maxMovieInYear(collection,year,n):
    result1 = db.movies.aggregate([
            {"$unwind":"$directors"},
            {"$match":{"year.$numberInt":year}},
            {"$group":{"_id":{"director":"$directors"},"num_of_movies":{"$sum":1}}},
            {"$sort":{"num_of_movies":-1}},
            {"$limit":n}
         ])
    Print(result1)
def maxMovieInGivenGenre(collection,genre,n):
    result2 = db.movies.aggregate([
        {"$unwind": "$directors"},
        {"$match": {"genres": genre}},
        {"$group": {"_id": {"director": "$directors"}, "num_of_movies": {"$sum": 1}}},
        {"$sort": {"num_of_movies": -1}},
        {"$limit": n}
    ])
    Print(result2)
def actorHasmaxMovies(collection,n):
    result2 = collection.aggregate([
            {"$unwind":"$cast"},
            {"$group":{"_id":{"Actor":"$cast"},"num_of_movies":{"$sum":1}}},
            {"$sort":{"num_of_movies":-1}},
            {"$limit":n}
         ])
    Print(result2)
def actorMaxMovieInYear(collection,year,n):
    result1 = db.movies.aggregate([
            {"$unwind":"$cast"},
            {"$match":{"year.$numberInt":year}},
            {"$group":{"_id":{"Actor":"$cast"},"num_of_movies":{"$sum":1}}},
            {"$sort":{"num_of_movies":-1}},
            {"$limit":n}
         ])
    Print(result1)
def ActormaxMovieInGivenGenre(collection,genre,n):
    result2 = db.movies.aggregate([
        {"$unwind": "$cast"},
        {"$match": {"genres": genre}},
        {"$group": {"_id": {"Actor": "$cast"},"num_of_movies": {"$sum": 1}}},
        {"$sort": {"num_of_movies": -1}},
        {"$limit": n}
    ])
    Print(result2)


collection = db['movies']

def moviesForEachGenre(collection,genre,n):
    result = collection.find({"genres": genre}, {"_id": 0, "title": 1, "imdb.rating": 1}).sort("imdb.rating", -1).limit(
        n)
    Print(result)


n = 5
n = int(input("Enter the value of n :"))

# Find top `N` movies  with the highest IMDB rating
highestIMDBrating(n, collection)

# Find top `N` movies with the highest IMDB rating in a given year

year=input("Enter the year :")
highestIMDBinYear(collection,year,n)

#Find top `N` movies with highest IMDB rating with number of votes > 1000
numberofVotesgreater(collection,n)

#Find top `N` movies with title matching a given pattern sorted by highest tomatoes ratings
patteren=input("Enter the patteren :")
titlematching(collection,patteren,n)

# find top N director  who created the maximum number of movies

maxMovies(collection,n)

#find top N director who created the maximum number of movies in a given year
year=input("Enter the Year :")

maxMovieInYear(collection,year,n)

# find top N director who created the maximum number of movies for a given genre
genre=input("Enter the genre :")
maxMovieInGivenGenre(collection,genre,n)

#Find top `N` actors who starred in the maximum number of movies
actorHasmaxMovies(collection,n)

#find top N actor who created the maximum number of movies in a given year
year=input("Enter the Year :")
actorMaxMovieInYear(collection,year,n)


# find top N actor who created the maximum number of movies for a given genre
genre=input("Enter the genre :")
ActormaxMovieInGivenGenre(collection,genre,n)

# Find top `N` movies for each genre with the highest IMDB rating

result=collection.aggregate([
    {"$unwind":"$genres"},
    {"$project":{"_id":0,"genre":"$genres"}}
])
distinct=set()
for i in result:
    distinct.add(i.get("genre"))
for i in distinct:
    print("The genre of the movie is : ",i)
    moviesForEachGenre(collection,i,n)




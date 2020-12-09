# %%
import pandas as pd
from datetime import datetime
from ClassMongo import Database_mongodb

database = Database_mongodb(db_name="exp")

# %%
database.insert("test", data={"name": "david", "casa": "carabayllo"})
# %%

database.insert("test", data=[{"name": "david", "casa": "carabayllo"}, {
                "name": "david2", "casa": "carabayllo"}], many=True)

# %%
data_ = [{"_id": 7, "author": "dave", "score": 80, "views": 100},
         {"_id": 6,
          "author": "dave", "score": 85, "views": 521},
         {"_id": 5,
          "author": "ahn", "score": 60, "views": 1000},
         {"_id": 4,
          "author": "li", "score": 55, "views": 5000},
         {"_id": 3,
          "author": "annT", "score": 60, "views": 50},
         {"_id": 2,
          "author": "li", "score": 94, "views": 999},
         {"_id": 1,
          "author": "ty", "score": 95, "views": 1000},
         ]

database.insert("aggr", data=data_, many=True)
# %%
"""
The $match selects the documents where the author field 
equals li, and the aggregation returns the following:
"""
data = database.agg("aggr", pipeline=[{"$match": {"author": "li"}}])
print(list(data))

# %%
data = database.find("aggr", query={"score": {"$gt": 60}}, many=True)
print(list(data))

# %%
# Query using data time


def strtime(x): return datetime.strptime(x, '%Y-%m-%dT%H:%M:%S')


datawtime = [
    {"_id": 1, "item": "abc", "price": 10, "quantity": 2,
        "date": strtime("2014-01-01T08:00:00")},
    {"_id": 2, "item": "jkl", "price": 20, "quantity": 1,
        "date": strtime("2014-02-03T09:00:00")},
    {"_id": 3, "item": "xyz", "price": 5, "quantity": 5,
        "date": strtime("2014-02-03T09:05:00")},
    {"_id": 4, "item": "abc", "price": 10, "quantity": 10,
        "date": strtime("2014-02-15T08:00:00")},
    {"_id": 5, "item": "xyz", "price": 5, "quantity": 10,
        "date": strtime("2014-02-15T09:05:00")},
]

# %%
database.insert("datadate", datawtime, many=True)

# %%
# Group by ONE field, months, then aggregare data  like avg of price and sum of price
agroup = database.agg("datadate", pipeline=[
    {"$group": {
        "_id": {"$month": "$date"},
        "sum_price": {"$sum": "$price"},
        "avg_price": {"$avg": "$price"}
    }}
])

agroup.head()
# %%
# Group by TWO fields, items and month of the year
agroup = database.agg("datadate", pipeline=[
    {"$group": {
        "_id": {"item": "$item", "month": {"$month": "$date"}},
        "sum_price": {"$sum": "$price"},
        "avg_price": {"$avg": "$price"}
    }}
])
agroup.head()

# %%
# Getting unique values
unique = database.unique("datadate", "item")


print(unique)

# %%
# Using proyect: to extract exact information from the collection
database.changedb("dbcontam")
unique = database.agg("trafico", pipeline=[
    {
        "$project": {
            "lectura": 1,
            "idtramo": 1,
            "month": {"$month": "$Fecha"}
        }
    },
    {
        "$match": {"month": 3}
    },
    {
        "$group": {"_id": {"month": "$month", "idtramo": "$idtramo"},
                   "avg_lecture": {"$avg": "$lectura"}}
    }
])
unique


# %%
def strdate(x): return datetime.strptime(x, '%Y-%m-%d')


databooks = [
    {
        "_id": 1,
        "title": "abc123",
        "isbn": "0001122223334",
        "author": {"last": "zzz", "first": "aaa"},
        "copies": 5,
        "lastModified": strdate("2016-07-28")
    },
    {
        "_id": 2,
        "title": "Baked Goods",
        "isbn": "9999999999999",
        "author": {"last": "xyz", "first": "abc", "middle": ""},
        "copies": 2,
        "lastModified": strdate("2017-07-21")
    },
    {
        "_id": 3,
        "title": "Ice Cream Cakes",
        "isbn": "8888888888888",
        "author": {"last": "xyz", "first": "abc", "middle": "mmm"},
        "copies": 5,
        "lastModified": strdate("2017-07-22")
    },
]
database.insert("books", data=databooks, many=True)

# %%
database.agg("books", pipeline=[
    {
        "$project": {
            "title": 1,
            "author.first": 1,
            "author.last": 1,
        }
    }
])

# %%
unique = database.agg("trafico", pipeline=[
    {
        "$match": {"$expr": {"$eq": [{"$month": "$Fecha"}, 3]}}
    },
    {
        "$project": {
            "lectura": 1,
            "idtramo": 1,
            "month": {"$month": "$Fecha"}
        }
    },

    {
        "$group": {"_id": {"month": "$month", "idtramo": "$idtramo"},
                   "avg_lecture": {"$avg": "$lectura"}}
    }
])
unique
# %%
locadata = [
    {
        "loc": {"type": "Point", "coordinates": [-73.99279, 40.719296]},
        "name": "Central Park 3",
        "category": "Parks"
    },
    {
        "loc": {"type": "Point", "coordinates": [-73.88, 40.78]},

        "name": "La Guardia Airport2",
        "category": "Airport"
    },
    {
        "loc": {"type": "LineString", "coordinates": [[-73.88, 40.78], [-73.8832, 40.78434], [-73.99279, 40.719296]]},

        "name": "La Guardia Airport david",
        "category": "Airport"
    }
]
database.changedb("exp")
database.insert("locadata", data=locadata, many=True)
# %%
database.createIndex("locadata", "loc", "2dsphere")


# %%
value = database.agg("locadata", pipeline=[
    {
        "$geoNear": {
            "near": {"type": "Point", "coordinates": [-73.8801, 40.78]},
            "distanceField": "dist.calculated",
            "maxDistance": 1000,
            "query": {"category": "Airport"},
            "includeLocs": "dist.location.point",
            "spherical": True,
        }
    },
    {"$limit": 1}
], df=False)
print(list(value))


# %%
# database.changedb("dbcontam")
# database.createIndex("estaciones", "geopoint.coordinates", "2d",) # UTM no es espherical
# database.createIndex("espiras_trafico", "espiras.coordinates", "2d") # UTM no es espherical

# %%
database.changedb("dbcontam")
value = database.agg("locadata", pipeline=[
    {
        "$geoNear": {
            "near": {"type": "Point", "coordinates": [-73.8801, 40.78]},
            "distanceField": "dist.calculated",
            "maxDistance": 1000,
            "query": {"category": "Airport"},
            "includeLocs": "dist.location.point",
            "spherical": True,
        }
    },
    {"$limit": 1}
], df=False)
print(list(value))

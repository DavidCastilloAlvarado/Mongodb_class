# Mongo DB Class

Mongodb class for python, the class implement the most usefull methods in mongo db.

- Insert
- - Many = True, False
- Find
- - Many = True, False
- Delete
- - Many = True, False
- Aggregate
- - df = True, False
- Index

### Compatible with:

- python 3.7 and later
- pymongo 3.11.2
- Pandas 1.1.4

# Quickstart

1. Installl MongoDB on your local machine or MongoDB Atlas

- Donwload Mongo [Community Server ](https://www.mongodb.com/try/download/community)
- Create an account in [Mongo Atlas](https://www.mongodb.com/cloud/atlas)

2. Clone this repo

```shell
$git clone https://github.com/DavidCastilloAlvarado/Mongodb_class.git
$cd Mongodb_class
```

3. Initialize this class on your proyect

```python
from ClassMongo import Database_mongodb
URL = "---url mongodb or mongoatlas---"
db_name= "mydb" # your database, if doesn't exist then will be created
mydb = Database_mongodb(URL, db_name)
...
...
mydb.insert(mycollection, {"param1": 2, "param2":3}, many=False)
```

# Examples

### **Insert**: insert(self, collection, data, many=False)

---

collection: string type, collection name,<br>
data: dict type or list of dicts, data to store in the collection <br>
many: bool, true is you going to send a bunch of documents, false if you only going to send one document

```python
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

database.insert("collecBooks", data=data_, many=True)

```

### **Agregate**: agg (self, collection, pipeline, df=True)

---

collection: string type, collection name, <br>
pipeline: list of dicts type, what is your pipeline to aggregate data <br>
df : bool , return like a dataframe or just a dict

```python
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
database.insert("datadate", datawtime, many=True)
agroup = database.agg("datadate", pipeline=[
    {"$group": {
        "_id": {"$month": "$date"},
        "sum_price": {"$sum": "$price"},
        "avg_price": {"$avg": "$price"}
    }}
])
```

### **Find**: find (self, collection, query={}, many=False)

---

collection: string type, collection name,<br>
query: dict type, what are you going to reques <br>
many: bool, true is you going to request a bunch of documents,\n false if you only going to request one document

```python
data = database.find("collecBooks", query={"score": {"$gt": 60}}, many=True)
print(list(data))

```

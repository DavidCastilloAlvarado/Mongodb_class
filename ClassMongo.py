from pymongo import MongoClient, GEO2D
import pandas as pd


class Database_mongodb(object):
    """
    URL: string type, database direccion, could be MongoAtlas \n
    db_name: string type, name of de data base\n
    """

    def __init__(self, URL="mongodb://localhost", db_name="init"):
        """
        URL: string type, database direccion, could be MongoAtlas \n
        db_name: string type, name of de data base\n
        """
        self.db_name = db_name
        self.cluster = MongoClient(URL)
        self.db = self.cluster[db_name]

    def changedb(self, new_db):
        """
        new_db: string type, name of another database to change the operations
        """
        self.db_name = new_db
        self.db = self.cluster[new_db]

    def insert(self, collection, data, many=False):
        """
        collection: string type, collection name,\n
        data: dict type or list of dicts, data to store in the collection \n
        many: bool, true is you going to send a bunch of documents,\n false if you only going to send one document 
        """
        if many:
            self.db[collection].insert_many(data)
        else:
            self.db[collection].insert(data)

    def find(self, collection, query={}, many=False):
        """
        collection: string type, collection name,\n
        query: dict type, what are you going to reques \n
        many: bool, true is you going to request a bunch of documents,\n false if you only going to request one document 
        """
        if many:
            return pd.DataFrame(self.db[collection].find(query))
        else:
            return self.db[collection].find_one(query)

    def delete(self, collection, query={}, many=False):
        if many:
            self.db[collection].delete_many(query)
        else:
            self.db[collection].delete_one(query)

    def agg(self, collection, pipeline, df=True):
        """
        collection: string type, collection name,\n
        pipeline: list of dicts type, what is your pipeline to aggregate data \n
        df : bool , return like a dataframe or just a dict
        """
        return pd.DataFrame(self.db[collection].aggregate(pipeline)) if df else self.db[collection].aggregate(pipeline)

    def unique(self, collection, field, distinct=None):
        """
        collection: string type, name of the coollection \n
        distinct: dict, especific distintion for uniques, {"field":"mystring"}
        """
        if distinct is None:
            return self.db[collection].distinct(field)
        else:
            return self.db[collection].distinct(field, distinct)

    def createIndex(self, collection, field, Indextype):
        self.db[collection].create_index([(field, Indextype)])

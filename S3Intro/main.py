from pprint import pprint as pp
from s3 import S3
import pymongo


# connect to the mongodb client and return the db
def get_mongo_db(client_str):
    client = pymongo.MongoClient(client_str)
    db = client.Sparta
    return db


# inserts the df into the given db document
def insert_df_to_db(dataframe, db, document):
    db[document].insert_many(dataframe.to_dict("records"))


if __name__ == "__main__":

    s3_instance = S3("data-eng-resources")  # INIT S3 class object
    df = s3_instance.merge_dfs(s3_instance.read_all_csv())  # read and merge the df of different csv
    final_df = s3_instance.round_all(s3_instance.group_and_avg(df))  # group, avg, and round the values in the merged df
    final_df = final_df.reset_index()  # remove "Species" as the index
    #print(final_df)

    database = get_mongo_db("mongodb://3.71.86.26:27017/Sparta")  # INIT db
    database.fishmarket.drop()
    database.create_collection('fishmarket')
    insert_df_to_db(final_df, database, 'fishmarket')  # insert final df into the collection

    #print(database.list_collection_names())
    cursor = database.get_collection('fishmarket')
    c = cursor.find()

    for doc in c:
        pp(doc)

    my_collection = database.fishmarket
    total_count = my_collection.count_documents({})
    print("Total number of documents : ", total_count)






from bson.objectid import ObjectId
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://user:user1@cluster0.prmzp9p.mongodb.net/"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

db = client.database01  

# add records to database
def add_records_to_data_base():
    try:
        db.cats.insert_one({
            "name": 'barsik',
            "age": 3,
            "features": ['ходить в капці', 'дає себе гладити', 'рудий'],
        })
        print("Successfully added")

        db.cats.insert_one({
            "name": 'barsik1',
            "age": 3,
            "features": ['ходить в капці', 'дає себе гладити', 'рудий'],
        })
        print("Successfully added")

        db.cats.insert_one({
            "name": 'tima',
            "age": 5,
            "features": ['ходить в капці', 'дає себе гладити', 'рудий'],
        })
        print("Successfully added")

    except Exception as e:
        print("Error during cat adding to database:", e)

# get all records from database
def get_all_records_from_data_base():
    try:
        all_cats = list(db.cats.find())
        if all_cats:
            print(all_cats)
        else:
            print("Database is empty")
    except Exception as e:
        print("Error during cat seeking:", e)

# get record from database by name
def get_record_in_data_base_by_name(name):
    try:
        cat = list(db.cats.find({"name": name}))
        if cat:
            print(cat)
        else:
            print(f"Cat with name {name} is not found")
    except Exception as e:
        print("Error during cat seeking:", e)
        
# update age record by name
def update_age_by_name(age, name):
    try:
        our_cat = list(db.cats.find({"name": name}))
        if our_cat:
            cat_id = our_cat[0]["_id"]
            db.cats.update_one(
                {"_id": ObjectId(cat_id)},
                {
                    "$set": {
                    "name": our_cat[0]["name"],
                    "age": age,
                    "features": our_cat[0]["features"],
                    }
                })
            print("Successfully updated")
        else:
           print(f"Cat with name {name} is not found") 
    except Exception as e:
        print("Error during cat updating:", e)

#udpate features record by name
def update_features_by_name(new_feature, name):
    try:
        if our_cat:
            our_cat = list(db.cats.find({"name": name}))
            cat_id = our_cat[0]["_id"]
            all_features = our_cat[0]["features"]
            all_features.append(new_feature)
            db.cats.update_one(
                {"_id": ObjectId(cat_id)},
                {
                    "$set": {
                    "name": our_cat[0]["name"],
                    "age": our_cat[0]["age"],
                    "features": all_features,
                    }
                })
            print("Successfully updated")
        else:
           print(f"Cat with name {name} is not found")
    except Exception as e:
        print("Error during cat updating:", e)

# delete record by name
def delete_one_by_name(name):
    try:
        db.cats.delete_one({"name": name})
    except Exception as e:
        print("Error during cat deleting:", e)

# delete all records
def delete_all_records():
    try:
        db.cats.delete_many({})
    except Exception as e:
        print("Error during cat deleting:", e)

def main():
    try:
        # Send a ping to confirm a successful connection
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")

        # adding records
        #add_records_to_data_base()

        # getting all records
        #get_all_records_from_data_base()

        # get record by name
        #get_record_in_data_base_by_name('tima')

        # update age by name
        #update_age_by_name(105, 'tima')
        #get_record_in_data_base_by_name('tima')

        # update features by name
        #update_features_by_name('nice', 'tima')
        #get_record_in_data_base_by_name('tima')

        # delete one by name
        #delete_one_by_name('barsik1')
        #get_all_records_from_data_base()

        # delete all records
        #delete_all_records()

    except Exception as e:
        print(e)

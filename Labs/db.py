import os
import pymongo
import certifi
from urllib import parse
from pprint import pprint
from dotenv import load_dotenv
from models import Product


load_dotenv()

DB_USER = os.environ.get("DB_USER")
DB_PW = os.environ.get("DB_PW")
CONNECTION_STRING = os.environ.get("DB_CONNECTION_STRING")

# username and password must be escaped according to RFC 3986
user = parse.quote_plus(DB_USER)
passwd = parse.quote_plus(DB_PW)
client = pymongo.MongoClient(CONNECTION_STRING.format(user, passwd), tlsCAFile=certifi.where())
# Create database to hold cosmic works data
# MongoDB will create the database if it does not exist
db = client.cosmic_works

# CREATE A COLLECTION
collection = db.products

# CREATE A DOCUMENT
product = Product(
        id="2BA4A26C-A8DB-4645-BEB9-F7D42F50262E",
        category_id="56400CF3-446D-4C3F-B9B2-68286DA3BB99",
        category_name="Bikes, Mountain Bikes",
        sku="BK-M18S-42",
        name="Mountain-100 Silver, 42",
        description='The product called "Mountain-500 Silver, 42"',
        price=742.42,
)

# Generate JSON using alias names defined on the model
product_json = product.model_dump(by_alias=True)

# Insert the JSON into the database, and retrieve the inserted/generated ID
product_id = collection.insert_one(product_json).inserted_id

print(f"Inserted product with ID: {product_id}")


# READ A DOCUMENT
retrieved_document = collection.find_one({"_id": product_id})

# Print the retrieved JSON document
print("JSON document retrieved from the database:")
pprint(retrieved_document)

# Cast JSON document into the Product model
retrieved_product = Product(**retrieved_document)

# Print the retrieved product
print("\nCast Product from document:")
print(retrieved_product)


# UPDATE A DOCUMENT
retrieved_product.name = "Mountain-100 Silver, 48\""
update_result = collection.find_one_and_update(
    {"_id": product_id},
    {"$set": {"name": retrieved_product.name}},
    return_document=pymongo.ReturnDocument.AFTER
)
print("Updated JSON document:")
print(update_result)
updated_product = Product(**update_result)
print(f"\nUpdated Product name: {updated_product.name}")


# DELETE A DOCUMENT
delete_result = collection.delete_one({"_id": product_id})
print(f"Deleted documents count: {delete_result.deleted_count}")
print(f"Number of documents in the collection: {collection.count_documents({})}")


# QUERY FOR MULTIPLE DOCUMENTS
# Insert multiple documents
products = [
    Product(
        id="2BA4A26C-A8DB-4645-BEB9-F7D42F50262E",
        category_id="56400CF3-446D-4C3F-B9B2-68286DA3BB99",
        category_name="Bikes, Mountain Bikes",
        sku="BK-M18S-42",
        name="Mountain-100 Silver, 42",
        description='The product called "Mountain-500 Silver, 42"',
        price=742.42
    ),
    Product(
        id="027D0B9A-F9D9-4C96-8213-C8546C4AAE71",
        category_id="26C74104-40BC-4541-8EF5-9892F7F03D72",
        category_name="Components, Saddles",
        sku="SE-R581",
        name="LL Road Seat/Saddle",
        description='The product called "LL Road Seat/Saddle"',
        price=27.12
    ),
    Product(
        id = "4E4B38CB-0D82-43E5-89AF-20270CD28A04",
        category_id = "75BF1ACB-168D-469C-9AA3-1FD26BB4EA4C",
        category_name = "Bikes, Touring Bikes",
        sku = "BK-T44U-60",
        name = "Touring-2000 Blue, 60",
        description = 'The product called Touring-2000 Blue, 60"',
        price = 1214.85
    ),
    Product(
        id = "5B5E90B8-FEA2-4D6C-B728-EC586656FA6D",
        category_id = "75BF1ACB-168D-469C-9AA3-1FD26BB4EA4C",
        category_name = "Bikes, Touring Bikes",
        sku = "BK-T79Y-60",
        name = "Touring-1000 Yellow, 60",
        description = 'The product called Touring-1000 Yellow, 60"',
        price = 2384.07
    ),
    Product(
        id = "7BAA49C9-21B5-4EEF-9F6B-BCD6DA7C2239",
        category_id = "26C74104-40BC-4541-8EF5-9892F7F03D72",
        category_name = "Components, Saddles",
        sku = "SE-R995",
        name = "HL Road Seat/Saddle",
        description = 'The product called "HL Road Seat/Saddle"',
        price = 52.64,
    )
]

# The bulk_write method takes a list of write operations and executes them in the same transaction
# The UpdateOne operation updates a single document, notice the upsert=True option, this means that
# if the document does not exist, it will be created, if it does exist, it will be updated
collection.bulk_write([pymongo.UpdateOne({"_id": prod.id}, {"$set": prod.model_dump(by_alias=True)}, upsert=True) for prod in products])

# Print all documents that have a category name of "Components, Saddles"
for result in collection.find({"categoryName": "Components, Saddles"}):
    pprint(result)

# Print all documents that have a category name that includes the word "Bikes"
for result in collection.find({"categoryName": {"$regex": "Bikes"}}):
    pprint(result)


# CLEAN UP RESOURCES
# db.drop_collection("products")
client.drop_database("cosmic_works")
client.close()
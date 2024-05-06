import os
import pymongo
import time
import json
import certifi
from urllib import parse
from openai import AzureOpenAI
from dotenv import load_dotenv
from tenacity import retry, wait_random_exponential, stop_after_attempt


load_dotenv()

# load settings
DB_USER = os.environ.get("DB_USER")
DB_PW = os.environ.get("DB_PW")
CONNECTION_STRING = os.environ.get("DB_CONNECTION_STRING")
EMBEDDINGS_DEPLOYMENT_NAME = "text-embedding-3-small"
COMPLETIONS_DEPLOYMENT_NAME = "gpt-4"
AOAI_ENDPOINT = os.environ.get("AOAI_ENDPOINT")
AOAI_KEY = os.environ.get("AOAI_KEY")
AOAI_API_VERSION = "2023-05-15"

# connect to DB
user = parse.quote_plus(DB_USER)
passwd = parse.quote_plus(DB_PW)
db_client = pymongo.MongoClient(CONNECTION_STRING.format(user, passwd), tlsCAFile=certifi.where())
# Create database to hold cosmic works data
# MongoDB will create the database if it does not exist
db = db_client.cosmic_works

# connect to Azure OpenAI
ai_client = AzureOpenAI(
    azure_endpoint = AOAI_ENDPOINT,
    api_version = AOAI_API_VERSION,
    api_key = AOAI_KEY
)

# vectorize and store the embeddings in each document
@retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(3))
def generate_embeddings(text: str):
    '''
    Generate embeddings from string of text using the deployed Azure OpenAI API embeddings model.
    This will be used to vectorize document data and incoming user messages for a similarity search with
    the vector index.
    '''
    response = ai_client.embeddings.create(input=text, model=EMBEDDINGS_DEPLOYMENT_NAME)
    embeddings = response.data[0].embedding
    time.sleep(0.5) # rest period to avoid rate limiting on AOAI
    return embeddings

# demonstrate embeddings generation using a test string
test = "good night, world"
print(generate_embeddings(test))

# Vectorize and update all documents in the Cosmic Works database
def add_collection_content_vector_field(collection_name: str):
    '''
    Add a new field to the collection to hold the vectorized content of each document.
    '''
    collection = db[collection_name]
    bulk_operations = []
    for doc in collection.find(no_cursor_timeout=True, batch_size=1):
        # remove any previous contentVector embeddings
        if "contentVector" in doc:
            del doc["contentVector"]

        # generate embeddings for the document string representation
        content = json.dumps(doc, default=str)
        content_vector = generate_embeddings(content)

        bulk_operations.append(pymongo.UpdateOne(
            {"_id": doc["_id"]},
            {"$set": {"contentVector": content_vector}},
            upsert=True
        ))
    # execute bulk operations
    collection.bulk_write(bulk_operations)

# Add vector field to products documents - this will take approximately 3-5 minutes due to rate limiting
add_collection_content_vector_field("products")

# Add vector field to customers documents - this will take approximately 1-2 minutes due to rate limiting
add_collection_content_vector_field("customers")

# Add vector field to customers documents - this will take approximately 15-20 minutes due to rate limiting
add_collection_content_vector_field("sales")

# Create the products vector index
db.command({
    'createIndexes': 'products',
    'indexes': [
      {
        'name': 'VectorSearchIndex',
        'key': {
          "contentVector": "cosmosSearch"
        },
        'cosmosSearchOptions': {
          'kind': 'vector-ivf',
          'numLists': 1,
          'similarity': 'COS',
          'dimensions': 1536
        }
      }
    ]
})

# Create the customers vector index
db.command({
    'createIndexes': 'customers',
    'indexes': [
      {
        'name': 'VectorSearchIndex',
        'key': {
          "contentVector": "cosmosSearch"
        },
        'cosmosSearchOptions': {
          'kind': 'vector-ivf',
          'numLists': 1,
          'similarity': 'COS',
          'dimensions': 1536
        }
      }
    ]
})

# Create the sales vector index
db.command({
    'createIndexes': 'sales',
    'indexes': [
      {
        'name': 'VectorSearchIndex',
        'key': {
          "contentVector": "cosmosSearch"
        },
        'cosmosSearchOptions': {
          'kind': 'vector-ivf',
          'numLists': 1,
          'similarity': 'COS',
          'dimensions': 1536
        }
      }
    ]
})

# Use vector search in vCore-based Azure Cosmos DB for MongoDB
def vector_search(collection_name, query, num_results=3):
    """
    Perform a vector search on the specified collection by vectorizing
    the query and searching the vector index for the most similar documents.

    returns a list of the top num_results most similar documents
    """
    collection = db[collection_name]
    query_embedding = generate_embeddings(query)
    pipeline = [
        {
            '$search': {
                "cosmosSearch": {
                    "vector": query_embedding,
                    "path": "contentVector",
                    "k": num_results
                },
                "returnStoredSource": True }},
        {'$project': { 'similarityScore': { '$meta': 'searchScore' }, 'document' : '$$ROOT' } }
    ]
    results = collection.aggregate(pipeline)
    return results

def print_product_search_result(result):
    '''
    Print the search result document in a readable format
    '''
    print(f"Similarity Score: {result['similarityScore']}")
    print(f"Name: {result['document']['name']}")
    print(f"Category: {result['document']['categoryName']}")
    print(f"SKU: {result['document']['categoryName']}")
    print(f"_id: {result['document']['_id']}\n")

query = "What bikes do you have?"
results = vector_search("products", query, num_results=4)
for result in results:
    print_product_search_result(result)

query = "What do you have that is yellow?"
results = vector_search("products", query, num_results=4)
for result in results:
    print_product_search_result(result)


# use vector search results in a RAG pattern with Chat GPT-4
# A system prompt describes the responsibilities, instructions, and persona of the AI.
system_prompt = """
You are a helpful, fun and friendly sales assistant for Cosmic Works, a bicycle and bicycle accessories store.
Your name is Cosmo.
You are designed to answer questions about the products that Cosmic Works sells.

Only answer questions related to the information provided in the list of products below that are represented
in JSON format.

If you are asked a question that is not in the list, respond with "I don't know."

List of products:
"""

def rag_with_vector_search(question: str, num_results: int = 3):
    """
    Use the RAG model to generate a prompt using vector search results based on the
    incoming question.
    """
    # perform the vector search and build product list
    results = vector_search("products", question, num_results=num_results)
    product_list = ""
    for result in results:
        if "contentVector" in result["document"]:
            del result["document"]["contentVector"]
        product_list += json.dumps(result["document"], indent=4, default=str) + "\n\n"

    # generate prompt for the LLM with vector results
    formatted_prompt = system_prompt + product_list

    # prepare the LLM request
    messages = [
        {"role": "system", "content": formatted_prompt},
        {"role": "user", "content": question}
    ]

    completion = ai_client.chat.completions.create(messages=messages, model=COMPLETIONS_DEPLOYMENT_NAME)
    return completion.choices[0].message.content

print(rag_with_vector_search("What bikes do you have?", 5))
print(rag_with_vector_search("What are the names and skus of yellow products?", 5))
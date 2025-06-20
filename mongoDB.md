1. Data Preparing
Convert CSV → JSON (optional but recommended)  
MongoDB works best with JSON-like data.  
Use Python locally or in Colab to convert:
```bash
import pandas as pd

df = pd.read_csv('customers.csv', encoding='ISO-8859-1')
df.to_json('customers.json', orient='records', lines=True)
```
2. Upload Dataset to MongoDB Atlas

**Option 1: Use MongoDB Compass (GUI)**
- Open MongoDB Compass
- Connect to your cluster (copy URI from MongoDB Atlas)
- Create a database, e.g., retail, and a collection, e.g., customers
- Click "Import Data" → Choose customers.json → JSON → click Import

**Option 2: Use mongoimport CLI (on your machine)**
```bash
mongoimport --uri "mongodb+srv://<username>:<password>@<cluster-url>/retail" \
  --collection customers --file customers.json --jsonArray
```
3. Load data from MongoAtlas to Colab
```bash
!pip install pymongo

# Necessary packages and classes
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Required URI to connect to the databases, which must be given in advance
uri = "mongodb+srv://admin1:pass1@cluster0.a0acxmd.mongodb.net/"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
```
4. Read data
```bash
# Get the data records in the collection 'users' of the database 'sample_mflix'
# These names must be given in advance
mongoData = client['retail']['customers']
# Check whether the data is get successfully by having a look at the first record
for c in mongoData.find():
  print(c)
  break
```
5. Transfer the data to a pandas DF for easy manipulation and save it
```bash
import pandas as pd
df1 = pd.DataFrame(list(mongoData.find()))
display(df1)

df1.to_csv("/content/Online Retail.csv")
```
6. 

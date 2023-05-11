import numpy as np
from fastapi import FastAPI, UploadFile, File, status
from pymongo import MongoClient
import pandas as pd
import io

app = FastAPI()


# Establish connection to MongoDB on startup
@app.on_event("startup")
async def startup_event():
    global client
    global db
    global collection
    client = MongoClient('mongodb://localhost:27017/')
    # DB name
    db = client["uploaded_file_csv"]
    # Collection name
    collection = db["file_data"]


# Retrieve data from MongoDB
@app.get("/data")
async def get_data():
    data = list(collection.find())
    for item in data:
        try:
            # Convert ObjectID to string
            item["_id"] = str(item["_id"])
        except Exception as e:
            print(f"Error converting _id: {e}")
    return data


# Upload CSV file to MongoDB
@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    # Takes the file type
    file_ext = file.filename.split(".").pop()

    # Check if the file is a CSV
    if file_ext == "csv":
        # Read the file contents and convert it to a DataFrame
        contents = await file.read()
        df = pd.read_csv(io.StringIO(contents.decode("utf-8")))

        # Replace infinite values with np.nan
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        # Replace NaN values with None
        df.fillna(value=np.empty, inplace=True)

        # Convert the DataFrame to a list of dictionaries (records)
        data = df.to_dict("records")

        # Loop through all the column names and store them in a list
        header = []
        for key in data[0]:
            header.append(key)

        # Drop the collection if it already exists
        if "file_data" in db.list_collection_names():
            collection.drop()

        # Insert the records into the collection as strings
        collection.insert_many([{key: str(value) for key, value in row.items()} for row in data])

        return {"message": f"Inserted {len(data)} records into the collection"}, status.HTTP_201_CREATED
    else:
        # File is not a .csv
        return {"message": "File provided is not .csv"}, status.HTTP_400_BAD_REQUEST


# Close the MongoDB connection on shutdown
@app.on_event("shutdown")
async def shutdown_event():
    client.close()
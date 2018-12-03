import os
import pymongo
import datetime
from bson import json_util
from google.cloud import bigquery
from bq_io import BigQueryWriter
import base64


def parse_change_event(change):
    event = {
        "_id": base64.b64encode(change["_id"]["_data"]).decode(),
        "operationType": change.get("operationType"),
        "fullDocument": json_util.dumps(change.get("fullDocument")),
        "updateDescription": json_util.dumps(change.get("updateDescription")),
        "collection": change.get("ns", {}).get("coll"),
        "documentKey": change.get("documentKey", {}).get("_id"),
        "clusterTime": datetime.datetime.now().timestamp(),
        "txnNumber": change.get("txnNumber"),
        "lsid": change.get("lsid"),
        "payload": json_util.dumps(change),
    }
    return event


# Grab variables from the environment
MONGODB_URI = os.getenv("MONGODB_URI")
DATABASE = os.getenv("DATABASE")
COLLECTION = os.getenv("COLLECTION")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "buffer-data")
BIG_QUERY_TABLE = os.getenv("BIG_QUERY_TABLE")
MAX_SKIPPED_RECORDS = os.getenv("MAX_SKIPPED_RECORDS", 5)

# Connect to the desired MongoDB collection
mongo_client = pymongo.MongoClient(MONGODB_URI, readPreference="secondaryPreferred")
db = mongo_client.get_database(DATABASE)
col = db.get_collection(COLLECTION)

# Get a resume token
bq_client = bigquery.Client(project=GOOGLE_CLOUD_PROJECT)

QUERY = f"""
select _id from `buda.{BIG_QUERY_TABLE}`
where
    clusterTime > '2018-11-26' and
    collection = '{COLLECTION}'
order by clusterTime desc limit 1
"""

query_job = bq_client.query(QUERY)
row = list(query_job.result())

if row:
    resume_token = {"_data": base64.b64decode(row[0][0])}

    cursor = None
    skipped_records = 0

    while not cursor and skipped_records < MAX_SKIPPED_RECORDS:
        try:
            cursor = col.watch(full_document="updateLookup", resume_after=resume_token)
        except pymongo.errors.OperationFailure as e:
            simple_cursor = col.watch(resume_after=resume_token)
            resume_token = simple_cursor.next().get("_id")
            skipped_records = skipped_records + 1
else:
    cursor = col.watch(full_document="updateLookup")

with cursor as stream, BigQueryWriter(
    BIG_QUERY_TABLE,
    dataset_id="buda",
    project_id=GOOGLE_CLOUD_PROJECT,
    client=bq_client,
    buffer_size=50,
) as bq_writer:
    for change in stream:
        record = parse_change_event(change)
        bq_writer.write(record)

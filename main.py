import os
import stacklogging
import pymongo
import datetime
from bson import json_util
from google.cloud import bigquery
import base64

MONGODB_URI = os.getenv("MONGODB_URI")
DATABASE = os.getenv("DATABASE")
COLLECTION = os.getenv("COLLECTION")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
BIG_QUERY_TABLE = os.getenv("BIG_QUERY_TABLE")
BIG_QUERY_DATASET = os.getenv("BIG_QUERY_DATASET")
MAX_SKIPPED_RECORDS = int(os.getenv("MAX_SKIPPED_RECORDS", 15))
MAX_DAYS_AGO = int(os.getenv("MAX_DAYS_AGO", 7))
BUFFER_SIZE = int(os.getenv("BUFFER_SIZE", 50))

logger = stacklogging.getLogger(__name__)


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


# Connect to MongoDB reading from secondary nodes
mongo_client = pymongo.MongoClient(MONGODB_URI, readPreference="secondaryPreferred")

db = mongo_client.get_database(DATABASE)
col = db.get_collection(COLLECTION)

bq_client = bigquery.Client(project=GOOGLE_CLOUD_PROJECT)
bq_dataset = bq_client.dataset(BIG_QUERY_DATASET)
bq_table = bq_dataset.table(BIG_QUERY_TABLE)

cursor = col.watch(full_document="updateLookup")

rows_buffer = []

with cursor as stream:
    for change in stream:
        record = parse_change_event(change)
        rows_buffer.append(record)

        if len(rows_buffer) == BUFFER_SIZE:
            errors = bq_client.insert_rows_json(
                bq_table,
                rows_buffer,
                skip_invalid_rows=True,
                ignore_unknown_values=True,
            )
            for row_errors in errors:
                for row_error in row_errors["errors"]:
                    logger.warning(row_error["message"])
            rows_buffer = []

# Carden

Save all the changes a collection had to BigQuery! Carden will read [MongoDB Change Streams](https://docs.mongodb.com/master/changeStreams/) for an specific `collection` and save each event in Big Query.

## Getting Started

Before running the Carden service, you'll need to create a BigQuery table to store the Change Streams Events. This step can be done with the `bq` command line tool running the following command:

```bash
PROJECT_ID="your-google-cloud-project-id"
DATASET="dataset-name"
TABLE="table-name"
bq mk \
    --clustering_fields collection,documentKey \
    --time_partitioning_field clusterTime\
    --table $PROJECT_ID:$DATASET.$TABLE \
    table_schema.json
```

After the table is created a deployment can be run (`kubectl apply -f your-service.deployment.yaml`) using the template deployment as a base.

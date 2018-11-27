# Carden

Save MongoDB Change Streams records into Big Query. Carden will crawl MongoDB oplog for a `collection` and send it to a Big Query table.

## Setup

Before running the Kubernetes deployments, you'll need to create the raw table. The Big Query table can be created with the `bq` command line tool.

```bash
bq mk --table [PROJECT_ID]:[DATASET].[TABLE] table_schema.json
```

After the table is created a deployment can be run (`kubectl apply -f your-service.deployment.yaml`) using the template deployment as a base.

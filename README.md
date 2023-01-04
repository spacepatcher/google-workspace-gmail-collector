## Google Workspace Gmail Collector

A simple application for collecting Gmail logs from your Google Workspace account and producing the events to Kafka topic. The app is designed to work continuously inside a container.

To be able to collect logs, you first need to set up [Gmail logging in BigQuery](https://support.google.com/a/answer/7233312). Gmail logs store records for each stage of a message in the Gmail delivery process.

The app makes SQL-like queries to the BigQuery dataset with Gmail logs at 60-second intervals and retrieves newly created entries.

### Usage

Declare environment variables and run:

```
docker build --tag gws-gmail . \
  && docker run --rm \
       -e KAFKA_SERVERS=$KAFKA_SERVERS \
       -e KAFKA_TOPIC=$KAFKA_TOPIC \
       --name gws-gmail \
       gws-gmail
```

Variables:
- `KAFKA_SERVERS` is an initial list of brokers as a CSV list of brokers in format `host1:9092,host2:9092,host3:9092`
- `KAFKA_TOPIC` is a name of Kafka topic

Files:
- `app/keyfile.json` contains Google Cloud [service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys)
- `app/kafka-ca.pem` contains Kafka CA certificate for verifying the broker's key
- `app/producer-gws-gmail.pem` contains Kafka client's public and private keys

### Authorization

GCP service account with roles `BigQuery Data Viewer` and `BigQuery User` is required to access the BigQuery dataset.


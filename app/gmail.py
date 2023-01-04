import json
from datetime import datetime, timedelta
from dataclasses import dataclass
from google.cloud import bigquery
from google.api_core import exceptions
import os
import time
import logging
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.WARN,
    format=
    '{"asctime":"%(asctime)s","name":"%(name)s","levelname":"%(levelname)s","message":"%(message)s"}'
)


@dataclass
class BigQuery:
    keyfile: str = 'keyfile.json'
    dataset_id: str = 'gmail_logs_dataset'

    def dataset(self, incremented=False):
        if incremented:
            table_id = 'daily_' + (datetime.now() -
                                   timedelta(1)).strftime('%Y%m%d')

        else:
            table_id = 'daily_' + datetime.now().strftime('%Y%m%d')

        return '{}.{}'.format(self.dataset_id, table_id)

    def auth(self):
        return bigquery.Client.from_service_account_json(self.keyfile)


@dataclass
class Kafka:
    bootstrap_servers: str = os.environ['KAFKA_SERVERS']
    topic: str = os.environ['KAFKA_TOPIC']
    ca: str = 'kafka-ca.pem'
    cert: str = 'producer-gws-gmail.pem'
    key: str = 'producer-gws-gmail.pem'

    def producer(self):
        try:
            p = Producer({
                'metadata.broker.list': self.bootstrap_servers,
                'security.protocol': 'SSL',
                'ssl.ca.location': self.ca,
                'ssl.certificate.location': self.cert,
                'ssl.key.location': self.key
            })

        except Exception as err:
            logging.fatal('Failed to create producer: %s' % err)
            exit(0)

        return p

    def payload(self, data):
        return json.dumps({
            'log_message': json.loads(data),
            'log_source': 'gws-gmail-logs-exporter',
            'log_sourcetype': 'gws_gmail',
            'log_utc_time_ingest': datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        })


def get_logs():
    bq = BigQuery()

    try:
        client = bq.auth()

    except Exception:
        raise

    time_range = int(datetime.now().timestamp() * 1000000)

    while True:
        time.sleep(60)

        incremented = False

        try:
            client.get_table(bq.dataset())

        except exceptions.NotFound:
            incremented = True

        sql = """
                SELECT *
                FROM `{}`
                WHERE event_info.timestamp_usec > {}
                ORDER BY event_info.timestamp_usec DESC
            """.format(bq.dataset(incremented), time_range)

        df = client.query(sql).to_dataframe()

        if not df.empty:
            send_kafka(df.to_json(orient='records', lines=True))

            time_range = df.iloc[0, df.columns.get_loc('event_info')].get(
                'timestamp_usec')

        if incremented:
            time_range = int(
                datetime.combine(datetime.today(),
                                 datetime.min.time()).timestamp() * 1000000)


def send_kafka(data):
    k = Kafka()

    p = k.producer()

    def delivery_callback(err, msg):
        if err:
            logging.error('Message failed delivery: %s' % err)
            time.sleep(1)

        else:
            logging.info('Message delivered to topic %s [%d] at offset %d' %
                         (msg.topic(), msg.partition(), msg.offset()))

    for item in data.splitlines():
        try:
            p.produce(k.topic, k.payload(item), callback=delivery_callback)

        except BufferError:
            logging.warn(
                'Local producer queue is full (%d messages awaiting delivery)'
                % len(p))

        p.poll(0)

    logging.info('Waiting for %d deliveries' % len(p))
    p.flush()


if __name__ == "__main__":
    get_logs()

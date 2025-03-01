import logging
import typing
from azure.functions import KafkaEvent
from azure.storage.blob import BlobServiceClient
import json
import os
import uuid


def main(kevents: typing.List[KafkaEvent]):
    for event in kevents:
        event_dec = event.get_body().decode('utf-8')
        event_json = json.loads(event_dec)
        logging.info("Python Kafka trigger function called for message " + event_json["Value"])
        publish_to_blob(event_json["Value"])


def publish_to_blob(data):
    localDemoContainer = str(uuid.uuid4())
    try:
        blob_service_client = BlobServiceClient.from_connection_string(os.environ["BlobConnectionString"])
        container_client = blob_service_client.get_container_client(localDemoContainer)

        # Create a container if not exists
        if not container_client.exists():
            container_client.create_container()

        blob_client = blob_service_client.get_blob_client(container=localDemoContainer, blob="local-blob")

        # Upload data to the blob
        blob_client.upload_blob(data, overwrite=True)
        logging.info(f"Blob created with data: {data}")
    except Exception as ex:
        logging.error(f"Failed to upload blob: {ex}")

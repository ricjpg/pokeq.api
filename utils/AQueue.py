import os
from azure.storage.queue import QueueClient, BinaryBase64DecodePolicy, BinaryBase64EncodePolicy
from dotenv import load_dotenv

load_dotenv()

class AQueue:
    def __init__(self):
        self.azure_sak = os.getenv('AZURE_SAK')
        self.queue_name = os.getenv('QUEUE_NAME')
        self.queue_client = QueueClient.from_connection_string(self.azure_sak, self.queue_name) # type: ignore
        self.queue_client.message_decode_policy = BinaryBase64DecodePolicy() # type: ignore
        self.queue_client.message_encode_policy = BinaryBase64EncodePolicy() # type: ignore

    async def insert_message_on_queue(self, message: str):
        message_bytes = message.encode('utf-8')
        self.queue_client.send_message(
            self.queue_client.message_encode_policy.encode(message_bytes) # type: ignore
        )

class AQueueDelete:
    def __init__(self):
        self.azure_sak = os.getenv('AZURE_SAK')
        self.queue_name = os.getenv('DELETE_QUEUE_NAME')
        self.queue_client = QueueClient.from_connection_string(self.azure_sak, self.queue_name) # type: ignore
        self.queue_client.message_decode_policy = BinaryBase64DecodePolicy() # type: ignore
        self.queue_client.message_encode_policy = BinaryBase64EncodePolicy() # type: ignore

    async def insert_delete_message(self, message: str):
        message_bytes = message.encode('utf-8')
        self.queue_client.send_message(
            self.queue_client.message_encode_policy.encode(message_bytes) # type: ignore
        )
import json
import asyncio
from aiokafka import AIOKafkaConsumer
from typing import Callable
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_SIMPLE_NOTIFICATIONS_TOPIC

class KafkaHandler:
    def __init__(self, task_handler: Callable, notification_handler: Callable):
        self.task_handler = task_handler
        self.notification_handler = notification_handler
        
    async def start_consumers(self):
        tasks = [
            asyncio.create_task(self.kafka_task_consumer()),
            asyncio.create_task(self.kafka_notification_consumer())
        ]
        
        await asyncio.gather(*tasks)
        
    async def kafka_task_consumer(self):
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        await consumer.start()
        try:
            async for msg in consumer:
                try:
                    task_data = msg.value
                    await self.task_handler(task_data)
                except Exception:
                    pass
        finally:
            await consumer.stop()
            
    async def kafka_notification_consumer(self):
        consumer = AIOKafkaConsumer(
            KAFKA_SIMPLE_NOTIFICATIONS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        await consumer.start()
        try:
            async for msg in consumer:
                try:
                    notification_data = msg.value
                    await self.notification_handler(notification_data)
                except Exception:
                    pass
        finally:
            await consumer.stop()
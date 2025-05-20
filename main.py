import asyncio
import threading
import time

from bot_handlers import bot
from kafka.consumer import KafkaHandler
from notification_handler import NotificationHandler

def run_bot():
    while True:
        try:
            bot.infinity_polling(timeout=60, long_polling_timeout=30)
        except Exception:
            time.sleep(5)
            continue

def run_kafka_consumer(notification_handler):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    kafka_handler = KafkaHandler(
        notification_handler.handle_new_task,
        notification_handler.handle_simple_notification
    )
    
    try:
        loop.run_until_complete(kafka_handler.start_consumers())
    except Exception:
        pass
    finally:
        loop.close()

if __name__ == "__main__":
    notification_handler = NotificationHandler(bot)
    
    kafka_thread = threading.Thread(
        target=run_kafka_consumer, 
        args=(notification_handler,),
        daemon=True
    )
    kafka_thread.start()
    
    run_bot()
from binance import ThreadedWebsocketManager
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
import json
import time

# Read the key and secret stored in the .env file
load_dotenv()
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")

# Set up the producer that would send the BTC trade data as bytes
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

is_running = True  # Flag to stop sending messages

# Define how I handle each message
def handle_msg(msg):

    # Ignore messages after shutdown
    if not is_running:  
        return
    
    if msg['e'] == 'error':
        print("Error:", msg['m'])
        return
    
    trade = {
        "symbol":   msg['s'],
        "price":    msg['p'],
        "quantity": msg['q'],
        "side":     "SELL" if msg['m'] else "BUY",
        "time":     msg['T']
    }
    # If there's no error, send to kafka topic
    producer.send("btc-trades", value=trade)
    print(f"Sent: {trade}")

# I define a timeout for the connection in seconds
TIMEOUT = 5

# Manages the Binance websocket connection
twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
twm.start()
twm.start_trade_socket(callback=handle_msg, symbol="BTCUSDT")

time.sleep(TIMEOUT)
is_running = False

twm.stop()           
producer.flush()     # Make sure all messages are sent to Kafka
producer.close()     # Close the Kafka connection
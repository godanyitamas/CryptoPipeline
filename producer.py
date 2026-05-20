from binance import ThreadedWebsocketManager
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
import json
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

load_dotenv()
api_key    = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def handle_msg(msg):
    if msg.get('e') == 'error':
        log.error(f"Error receiving message: {msg.get('m')}")
        return  # reconnect loop handles restart

    trade = {
        "symbol":   msg['s'],
        "price":    msg['p'],
        "quantity": msg['q'],
        "side":     "SELL" if msg['m'] else "BUY",
        "time":     msg['T']
    }
    producer.send("btc-trades", value=trade)
    log.info(f"Sent: {trade}")


def start_stream():
    """Start TWM and return (twm, socket_key)."""
    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    twm.start()
    key = twm.start_trade_socket(callback=handle_msg, symbol="BTCUSDT")
    log.info("WebSocket stream started.")
    return twm, key


RECONNECT_DELAY  = 5   # seconds before first retry
MAX_DELAY        = 60  # cap backoff at 60s

delay = RECONNECT_DELAY
while True:
    twm = None
    try:
        twm, _ = start_stream()
        twm.join()                      # blocks until stream stops
        log.warning("Stream ended unexpectedly, reconnecting...")
    except Exception as e:
        log.error(f"Stream error: {e}")
    finally:
        try:
            if twm:
                twm.stop()
        except Exception:
            pass

    log.info(f"Waiting {delay}s before reconnect...")
    time.sleep(delay)
    delay = min(delay * 2, MAX_DELAY)  # exponential backoff, capped at 60s
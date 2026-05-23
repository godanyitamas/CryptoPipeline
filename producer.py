from binance import ThreadedWebsocketManager
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
import json
import time
import logging
import threading

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

load_dotenv()
api_key    = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

_should_reconnect = threading.Event()
_error_logged = False  # prevent spamming the same error

def handle_msg(msg):
    global _error_logged
    if msg.get('e') == 'error':
        if not _error_logged:
            log.error(f"WebSocket error: {msg.get('m')}")
            _error_logged = True
            _should_reconnect.set()
        return

    _error_logged = False  # reset on successful message
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
    twm = ThreadedWebsocketManager(api_key=api_key, api_secret=api_secret)
    twm.start()
    key = twm.start_trade_socket(callback=handle_msg, symbol="BTCUSDT")
    log.info("WebSocket stream started.")
    return twm, key


RECONNECT_DELAY = 5
MAX_DELAY       = 60
delay           = RECONNECT_DELAY

while True:
    twm = None
    _should_reconnect.clear()  # ← critical: reset before each attempt
    _error_logged = False

    try:
        twm, _ = start_stream()
        _should_reconnect.wait()  # blocks until error fires
        log.warning("Reconnect signal received, restarting stream...")

    except Exception as e:
        log.error(f"Failed to start stream: {e}")
        delay = min(delay * 2, MAX_DELAY)
    else:
        delay = RECONNECT_DELAY  # reset delay on clean reconnect
    finally:
        try:
            if twm:
                twm.stop()
        except Exception:
            pass

    log.info(f"Waiting {delay}s before reconnect...")
    time.sleep(delay)
# Crypto Pipeline

Real-time BTC trade data pipeline using Binance websocket, that aggregates and displays ohlcv information.

## Architecture

<img src="assets/flowchart.png" width="700" alt="Pipeline flowchart"/>


## Setup
1. Copy `.env.example` to `.env` and fill in your Binance API keys
2. `docker compose up -d`
3. `pip install -r requirements.txt`
4. `python producer.py`
5. `python consumer.py`
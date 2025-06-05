from confluent_kafka import Producer
import time
import logging
import json
import ccxt
import requests 
from datetime import datetime

# --- Konfiguracja Kafka 
BOOTSTRAP_SERVERS = 'broker:9092'
TOPIC = 'realtimecryptoprices'

# --- Konfiguracja CCXT / Giełdy 
exchange = ccxt.kucoin()


SYMBOLS_TO_TRACK = ['ETH/USDT', 'ADA/USDT', 'SOL/USDT', 'BTC/USDT', 'LTC/USDT', 'DOT/USDT', 'XRP/USDT']


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Konfiguracja producenta Kafka 
conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
producer = Producer(conf)

logging.info(f"Producent próbuje połączyć się z brokerem: {BOOTSTRAP_SERVERS}")

# --- Funkcja callback  
def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Dostarczenie wiadomości nie powiodło się: {err}")
    else:
        value_str = msg.value().decode('utf-8') if msg.value() else 'N/A'
        logging.info(f"Wiadomość dostarczona do {msg.topic()} [{msg.partition()}] @ offset {msg.offset()} - Zawartość: {value_str[:100]}...")

# --- Funkcja do pobierania aktualnych cen 
def fetch_current_prices(symbols):
    data = {}
    for symbol in symbols:
        try:
            ticker = exchange.fetch_ticker(symbol)
            data[symbol] = ticker['last']
        except Exception as e:
            logging.warning(f"Błąd podczas pobierania {symbol}: {e}")
            data[symbol] = None
    data['timestamp'] = datetime.utcnow().isoformat() + 'Z'
    return data

# --- Funkcja do pobierania Bitcoin Dominance
def fetch_bitcoin_dominance():
    """Zwraca procentowy udział Bitcoina w całkowitej kapitalizacji rynku."""
    url = "https://api.coingecko.com/api/v3/global"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        jd = r.json()
        return jd['data']['market_cap_percentage']['btc']
    except Exception as e:
        logging.warning(f"Błąd podczas pobierania BTC Dominance: {e}")
        return None

# --- Główna pętla producenta
try:
    logging.info("Start pobierania cen i dominacji BTC, wysyłania do Kafka...")
    while True:
        # aktualne ceny
        prices_data = fetch_current_prices(SYMBOLS_TO_TRACK)

        # Bitcoin Dominance
        btc_dom = fetch_bitcoin_dominance()
        prices_data['btc_dominance'] = btc_dom

        if any(v is not None for k, v in prices_data.items() if k != 'timestamp' and k != 'btc_dominance') or btc_dom is not None:

            json_data = json.dumps(prices_data)

            key = str(prices_data['timestamp']).encode('utf-8')
            value = json_data.encode('utf-8')

            producer.produce(TOPIC,
                             key=key,
                             value=value,
                             callback=delivery_report)

            now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            logging.info(f"[{now_utc} UTC] Wysyłam dane do Kafka (Topic: {TOPIC}): {json_data}")
        else:
            logging.warning("Nie pobrano żadnych sensownych danych (ceny ani dominacji BTC), nie wysyłam do Kafka.")

        producer.poll(0)
        producer.flush()

        time.sleep(5)

except KeyboardInterrupt:
    logging.info("Producent zatrzymany przez użytkownika.")
except Exception as e:
    logging.error(f"Wystąpił nieoczekiwany błąd w producencie: {e}", exc_info=True)
finally:
    logging.info("Czekam na dostarczenie wszystkich oczekujących wiadomości...")
    producer.flush(timeout=10)
    logging.info("Producent zakończył działanie.")
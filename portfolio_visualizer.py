from confluent_kafka import Consumer, KafkaError
import logging
import json
import pandas as pd
from datetime import datetime
import plotly.graph_objects as go
from plotly.offline import plot # Do zapisywania wykresu w HTML

# --- Konfiguracja Kafka
BOOTSTRAP_SERVERS = 'broker:9092'
PORTFOLIO_UPDATES_TOPIC = 'portfolioupdates'
GROUP_ID = 'portfolio_visualizer_group_plotly_v2'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Konfiguracja konsumenta Kafka
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
}
consumer_viz = Consumer(conf)

logging.info(f"Wizualizator próbuje połączyć się z brokerem: {BOOTSTRAP_SERVERS} i subskrybować topic: {PORTFOLIO_UPDATES_TOPIC}")

consumer_viz.subscribe([PORTFOLIO_UPDATES_TOPIC])

# --- Dane do wykresu ---
portfolio_history_for_viz = []

# --- Funkcja aktualizująca i zapisująca wykres Plotly
def update_and_save_plot():
    if not portfolio_history_for_viz:
        return

    df_viz = pd.DataFrame(portfolio_history_for_viz)
    df_viz['timestamp'] = pd.to_datetime(df_viz['timestamp'])
    df_viz.sort_values('timestamp', inplace=True)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_viz['timestamp'], y=df_viz['total_value'],
                             mode='lines+markers', name='Wartość Portfela'))

    fig.update_layout(
        title='Wartość Portfela w Czasie Rzeczywistym',
        xaxis_title='Czas',
        yaxis_title='Wartość Portfela (USDT)',
        hovermode='x unified'
    )

    # wykres do pliku HTML
    file_path = 'portfolio_value_realtime.html'
    plot(fig, filename=file_path, auto_open=False) # auto_open=False, żeby nie próbowało otwierać w przeglądarce automatycznie
    logging.info(f"Wykres portfela zaktualizowany i zapisany do: {file_path}")

# --- Główna pętla konsumenta wizualizatora
try:
    logging.info("Start wizualizatora portfela...")
    update_interval_messages = 5
    message_count = 0

    while True:
        msg = consumer_viz.poll(timeout=1.0)

        if msg is None:
            continue 

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info(f"Osiągnięto koniec partycji {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
            elif msg.error():
                logging.error(f"Błąd wizualizatora: {msg.error()}")
                break
        else:
            decoded_value = msg.value().decode('utf-8')
            try:
                portfolio_data = json.loads(decoded_value)
                logging.info(f"Odebrano aktualizację portfela: {portfolio_data['total_value']:.2f} USDT")
                portfolio_history_for_viz.append({
                    'timestamp': portfolio_data['timestamp'],
                    'total_value': portfolio_data['total_value']
                })
                message_count += 1

                # Zapisz wykres co N wiadomości
                if message_count % update_interval_messages == 0:
                    update_and_save_plot()
                    message_count = 0 # Zresetuj licznik
            except json.JSONDecodeError as e:
                logging.error(f"Błąd dekodowania JSON w wizualizatorze: {e} dla wiadomości: {decoded_value}")
            except Exception as e:
                logging.error(f"Wystąpił błąd podczas przetwarzania danych w wizualizatorze: {e}", exc_info=True)

except KeyboardInterrupt:
    logging.info("\nWizualizator zatrzymany przez użytkownika.")
    # Zapisz końcowy wykres po zatrzymaniu
    if portfolio_history_for_viz:
        update_and_save_plot()
    logging.info("Wizualizator zakończył działanie.")
except Exception as e:
    logging.error(f"Wystąpił nieoczekiwany błąd w wizualizatorze: {e}", exc_info=True)
finally:
    logging.info("Zamykam konsumenta wizualizatora...")
    consumer_viz.close()
    logging.info("Wizualizator zakończył działanie.")
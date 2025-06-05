from confluent_kafka import Consumer, KafkaError, Producer # Dodano Producer
import logging
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# --- Konfiguracja Kafka
BOOTSTRAP_SERVERS = 'broker:9092'
TOPIC = 'realtimecryptoprices'
GROUP_ID = 'my_realtime_consumer_group_altseason_v2'

# --- Konfiguracja dla producenta
PORTFOLIO_UPDATES_TOPIC = 'portfolioupdates'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Konfiguracja konsumenta kafka
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)

logging.info(f"Konsument próbuje połączyć się z brokerem: {BOOTSTRAP_SERVERS} i subskrybować topic: {TOPIC}")

consumer.subscribe([TOPIC])

producer_portfolio_updates = Producer(conf)

# Funkcja callback
def delivery_report_portfolio(err, msg):
    if err is not None:
        logging.error(f"Dostarczenie aktualizacji portfela nie powiodło się: {err}")

# Globalny DataFrame do gromadzenia danych
data_log_df = pd.DataFrame()

SYMBOLS_TO_TRACK = ['ETH/USDT', 'ADA/USDT', 'SOL/USDT', 'BTC/USDT', 'LTC/USDT', 'DOT/USDT', 'XRP/USDT']


def detect_altseason(df_single_row, btc_dominance):
    """
    Oczekuje:
      - df_single_row: DataFrame z dokładnie jednym wierszem, kolumny to pary 'XXX/USDT'
      - btc_dominance: float, procentowy udział Bitcoina (np. 55.3)
    Zwraca:
      - altseason (bool)
    """
    # 1) Pobranie zwrotu BTC
    btc_return = df_single_row.get('BTC/USDT', pd.Series([0.0])).iloc[0]
    # 2) Lista altów: wszystkie pary USDT poza BTC
    altcoins = [col for col in df_single_row.columns if col.endswith('/USDT') and col != 'BTC/USDT']
    # 3) Średni zwrot altów
    alt_returns = df_single_row[altcoins].mean(axis=1).iloc[0]

    # 4) Progi BTC dominance
    dominance_thresholds = [56.0, 54.2, 52.8]
    passed_thresholds = [t for t in dominance_thresholds if btc_dominance is not None and btc_dominance < t]
    count_passed = len(passed_thresholds)

    # 5) Informacje diagnostyczne
    logging.info(f"    BTC zwrot:          {btc_return:.2%}")
    logging.info(f"    Śr. zwrot ALT:      {alt_returns:.2%}")
    if btc_dominance is not None:
        logging.info(f"    BTC Dominance:      {btc_dominance:.2f}%")
        logging.info(f"    Przebite progi:     {passed_thresholds}  (łącznie: {count_passed} z {len(dominance_thresholds)})")
    else:
        logging.info("    BTC Dominance:      brak danych (błąd pobrania)")
        logging.info("    Przebite progi:     []  (nie można ocenić bez dominacji)")

    # 6) Warunek Altseason:
    #   alt_returns – btc_return > 1pp (0.01)
    #   co najmniej jeden próg dominacji jest przebity (count_passed >= 1)
    altseason = False
    if (alt_returns - btc_return) > 0.01 and count_passed >= 1:
        altseason = True

    logging.info(f"    Altseason?:         {'TAK' if altseason else 'NIE'}\n")
    return altseason

def generate_signals(is_alt_flag, df_single_row, already_holding=None):
    """
    Reguły:
      - jeśli altseason=True i zwrot > 1%   => BUY
      - jeśli altseason=True i zwrot ≤ 1%    => HOLD
      - jeśli altseason=False                => SELL (ale nie sprzedawaj, jeśli coin jest w already_holding)
    Zwraca słownik { 'BTC/USDT': 'SELL', 'ETH/USDT': 'BUY', ... }
    """
    if already_holding is None:
        already_holding = []

    signals_out = {}
    for c in df_single_row.columns:
        # Pamiętaj, że tutaj 'c' może być nazwą kolumny dla cechy (_ma3, _std3), która nie jest monetą
        # Upewnij się, że generujemy sygnały tylko dla rzeczywistych symboli walut
        if c in SYMBOLS_TO_TRACK:
            rv = df_single_row[c].iloc[0]
            if is_alt_flag:
                signals_out[c] = 'BUY' if rv > 0.01 else 'HOLD'
            else:
                # jeżeli próbujemy sprzedać coś, czego już mamy, to zamieniamy SELL na HOLD
                if c in already_holding:
                    signals_out[c] = 'HOLD'
                else:
                    signals_out[c] = 'SELL'
    return signals_out

def log_and_backtest_advanced(signals_dict, prices_dict, portfolio_state):
    """
    - signals_dict: {'BTC/USDT': 'SELL', 'ETH/USDT': 'BUY', ...}
    - prices_dict: {'BTC/USDT': 28700, 'ETH/USDT': 1840, ...}
    
    Funkcja:
      - BUY: dzieli cash po równo między waluty, które mają sygnał BUY,
             przy zakupie aktualizuje entry_price jako ważoną średnią oraz sumę alokacji.
      - SELL: sprzedaje całą pozycję i usuwa z 'positions'.
      - HOLD: utrzymuje obecną pozycję.
      - Na koniec liczy wartość portfela, zapisuje do historii i wypisuje % zmiany od wartości początkowej.
    """
    # 1. Lista monet do BUY
    coins_buy = [c for c, s in signals_dict.items() if s == 'BUY']
    n_buy = len(coins_buy)
    cash_avail = portfolio_state['cash']
    cash_per_coin = (cash_avail / n_buy) if n_buy > 0 else 0

    for coin, sig in signals_dict.items():
        price = prices_dict.get(coin, None)
        if price is None:
            continue

        if sig == 'BUY':
            if portfolio_state['cash'] >= cash_per_coin and n_buy > 0:
                qty_to_buy = cash_per_coin / price
                if coin in portfolio_state['positions']:
                    existing = portfolio_state['positions'][coin]
                    prev_qty = existing['qty']
                    prev_entry = existing['entry_price']
                    prev_alloc = existing['allocated']
                    new_alloc = prev_alloc + cash_per_coin
                    total_qty = prev_qty + qty_to_buy
                    new_entry_price = (prev_entry * prev_qty + price * qty_to_buy) / total_qty
                    portfolio_state['positions'][coin] = {
                        'qty': total_qty,
                        'entry_price': new_entry_price,
                        'allocated': new_alloc
                    }
                else:
                    portfolio_state['positions'][coin] = {
                        'qty': qty_to_buy,
                        'entry_price': price,
                        'allocated': cash_per_coin
                    }
                portfolio_state['cash'] -= cash_per_coin

        elif sig == 'SELL':
            if coin in portfolio_state['positions']:
                qty_held = portfolio_state['positions'][coin]['qty']
                portfolio_state['cash'] += qty_held * price
                del portfolio_state['positions'][coin]

    # 2. Obliczamy wartość portfela: cash + suma(market_value każdej pozycji)
    total_val = portfolio_state['cash']
    for c_sym, pos in portfolio_state['positions'].items():
        if c_sym in prices_dict:
            total_val += pos['qty'] * prices_dict[c_sym]

    # 3. Zapis do historii
    portfolio_state['history'].append({
        'timestamp': datetime.utcnow(),
        'value': total_val
    })

    # 4. Obliczamy procentową zmianę względem wartości początkowej
    initial = portfolio_state.get('initial_value', None)
    if initial is not None and initial > 0:
        pct_change = (total_val - initial) / initial * 100
    else:
        pct_change = 0.0

    # 5. Wypisujemy stan portfela wraz z detalami pozycji i % zmiany
    logging.info("  Stan portfela (advanced backtest):")
    logging.info(f"    Cash: {portfolio_state['cash']:.2f} USDT")
    if portfolio_state['positions']:
        logging.info("    Pozycje:")
        for c_sym, pos in portfolio_state['positions'].items():
            market_val = pos['qty'] * prices_dict.get(c_sym, 0)
            alloc_pct = (market_val / total_val * 100) if total_val > 0 else 0
            logging.info(f"      {c_sym}: qty={pos['qty']:.6f}, entry_price={pos['entry_price']:.2f}, "
                         f"market_val={market_val:.2f} USDT, allocated={pos['allocated']:.2f} USDT, "
                         f"allocation={alloc_pct:.2f}%")
    else:
        logging.info("    Brak otwartych pozycji.")
    logging.info(f"    Wartość portfela: {total_val:.2f} USDT")
    logging.info(f"    % zmiana od wartości początkowej: {pct_change:+.2f}%\n")

    return portfolio_state

def evaluate_performance(history_list):
    """
    - history_list: lista słowników
    Buduje DataFrame, liczy zwroty i Sharpe Ratio, rysuje wykres wartości portfela.
    Zwraca (DataFrame, sharpe).
    """
    dfh = pd.DataFrame(history_list)
    dfh.set_index('timestamp', inplace=True)
    dfh.sort_index(inplace=True)
    dfh['returns'] = dfh['value'].pct_change()

    # Przybliżony Sharpe
    sharpe = dfh['returns'].mean() / dfh['returns'].std() * (365 ** 0.5) if dfh['returns'].std() != 0 else 0

    plt.figure(figsize=(9, 5))
    plt.plot(dfh.index, dfh['value'], marker='o', linestyle='-')
    plt.title(f"Wartość portfela | Sharpe Ratio: {sharpe:.2f}")
    plt.xlabel("Czas")
    plt.ylabel("Wartość portfela (USDT)")
    plt.grid(True)
    plt.tight_layout()
    
    # --- Zapisz wykres do PNG
    filename = f"portfolio_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
    plt.savefig(filename)
    logging.info(f"Wykres wartości portfela zapisany jako {filename}")

    plt.close() # Zamknij wykres po zapisaniu
    
    return dfh, sharpe


# --- Inicjalizacja portfela
portfolio = {
    'cash': 5635.0,
    'positions': {
        'BTC/USDT': {
            'qty': 0.05,
            'entry_price': 28000.0,
            'allocated': 1400.0
        },
        'ETH/USDT': {
            'qty': 0.50,
            'entry_price': 1800.0,
            'allocated': 900.0
        },
        'ADA/USDT': {
            'qty': 1000.0,
            'entry_price': 0.40,
            'allocated': 400.0
        },
        'SOL/USDT': {
            'qty': 20.0,
            'entry_price': 22.00,
            'allocated': 440.0
        },
        'LTC/USDT': {
            'qty': 10.0,
            'entry_price': 100.00,
            'allocated': 1000.0
        },
        'DOT/USDT': {
            'qty': 5.0,
            'entry_price': 25.00,
            'allocated': 125.0
        },
        'XRP/USDT': {
            'qty': 200.0,
            'entry_price': 0.50,
            'allocated': 100.0
        }
    },
    'initial_value': 10000.0,
    'history': []
}


# --- Główna pętla konsumenta
try:
    logging.info("Start konsumenta - odbieranie i przetwarzanie danych...")
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info(f"Osiągnięto koniec partycji {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
            elif msg.error():
                logging.error(f"Błąd konsumenta: {msg.error()}")
                break
        else:
            decoded_value = msg.value().decode('utf-8')

            try:
                data_from_kafka = json.loads(decoded_value)

                # Wyodrębnij btc_dominance i timestamp przed dodaniem do DataFrame
                btc_dominance = data_from_kafka.pop('btc_dominance', None) # Usuwa 'btc_dominance' i zwraca jego wartość
                timestamp_str = data_from_kafka.pop('timestamp', None)
                
                # Konwersja timestampu na datetime
                current_timestamp = pd.to_datetime(timestamp_str) if timestamp_str else datetime.utcnow()

                current_prices_dict = {
                    symbol: pd.to_numeric(data_from_kafka.get(symbol), errors='coerce')
                    for symbol in SYMBOLS_TO_TRACK
                    if symbol in data_from_kafka
                }
                
                # timestamp do słownika cen, zanim utworzysz DataFrame
                current_prices_dict['timestamp'] = current_timestamp

                # tymczasowy DataFrame z jednego wiersza
                new_row_df = pd.DataFrame([current_prices_dict])
                
                # Konkatenacja z głównym DataFrame
                data_log_df = pd.concat([data_log_df, new_row_df], ignore_index=True)
                
                if len(data_log_df) > 500: 
                    data_log_df = data_log_df.tail(500).reset_index(drop=True)

                # 4. Wykonywanie obliczeń (jeśli mamy wystarczająco danych dla okien)
                # Potrzebujemy minimum 5 wierszy dla rolling window=5
                if len(data_log_df) >= 5: 
                    
                    returns_df = data_log_df[SYMBOLS_TO_TRACK].pct_change()
                    
                    # Rolling window: średnie i odchylenia standardowe
                    rolling_short = returns_df.rolling(window=3).mean().add_suffix('_ma3')
                    rolling_medium = returns_df.rolling(window=5).mean().add_suffix('_ma5')
                    rolling_std = returns_df.rolling(window=3).std().add_suffix('_std3')

                    # Połączenie wyników do jednego DataFrame
                    combined_features = pd.concat([returns_df, rolling_short, rolling_medium, rolling_std], axis=1)
                    
                    last_processed_row = combined_features.iloc[[-1]].dropna(axis=1, how='all')

                    if last_processed_row.empty:
                        logging.warning("Ostatni wiersz po obliczeniach okien czasowych jest pusty. Pominęto detekcję.")
                        continue

                    logging.info(f"\n=== {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC ===")
                    
                    # 5 Detekcja Altseason
                    is_altseason = detect_altseason(last_processed_row, btc_dominance)

                    # 6. Generowanie sygnałów
                    already_holding = list(portfolio['positions'].keys())
                    signals = generate_signals(is_altseason, last_processed_row, already_holding=already_holding)
                    logging.info(f"  Sygnały: {signals}\n")

                    # 7. Zaawansowany backtest
                    current_prices_for_backtest = {
                        symbol: data_from_kafka.get(symbol)
                        for symbol in SYMBOLS_TO_TRACK if data_from_kafka.get(symbol) is not None
                    }
                    if current_prices_for_backtest: # Upewnij się, że są jakieś ceny
                        portfolio = log_and_backtest_advanced(signals, current_prices_for_backtest, portfolio)
                        

                        
                        try:
                            # Upewnij się, że history ma co najmniej jeden element
                            total_val_to_send = portfolio['history'][-1]['value'] if portfolio['history'] else portfolio['initial_value']

                            portfolio_snapshot = {
                                'timestamp': datetime.utcnow().isoformat() + 'Z',
                                'cash': portfolio['cash'],
                                'positions': {k: v for k, v in portfolio['positions'].items()},
                                'total_value': total_val_to_send
                            }
                            
                            message_value = json.dumps(portfolio_snapshot).encode('utf-8')
                            producer_portfolio_updates.produce(
                                PORTFOLIO_UPDATES_TOPIC,
                                key=portfolio_snapshot['timestamp'].encode('utf-8'),
                                value=message_value,
                                callback=delivery_report_portfolio
                            )
                            producer_portfolio_updates.poll(0) # Sprawdź callbacki (nieblokujące)
                        except Exception as e:
                            logging.error(f"Błąd podczas wysyłania aktualizacji portfela do Kafki: {e}", exc_info=True)

                    else:
                        logging.warning("Brak aktualnych cen do przeprowadzenia backtestu.")

                else:
                    logging.info(f"Odebrano {len(data_log_df)} wierszy. Potrzeba minimum 5 do obliczeń okien czasowych i detekcji.")
                    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    logging.info(f"[{now_utc} UTC] Odebrano surowe dane (za mało do okien): {data_from_kafka}")
                
            except json.JSONDecodeError as e:
                logging.error(f"Błąd dekodowania JSON: {e} dla wiadomości: {decoded_value}")
            except Exception as e:
                logging.error(f"Wystąpił błąd podczas przetwarzania danych w konsumencie: {e}", exc_info=True)
            
except KeyboardInterrupt:
    logging.info("\nKonsument zatrzymany przez użytkownika.")
except Exception as e:
    logging.error(f"Wystąpił nieoczekiwany błąd w konsumencie: {e}", exc_info=True)
finally:
    logging.info("Zamykam konsumenta...")
    consumer.close()
    
    logging.info("Czekam na dostarczenie wszystkich oczekujących aktualizacji portfela...")
    producer_portfolio_updates.flush(timeout=10)
    logging.info("Producent aktualizacji portfela zakończył działanie.")

    # 8 Ocena skuteczności i wizualizacja po zakończeniu działania
    if portfolio['history']:
        logging.info("Generowanie wykresu wartości portfela (po zakończeniu działania)...")
        try:
            df_hist, port_sharpe = evaluate_performance(portfolio['history'])
            logging.info(f"Sharpe Ratio portfela (po zakończeniu): {port_sharpe:.2f}")
        except Exception as e:
            logging.error(f"Błąd podczas generowania wykresu lub obliczania Sharpe Ratio: {e}", exc_info=True)
    else:
        logging.info("Brak historii portfela do wizualizacji.")
    
    logging.info("Konsument zakończył działanie.")
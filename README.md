# 📊 Real-Time Portfolio Visualizer (Kafka + Plotly)

A Python-based application for **real-time portfolio value visualization** using **Apache Kafka** for message streaming and **Plotly** for interactive chart generation.  
The project demonstrates integration between data pipelines and dynamic visualization in a lightweight, script-based environment.

---

## 🚀 Features

- Consumes portfolio update messages from a **Kafka topic**
- Parses and aggregates incoming JSON payloads with portfolio metrics
- Dynamically updates an interactive **Plotly** line chart
- Automatically saves the chart as an offline **HTML dashboard**
- Logs processing details and errors for transparency and debugging

---

## 🧠 Technologies

- **Python 3.x**
- **Apache Kafka** (via `confluent_kafka`)
- **Pandas**
- **Plotly**
- **JSON / Logging / Datetime**

---

## ⚙️ How It Works

1. The script subscribes to a Kafka topic (e.g., `portfolioupdates`).
2. Each incoming message contains real-time portfolio data (`timestamp`, `total_value`).
3. Messages are decoded and appended to a local data buffer.
4. Every few updates, an interactive Plotly chart is regenerated and saved to `portfolio_value_realtime.html`.
5. The visualization can be refreshed in any browser to view the latest data.

---

## 🧩 Example Message Format

```json
{
  "timestamp": "2025-10-15T14:05:21Z",
  "total_value": 10325.42
}

---

## 🖥️ Output Example

File: portfolio_value_realtime.html

Chart: Line plot of portfolio total value over time, auto-updated every 5 messages

---

##📦 Requirements

pip install confluent-kafka pandas plotly

---

##🧑‍💻 Author

Developed by Kamil Drzewiecki
Focus areas: Data Science • Machine Learning • Streaming Analytics
📧 [[LinkedIn](https://www.linkedin.com/in/kamil-drzewiecki-ds/) / [GitHub](https://github.com/Drzewkam)]

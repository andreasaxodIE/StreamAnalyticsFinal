# Stream Analytics Final Project

This repository contains our final project for the Big Data and Streaming Systems course. We built a small food delivery analytics pipeline that starts with synthetic events and ends with live dashboard views.

The project started with a data generator and then grew into a full streaming setup. Right now the repo includes:

- two event feeds in AVRO
- a Python generator for fake orders and courier activity
- an Azure Event Hub producer
- Spark Structured Streaming jobs for the analytics
- a Streamlit dashboard for the final visual part

The idea was to model something close to a delivery app and answer questions like these in near real time:

- Which zones are getting the most orders?
- Where are cancellations higher?
- Does bad weather slow deliveries down?
- Are there enough idle couriers for current demand?
- Can we detect strange courier behaviour from the event stream?

## Team

| Name | Main contribution |
| --- | --- |
| Andrea Saxod | Feed design and AVRO schema work |
| Matias Arevalo | Generator logic and event simulation |
| Cloe Chapotot | Data quality checks and edge cases |
| Joao Paulo Tobar Prado | Spark streaming pipeline |
| Vittorio Fialdini | Spark streaming pipeline |
| Clementine, Francesca Mathieu | Dashboard and visualisation |

## How the pipeline works

1. `generator/main.py` creates two synthetic feeds.
2. `producer/eventhub_producer.py` publishes the events to Azure Event Hub.
3. `spark/run_all_ucs.py` reads both topics with Spark Structured Streaming.
4. Spark writes aggregated parquet outputs.
5. `dashboard/app.py` reads those outputs and refreshes the charts.

So the repo is not just a dataset generator anymore. It is the whole path from event creation to analytics output.

## The two feeds

### 1. Order lifecycle events

This feed follows each order from placement to delivery or cancellation. Typical statuses are:

`PLACED -> ACCEPTED -> PREPARING -> READY_FOR_PICKUP -> PICKED_UP -> EN_ROUTE -> DELIVERED`

There can also be cancelled or failed paths. This feed is useful for metrics such as:

- order volume by zone
- cancellation rate
- prep time SLA breaches
- weather impact on delivery time
- ETA accuracy
- average order value

### 2. Courier status events

This feed tracks the courier side. It includes status changes plus frequent location updates. Example states are:

`OFFLINE -> ONLINE_IDLE -> HEADING_TO_RESTAURANT -> WAITING_AT_RESTAURANT -> PICKED_UP -> EN_ROUTE_TO_CUSTOMER -> DELIVERED`

This feed is denser than the order feed because couriers keep producing events while moving. It is useful for metrics such as:

- available couriers by zone
- supply versus demand imbalance
- courier anomalies
- productivity by vehicle type

## Repository structure

```text
StreamAnalyticsFinal-main/
|-- README.md
|-- requirements.txt
|-- schemas/
|   |-- order_lifecycle_event.avsc
|   `-- courier_status_event.avsc
|-- generator/
|   |-- main.py
|   |-- config.py
|   |-- order_generator.py
|   |-- courier_generator.py
|   |-- avro_writer.py
|   `-- README Generator.md
|-- producer/
|   `-- eventhub_producer.py
|-- spark/
|   |-- run_all_ucs.py
|   |-- spark_session.py
|   `-- uc*.py
|-- dashboard/
|   `-- app.py
|-- settings/
|   `-- eventhub_config.py
`-- sample_data/
    |-- order_lifecycle_events.jsonl
    |-- order_lifecycle_events.avro
    |-- courier_status_events.jsonl
    `-- courier_status_events.avro
```

## Analytics implemented

The main analytics that are currently wired into the streaming pipeline are:

1. Order volume and cancellation rate by zone
2. Prep time SLA breaches
3. Weather impact on delivery times
4. Courier anomaly detection
5. Supply versus demand by zone
6. Average processing time
7. Average order value and revenue
8. ETA accuracy
9. Courier productivity by vehicle type and zone

The dashboard groups some of these under friendlier business labels, but these are the core use cases in the code.

## To run our project run the following Collab Notebook: 
https://colab.research.google.com/drive/1OojVSmIqxem2BT5CUawdOFE-3D2Ewv47?usp=sharing


## Notes about the current repo

This project was built for a university demo, so a few things are still a bit class-project style:

- some Azure-related values are still hardcoded for convenience
- the generator can be run fully locally, but the full live pipeline depends on Azure Event Hub
- there is more than one way to run the analytics because we kept some individual Spark scripts while also adding `run_all_ucs.py`

If we were continuing this project, the next clean-up step would probably be moving all credentials and paths into environment variables and adding a one-command setup.

## Sample output

The `sample_data/` folder already contains example files, so you can inspect the generated events without starting the full streaming pipeline.

Order feed files:

- `order_lifecycle_events.jsonl`
- `order_lifecycle_events.avro`

Courier feed files:

- `courier_status_events.jsonl`
- `courier_status_events.avro`

## Final comment

We tried to keep the project practical rather than overly theoretical. The code is meant to show how a streaming pipeline can be built step by step, starting from event design and ending with simple live analytics.

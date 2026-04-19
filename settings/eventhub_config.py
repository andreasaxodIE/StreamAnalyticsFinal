"""
Azure Event Hub configuration template.

SETUP:
  1. Copy this file:  cp config/eventhub_config_template.py config/eventhub_config.py
  2. Fill in your connection strings below.
  3. NEVER commit eventhub_config.py — it's in .gitignore.
"""

# Azure Event Hub Namespace (Group 09, BBADBA A)
EVENT_HUB_NAMESPACE = "iesstsabbadbaa-grp-06-10"

# Topic names (Event Hub Instances)
ORDERS_TOPIC = "group_09_orders"
COURIERS_TOPIC = "group_09_couriers"


EVENTHUB_CONNECTION_STR = "Endpoint=sb://iesstsabbadbaa-grp-06-10.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=2GZJc3vlo2ksPr+vu44pSrbhOeCkQR0Vc+AEhM+iPn4="

# Kafka-compatible bootstrap server (derived — don't change)
KAFKA_BOOTSTRAP_SERVERS = f"{EVENT_HUB_NAMESPACE}.servicebus.windows.net:9093"

# SASL/SSL config for Kafka clients
KAFKA_SASL_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString",
    "sasl.password": EVENTHUB_CONNECTION_STR,
}

# Spark-specific Kafka config (keys prefixed with "kafka.")
SPARK_KAFKA_CONFIG = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{EVENTHUB_CONNECTION_STR}";'
    ),
}

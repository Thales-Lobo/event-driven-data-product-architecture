#!/usr/bin/env bash
# Stream messages from a Kafka topic inside the running `kafka` container,
# piped through format_kafka_line.py for a readable, non-cluttered view --
# still showing the exact event data (nothing summarized or hidden), just
# reformatted from the raw tab-separated console-consumer output.
#
# This shows the exact traffic between the API (producer) and the
# Orchestrator (consumer) -- tangible proof that Kafka sits in the middle of
# the reactive flow, decoupling the two containers.
#
# Usage: watch_kafka_topic.sh <topic-name>
set -euo pipefail

cd "$(dirname "$0")/.."

TOPIC="${1:?Usage: watch_kafka_topic.sh <topic-name>}"

echo "── Streaming topic '${TOPIC}' (Ctrl+C to stop) ──"
echo

# `-T` disables pseudo-tty allocation on `docker compose exec`, which keeps
# the piped output clean and predictable regardless of terminal type.
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --property print.key=true \
  --property print.timestamp=true \
  | python3 "$(dirname "$0")/format_kafka_line.py"
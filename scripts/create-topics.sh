set -euo pipefail

resolve_bin() {
  local override="${KAFKA_BIN:-}"

  if [ -n "${override}" ]; then
    echo "${override}"

    return 0
  fi

  local candidates=(
    "/opt/kafka/bin"
    "/opt/bitnami/kafka/bin"
  )

  for path in "${candidates[@]}"; do
    if [ -x "${path}/kafka-topics.sh" ]; then
      echo "${path}"

      return 0
    fi
  done

  echo "Unable to locate kafka-topics.sh" >&2

  return 1
}

BIN="$(resolve_bin)"
BS="${KAFKA_BOOTSTRAP_SERVER:-kafka-1:9092}"
TOPIC_REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-2}"
BS_HOST="${BS%%:*}"
BS_PORT="${BS##*:}"

echo "⏳ Waiting for Kafka TCP on ${BS} ..."

for i in {1..60}; do
  if (exec 3<>"/dev/tcp/${BS_HOST}/${BS_PORT}") 2>/dev/null; then
    exec 3>&- 3<&-
    break
  fi
  sleep 2
done

for i in {1..60}; do
  if "${BIN}/kafka-topics.sh" --bootstrap-server "${BS}" --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "→ Topics before:"
"${BIN}/kafka-topics.sh" --bootstrap-server "${BS}" --list || true

create() {
  local topic="$1" parts="${2:-3}" rf="${3:-${TOPIC_REPLICATION_FACTOR}}"
  echo "→ Creating topic ${topic} (${parts} partitions, RF=${rf})"
  "${BIN}/kafka-topics.sh" --bootstrap-server "${BS}" \
    --create --topic "${topic}" \
    --partitions "${parts}" \
    --replication-factor "${rf}" \
    --if-not-exists
}

create new-mail
create interesting-mail
create hire-event
create notification
create applications-sync-unprocessed
create applications-sync-processed
create application-update-unprocessed
create application-update-processed
create notification-sync
create add-embedding-for-application
create save-embedding-for-application
create feedback

echo "→ Topics after:"
"${BIN}/kafka-topics.sh" --bootstrap-server "${BS}" --list

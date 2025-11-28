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
BS=kafka:9092

echo "⏳ Waiting for Kafka TCP on ${BS} ..."

for i in {1..60}; do
  if (exec 3<>/dev/tcp/kafka/9092) 2>/dev/null; then
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
  local topic="$1" parts="${2:-3}" rf="${3:-1}"
  echo "→ Creating topic ${topic} (${parts} partitions, RF=${rf})"
  "${BIN}/kafka-topics.sh" --bootstrap-server "${BS}" \
    --create --topic "${topic}" \
    --partitions "${parts}" \
    --replication-factor "${rf}" \
    --if-not-exists
}

create new-mail 3 1
create interesting-mail 3 1
create hire-event 3 1
create notification 3 1
create applications-sync-unprocessed 3 1
create applications-sync-processed 3 1
create application-update-unprocessed 3 1
create application-update-processed 3 1
create notification-sync 3 1
create add-embedding-for-application 3 1
create save-embedding-for-application 3 1
create feedback 3 1

echo "→ Topics after:"
"${BIN}/kafka-topics.sh" --bootstrap-server "${BS}" --list

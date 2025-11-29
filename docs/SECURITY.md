# Security

- Emails are saved only locally, nothing is sent outside
- Services communicate over the internal Docker network; only documented ports are exposed to the host, and Kafka/Postgres/Redis data stays within local volumes
- External calls (Gmail, OpenAI, Google Sheets, Telegram) are authenticated with explicit tokens and keys so nothing leaves the environment without configured credentials

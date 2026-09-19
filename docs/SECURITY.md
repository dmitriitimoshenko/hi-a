# Security

- Email data is stored locally; it is not forwarded to external systems except for explicitly configured APIs (Telegram and OpenAI), and only the hosting user has access to the email data.
- The service does not fetch historical emails; it monitors only new messages and stores them in the internal database only after classifying them as job-search-related.
- Services communicate solely over the internal Docker network. Postgres and Redis are not published to the host at all; only the services' HTTP ports (8081-8083, 8085, 8086, 8088) are exposed, and Postgres/Redis data remains confined to local volumes.
- External integrations (Gmail, OpenAI, Google Sheets, Telegram) are invoked with explicit tokens and keys, so nothing leaves the environment without configured credentials.

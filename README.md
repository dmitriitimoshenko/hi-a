# Hire Insight Assistant

A self-hosted assistant for job seekers that connects Gmail, Google Sheets, Telegram, and AI to classify hiring emails and keep application statuses synchronized.

This project is AI friendly and built primarily in Python. It started as a web infrastructure playground and has evolved into a valuable tool for everyday job search tracking.

## Project mission

- Turn scattered hiring emails, spreadsheet edits, and manual updates into a coherent self-hosted workflow
- Use modern AI tooling to reduce manual triage and classification for a job seeker
- Keep the user informed through an everyday messenger while collecting feedback without adding noise

## Requirements
- Hosting mashine *(for example, EC2 or a local machine)* with Python3.13, Docker and Docker Compose installed
- [OpenAI Platform](https://platform.openai.com/) account to enable embeddings
- Google Account fot Gmail, Sheets and GCP usage *(recommended to create a new account)*
- Telegram account

## Quick start

1. Prepare a Google Sheet using [this template](https://docs.google.com/spreadsheets/d/1BF_nt4NeeTJCH96qmHD-DeUn5E7YN3LZikL_zSP_IGk/copy) for application tracking.
    * In Google Cloud Platform, generate a `json` credentials file for Google Sheets API access.
2. Create a Telegram bot via [Bot Father](https://telegram.me/BotFather).
3. **Optional:** register a dedicated Gmail address for job seeking to reduce spam and improve classifier accuracy.
4. Install Python3.13, Docker and Docker Compose on the host.
5. Populate `.env.example` with required secrets following instruction from the file.
6. In the copied Google Sheet, use the `applications_list` page (`example` page can be removed).
7. Bring up HI-A with:
    ```
    python3.13 -m venv mail-processor/.venv \
    docker compose up -d \
    ./scripts/migrate.sh
    ```
8. Populate the `embd_cntr` table with data from `mail-processor/embeddings/embd_cntr_populate.csv`.

## Related documents

* [Security](docs/SECURITY.md)
* [Architecture](docs/ARCH.md)
* [Codestyle](docs/CODESTYLE.md)
* [Mail Mapper description](docs/MAIL_MAPPER.md)

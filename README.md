# hire-insight-ai-assistant

A self-hosted tool for job seekers that ties together Gmail, Google Sheets, Telegram, and AI to classify hiring emails and track job application statuses automatically

### Project mission

- Turn scattered hiring emails, applications sheet edits, and manual updates into a coherent online self-hosted service
- Use contemporary AI-tools to reduce manual triage and classification for a job seeker
- Keep a user up to date and collect user's responses and feedback through Telegram, which doesn't overload the user while everyday applications statuses tracking

### What is it?

This is a 80% vibe-coded project written mostly in Python. It was started as an infrastructure playground for the creator but in a while, this became a valuable tool for everyday job seeking

### How to start?

1. Choose something to host the service (*ec2*, *local mashine* etc.)
2. Top up the ballance at [OpenAI Platform](https://platform.openai.com/) to be able to use embeddings (1$ is way more then enough)
3. Prepare a Google sheet using [this template](https://docs.google.com/spreadsheets/d/1BF_nt4NeeTJCH96qmHD-DeUn5E7YN3LZikL_zSP_IGk/copy) for application tracking
    * using Google Cloud Platform, generate a `json` credentials file for access to Google Sheets (GCP -> Google Sheet API)
4. Create a Telegram Bot (you can use [Bot Father](https://telegram.me/BotFather) for quick creation)
5. ***optional:*** Register a new Gmail address for job seeking. This way you will
    * save your real email address from spam and flood
    * let classifier be more accurate
6. Install Docker, Docker Compose, and Make on the hosting machine
7. Populate `.env` with required secrets: `OPENAI_API_KEY`, `GMAIL_USER` (your emial address), `GMAIL_APP_PASSWORD` (you can easily take App Password in your Gmail account menu), `SHEET_ID` (you can take it from the sheet URL) and `BOT_TOKEN`; place Google credentials at `secrets/gsa-credentials.json`
8. **Before** the HI-A is started, clean up the template Google Sheet by deleting the example rows
9. Bring up the HI-A with the following bash command (install `python3.13` if not installed): 
    ```
    python3.13 -m venv mail-processor/.venv
    make up \
    ./scripts/migrate.sh
    ```
10. Populate `embd_cntr` table with the data from this csv file: `embd_cntr_populate.csv`

### Why is it safe?

- Emails are saved only locally, nothing is sent to the outside
- Services communicate over the internal Docker network; only documented ports are exposed to the host, and Kafka/Postgres/Redis data stays within local volumes; until ports `5432`, `5433` and `5434` are closed, your data cannot be accesed from outside
- External calls (Gmail, OpenAI, Google Sheets, Telegram) are authenticated with explicit tokens and keys so nothing leaves the environment without configured credentials

### Side topics

* [To contributors](CONTRIBUTORS.md)
* [Codestyle](CODESTYLE.md)
* [Mail Mapper description](mail-mapper/README.md)

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    amount_from NUMERIC(12, 2),
    amount_to NUMERIC(12, 2),
    currency VARCHAR(3) NOT NULL,
    period VARCHAR(32) NOT NULL,
    CONSTRAINT ck_salary_amount_from_non_bigger_than_amount_to CHECK (
        (amount_from IS NULL) OR (amount_to IS NULL) OR (amount_to >= amount_from)
    )
);

CREATE TABLE IF NOT EXISTS application (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    company VARCHAR(255) NOT NULL,
    title VARCHAR(255) NOT NULL,
    employment_type VARCHAR(64) NOT NULL,
    work_mode VARCHAR(64) NOT NULL,
    status VARCHAR(64) NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL,
    responded_at TIMESTAMPTZ,
    next_follow_up_at TIMESTAMPTZ,
    stage VARCHAR(64),
    meta JSON,
    embedding VECTOR(1536),
    row_id INTEGER NOT NULL,
    applied_email_received TIMESTAMPTZ,
    applied_email_id INTEGER,
    denied_email_received TIMESTAMPTZ,
    denied_email_id INTEGER,
    meeting_inv_email_received TIMESTAMPTZ,
    meeting_inv_email_id INTEGER,
    meeting_crt_email_received TIMESTAMPTZ,
    meeting_crt_email_id INTEGER,
    meeting_upd_email_received TIMESTAMPTZ,
    meeting_upd_email_id INTEGER,
    meeting_cncl_email_received TIMESTAMPTZ,
    meeting_cncl_email_id INTEGER,
    salary_applied_id INTEGER,
    salary_proposed_id INTEGER,
    CONSTRAINT fk_application_salary_applied FOREIGN KEY (salary_applied_id) REFERENCES salary(id) ON DELETE SET NULL,
    CONSTRAINT fk_application_salary_proposed FOREIGN KEY (salary_proposed_id) REFERENCES salary(id) ON DELETE SET NULL,
    CONSTRAINT uq_application_row_id UNIQUE (row_id)
);

CREATE INDEX IF NOT EXISTS ix_application_salary_applied_id ON application (salary_applied_id);
CREATE INDEX IF NOT EXISTS ix_application_salary_proposed_id ON application (salary_proposed_id);

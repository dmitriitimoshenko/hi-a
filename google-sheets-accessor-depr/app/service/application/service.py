import logging
import re

from dataclasses import dataclass, field
from datetime import timezone, datetime
from decimal import Decimal
from fastapi import Depends
from typing import Any, cast
from icalendar import Calendar
from sqlalchemy import and_, not_
from sqlalchemy.orm import selectinload

from app.integrations.google_sheets_client.service.service import (
    SheetsService,
    get_sheets_service,
)
from app.integrations.open_ai.service.service import OpenAIService, get_openai_service
from app.database.database import get_db, Session
from app.redis_client.client import RedisClient, get_redis_client
from app.models import Application, Salary
from app.enums.application_status import ApplicationStatus
from app.tools.normalization import normalize_content
from app.kafka_client import EmbeddingPublisher, get_embedding_publisher
from app.config import Config

logging.basicConfig(level=logging.DEBUG)

ROWS_PROCESSED_KEY = "rows:processed"
MIN_SHEET_ROW = 3
MAX_SHEET_ROW = 1000


@dataclass
class SalarySnapshot:
    amount_from: Decimal | None
    amount_to: Decimal | None
    currency: str | None
    period: str | None


@dataclass
class SheetRowSnapshot:
    row_id: int
    has_values: bool
    company: str | None
    employment_type: str | None
    work_mode: str | None
    title: str | None
    status: str | None
    applied_at: datetime | None
    responded_at: datetime | None
    next_follow_up_at: datetime | None
    stage: str | None
    meta_contacts: str | None
    meta_job_description: str | None
    meta_notes: str | None
    salary_applied: SalarySnapshot | None
    salary_proposed: SalarySnapshot | None
    errors: list[str] = field(default_factory=list)


@dataclass
class FieldDifference:
    field_name: str
    sheet_value: Any
    db_value: Any
    message: str


class ApplicationService:
    def __init__(
        self,
        db: Session,
        redis_client: RedisClient,
        sheets_service: SheetsService,
        openai_service: OpenAIService,
        embedding_publisher: EmbeddingPublisher,
    ) -> None:
        self._db = db
        self._redis_client = redis_client
        self._sheets_service = sheets_service
        self._openai_service = openai_service
        self._embedding_publisher = embedding_publisher
        self._logger = logging.getLogger(__name__)

    def fetch(
        self,
        sheet_id: str,
        sheet_page: str,
        sheet_start_ceil: str | None,
        sheet_end_ceil: str | None,
    ) -> tuple[int, int]:
        rows_processed = int(self._redis_client.get(ROWS_PROCESSED_KEY) or 0)
        self._logger.debug(f"rows:processed from REDIS: {rows_processed}")
        if sheet_start_ceil is None:
            sheet_start_ceil = "A" + str(3 + rows_processed)
        if sheet_end_ceil is None:
            sheet_end_ceil = "P1000"

        sheet_range_str = sheet_page + "!" + sheet_start_ceil + ":" + sheet_end_ceil
        vals = self._sheets_service.get_values(sheet_id, sheet_range_str)

        valid_rows, skipped_rows = self._filter_trailing_invalid_rows(vals)

        if skipped_rows > 0:
            self._logger.warning(
                f"Detected {skipped_rows} trailing invalid rows, skipping them"
            )

        new_rows_processed = rows_processed + len(vals)

        if vals and not self._is_row_valid_for_processing(vals[-1]):
            new_rows_processed = max(new_rows_processed - 1, 0)

        self._redis_client.set(ROWS_PROCESSED_KEY, str(new_rows_processed))
        self._logger.info(
            f"Fetched {len(vals)} rows from Google Sheet: {id}, range: {sheet_range_str}"
        )

        first_new_row_id = 3 + rows_processed
        applications, salaries = self._map_to_application_and_salary_models(
            valid_rows, first_new_row_id
        )

        salaries_saved_amount = 0
        applications_saved_amount = 0

        for i in range(len(applications)):
            if salaries[i][0] is not None:
                try:
                    self._db.add(salaries[i][0])
                    self._db.commit()
                    salaries_saved_amount += 1
                    self._logger.debug(
                        f"Saved applied salary with ID: {salaries[i][0].id}"
                    )
                except Exception as e:
                    msg = f"Failed to add salary applied to the DB: {e}"
                    self._logger.error(msg)
                    raise ValueError(msg)

            if salaries[i][1] is not None:
                try:
                    self._db.add(salaries[i][1])
                    self._db.commit()
                    salaries_saved_amount += 1
                    self._logger.debug(
                        f"Saved proposed salary with ID: {salaries[i][1].id}"
                    )
                except Exception as e:
                    msg = f"Failed to add salary proposed to the DB: {e}"
                    self._logger.error(msg)
                    raise ValueError(msg)

            try:
                if salaries[i][0] is not None:
                    applications[i].salary_applied_id = salaries[i][0].id
                if salaries[i][1] is not None:
                    applications[i].salary_proposed_id = salaries[i][1].id
                self._db.add(applications[i])
                self._db.commit()
                applications_saved_amount += 1
                self._logger.debug(f"Saved application with ID: {applications[i].id}")
                self._enqueue_embedding_job(applications[i])
            except Exception as e:
                msg = f"Failed to add an application to the DB: {e}"
                self._logger.error(msg)
                raise ValueError(msg)

        return applications_saved_amount, salaries_saved_amount

    def log_differences_between_sheet_and_db(
        self,
        sheet_id: str,
        sheet_page: str,
        sheet_start_row: int,
        sheet_end_row: int,
    ) -> tuple[int, int, list[dict[str, Any]]]:
        if not sheet_id:
            raise ValueError("Sheet id is required")

        if sheet_start_row < 3 or sheet_end_row < 3:
            raise ValueError("Both sheet_start_row and sheet_end_row are required")

        if sheet_end_row < sheet_start_row:
            raise ValueError(
                f"End row {sheet_end_row} must be greater or equal to start row {sheet_start_row}"
            )

        sheet_range_str = f"{sheet_page}!A{sheet_start_row}:P{sheet_end_row}"

        try:
            sheet_rows = self._sheets_service.get_values(sheet_id, sheet_range_str)
        except Exception as e:
            msg = f"Failed to read sheet range {sheet_range_str}: {e}"
            self._logger.error(msg)
            raise ValueError(msg)

        row_ids = list(range(sheet_start_row, sheet_end_row + 1))

        try:
            applications = (
                self._db.query(Application)
                .where(Application.row_id.in_(row_ids))
                .options(
                    selectinload(Application.salary_applied),
                    selectinload(Application.salary_proposed),
                )
                .order_by(Application.row_id)
                .all()
            )
        except Exception as e:
            msg = (
                f"Failed to load applications for rows {sheet_start_row}-{sheet_end_row}: {e}"
            )
            self._logger.error(msg)
            raise ValueError(msg)

        row_to_application = {application.row_id: application for application in applications}

        rows_checked = 0
        rows_with_differences = 0
        expected_rows = len(row_ids)
        diff_entries: list[dict[str, Any]] = []

        for offset in range(expected_rows):
            row_id = sheet_start_row + offset
            row_values = sheet_rows[offset] if offset < len(sheet_rows) else []
            snapshot = self._build_sheet_row_snapshot(row_values, row_id)
            application = row_to_application.get(row_id)

            self._reset_meeting_status_if_needed(
                sheet_id,
                sheet_page,
                snapshot,
                application,
            )

            field_differences, errors = self._collect_differences_for_row(
                snapshot,
                application,
            )

    # row_id: int
    # company: str | None
    # employment_type: str | None
    # work_mode: str | None
    # title: str | None
    # status: str | None
    # applied_at: datetime | None
    # responded_at: datetime | None
    # next_follow_up_at: datetime | None
    # stage: str | None
    # meta_contacts: str | None
    # meta_job_description: str | None
    # meta_notes: str | None
    # salary_applied: SalarySnapshot | None
    # salary_proposed: SalarySnapshot | None
    # errors: list[str] = field(default_factory=list)

            if field_differences or errors:
                rows_with_differences += 1

                for diff in field_differences:
                    self._logger.warning(diff.message)

                for error in errors:
                    self._logger.warning(error)

                diff_entry = self._build_diff_payload(
                    snapshot,
                    application,
                    field_differences,
                    errors,
                )
                diff_entries.append(diff_entry)

            rows_checked += 1

        return rows_checked, rows_with_differences, diff_entries

    def cleanup_overdue_meetings(
        self,
        sheet_id: str,
        sheet_page: str,
    ) -> int:
        if not sheet_id:
            raise ValueError("Sheet id is required")

        page = sheet_page or "applications_list"
        sheet_range_str = f"{page}!A{MIN_SHEET_ROW}:P{MAX_SHEET_ROW}"

        try:
            sheet_rows = self._sheets_service.get_values(sheet_id, sheet_range_str)
        except Exception as e:
            msg = f"Failed to read sheet range {sheet_range_str}: {e}"
            self._logger.error(msg)
            raise ValueError(msg)

        if not sheet_rows:
            return 0

        first_row_id = MIN_SHEET_ROW
        row_ids = list(range(first_row_id, first_row_id + len(sheet_rows)))

        try:
            applications = (
                self._db.query(Application)
                .where(Application.row_id.in_(row_ids))
                .options(
                    selectinload(Application.salary_applied),
                    selectinload(Application.salary_proposed),
                )
                .order_by(Application.row_id)
                .all()
            )
        except Exception as e:
            msg = (
                f"Failed to load applications for meeting cleanup rows "
                f"{first_row_id}-{row_ids[-1]}: {e}"
            )
            self._logger.error(msg)
            raise ValueError(msg)

        row_to_application = {application.row_id: application for application in applications}
        resets = 0

        for offset, row_id in enumerate(row_ids):
            row_values = sheet_rows[offset] if offset < len(sheet_rows) else []
            snapshot = self._build_sheet_row_snapshot(row_values, row_id)
            application = row_to_application.get(row_id)

            did_reset = self._reset_meeting_status_if_needed(
                sheet_id,
                page,
                snapshot,
                application,
            )

            if did_reset:
                resets += 1

        return resets

    def _reset_meeting_status_if_needed(
        self,
        sheet_id: str,
        sheet_page: str,
        snapshot: SheetRowSnapshot,
        application: Application | None,
    ) -> bool:
        status_value = snapshot.status or ""
        next_follow_up_at = snapshot.next_follow_up_at

        if status_value.lower() != ApplicationStatus.MEETING.value:
            return False

        if next_follow_up_at is None:
            return False

        normalized_follow_up_at = self._strip_timezone(next_follow_up_at)
        now_cmp = datetime.now()

        if normalized_follow_up_at > now_cmp:
            return False

        if application is None:
            self._logger.warning(
                "Row %s has meeting follow up in sheet but application is missing in DB",
                snapshot.row_id,
            )

            return False

        previous_status = application.status

        try:
            self._sheets_service.update_values(
                sheet_id,
                sheet_page,
                f"I{snapshot.row_id}",
                ApplicationStatus.PENDING.value,
            )
        except Exception as e:
            self._logger.error(
                "Failed to update sheet for meeting reset on row %s: %s",
                snapshot.row_id,
                e,
            )

            return False

        try:
            application.status = ApplicationStatus.PENDING.value
            self._db.add(application)
            self._db.commit()
        except Exception as e:
            self._db.rollback()
            self._logger.error(
                "Failed to update DB for meeting reset on row %s: %s",
                snapshot.row_id,
                e,
            )

            try:
                self._sheets_service.update_values(
                    sheet_id,
                    sheet_page,
                    f"I{snapshot.row_id}",
                    previous_status or "",
                )
            except Exception as restore_error:
                self._logger.warning(
                    "Failed to revert sheet after DB rollback for row %s: %s",
                    snapshot.row_id,
                    restore_error,
                )

            return False

        snapshot.status = ApplicationStatus.PENDING.value

        self._logger.info(
            "Reset meeting status to pending for row_id=%s because next_follow_up_at is in the past or now",
            snapshot.row_id,
        )

        return True

    def _collect_differences_for_row(
        self,
        sheet_snapshot: SheetRowSnapshot,
        application: Application | None,
    ) -> tuple[list[FieldDifference], list[str]]:
        errors = list(sheet_snapshot.errors)
        differences: list[FieldDifference] = []

        if application is None and not sheet_snapshot.has_values:
            return differences, errors

        if application is None:
            differences.append(
                FieldDifference(
                    field_name="application",
                    sheet_value=self._sheet_snapshot_to_payload(
                        sheet_snapshot,
                        None,
                    ),
                    db_value=None,
                    message=(
                        f"Row {sheet_snapshot.row_id}: missing application in DB"
                    ),
                ),
            )

            return differences, errors

        if not sheet_snapshot.has_values:
            differences.append(
                FieldDifference(
                    field_name="sheet_row_empty",
                    sheet_value=None,
                    db_value=self._application_to_payload(application),
                    message=(
                        f"Row {sheet_snapshot.row_id}: sheet row is empty but DB has application id={application.id}"
                    ),
                ),
            )

            return differences, errors

        self._normalize_salary_scaling(application, sheet_snapshot)

        differences.extend(
            self._compare_scalar(
                sheet_snapshot.row_id,
                "company",
                sheet_snapshot.company,
                application.company,
            )
        )
        differences.extend(
            self._compare_scalar(
                sheet_snapshot.row_id,
                "employment_type",
                sheet_snapshot.employment_type,
                application.employment_type,
            )
        )
        differences.extend(
            self._compare_scalar(
                sheet_snapshot.row_id,
                "work_mode",
                sheet_snapshot.work_mode,
                application.work_mode,
            )
        )
        differences.extend(
            self._compare_scalar(
                sheet_snapshot.row_id,
                "title",
                sheet_snapshot.title,
                application.title,
            )
        )
        differences.extend(
            self._compare_scalar(
                sheet_snapshot.row_id,
                "status",
                sheet_snapshot.status,
                application.status,
            )
        )
        differences.extend(
            self._compare_scalar(
                sheet_snapshot.row_id,
                "stage",
                sheet_snapshot.stage,
                application.stage,
            )
        )
        differences.extend(
            self._compare_datetime(
                sheet_snapshot.row_id,
                "applied_at",
                sheet_snapshot.applied_at,
                application.applied_at,
                "%d/%m/%Y",
            )
        )
        differences.extend(
            self._compare_datetime(
                sheet_snapshot.row_id,
                "responded_at",
                sheet_snapshot.responded_at,
                application.responded_at,
                "%d/%m/%Y",
            )
        )
        differences.extend(
            self._compare_datetime(
                sheet_snapshot.row_id,
                "next_follow_up_at",
                sheet_snapshot.next_follow_up_at,
                application.next_follow_up_at,
                "%d/%m/%Y %H:%M:%S",
            )
        )

        meta = application.meta or {}

        differences.extend(
            self._compare_scalar(
                sheet_snapshot.row_id,
                "meta.contacts",
                sheet_snapshot.meta_contacts,
                meta.get("contacts"),
            )
        )
        differences.extend(
            self._compare_scalar(
                sheet_snapshot.row_id,
                "meta.job_description",
                sheet_snapshot.meta_job_description,
                meta.get("job_description"),
            )
        )
        differences.extend(
            self._compare_scalar(
                sheet_snapshot.row_id,
                "meta.notes",
                sheet_snapshot.meta_notes,
                meta.get("notes"),
            )
        )

        differences.extend(
            self._compare_salary(
                sheet_snapshot.row_id,
                "salary_applied",
                sheet_snapshot.salary_applied,
                self._to_salary_snapshot(application.salary_applied),
            )
        )
        differences.extend(
            self._compare_salary(
                sheet_snapshot.row_id,
                "salary_proposed",
                sheet_snapshot.salary_proposed,
                self._to_salary_snapshot(application.salary_proposed),
            )
        )

        return differences, errors

    def _compare_scalar(
        self,
        row_id: int,
        field_name: str,
        sheet_value: Any,
        db_value: Any,
    ) -> list[FieldDifference]:
        sheet_normalized = self._normalize_scalar(sheet_value)
        db_normalized = self._normalize_scalar(db_value)

        if sheet_normalized == db_normalized:
            return []

        message = (
            f"Row {row_id}: {field_name} mismatch "
            f"(sheet={sheet_normalized!r}, db={db_normalized!r})"
        )

        difference = FieldDifference(
            field_name=field_name,
            sheet_value=sheet_normalized,
            db_value=db_normalized,
            message=message,
        )

        return [difference]

    def _compare_datetime(
        self,
        row_id: int,
        field_name: str,
        sheet_value: datetime | None,
        db_value: datetime | None,
        date_format: str,
    ) -> list[FieldDifference]:
        sheet_formatted = self._format_datetime(sheet_value, date_format)
        db_formatted = self._format_datetime(db_value, date_format)

        return self._compare_scalar(row_id, field_name, sheet_formatted, db_formatted)

    def _format_datetime(self, value: datetime | None, date_format: str) -> str | None:
        if value is None:
            return None

        normalized_value = self._strip_timezone(value)
        normalized_value = normalized_value.replace(microsecond=0)

        return normalized_value.strftime(date_format)

    def _strip_timezone(self, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value

        return value.astimezone(timezone.utc).replace(tzinfo=None)

    def _compare_salary(
        self,
        row_id: int,
        field_name: str,
        sheet_salary: SalarySnapshot | None,
        db_salary: SalarySnapshot | None,
    ) -> list[FieldDifference]:
        if sheet_salary is None and db_salary is None:
            return []

        if sheet_salary is None or db_salary is None:
            message = (
                f"Row {row_id}: {field_name} mismatch "
                f"(sheet={self._format_salary(sheet_salary)}, db={self._format_salary(db_salary)})"
            )

            diff = FieldDifference(
                field_name=field_name,
                sheet_value=self._salary_snapshot_to_payload(sheet_salary),
                db_value=self._salary_snapshot_to_payload(db_salary),
                message=message,
            )

            return [diff]

        differences: list[FieldDifference] = []

        if sheet_salary.amount_from != db_salary.amount_from:
            message = (
                f"Row {row_id}: {field_name}.amount_from mismatch "
                f"(sheet={sheet_salary.amount_from!r}, db={db_salary.amount_from!r})"
            )
            differences.append(
                FieldDifference(
                    field_name=f"{field_name}.amount_from",
                    sheet_value=self._serialize_decimal(sheet_salary.amount_from),
                    db_value=self._serialize_decimal(db_salary.amount_from),
                    message=message,
                )
            )

        if sheet_salary.amount_to != db_salary.amount_to:
            message = (
                f"Row {row_id}: {field_name}.amount_to mismatch "
                f"(sheet={sheet_salary.amount_to!r}, db={db_salary.amount_to!r})"
            )
            differences.append(
                FieldDifference(
                    field_name=f"{field_name}.amount_to",
                    sheet_value=self._serialize_decimal(sheet_salary.amount_to),
                    db_value=self._serialize_decimal(db_salary.amount_to),
                    message=message,
                )
            )

        if sheet_salary.currency != db_salary.currency:
            message = (
                f"Row {row_id}: {field_name}.currency mismatch "
                f"(sheet={sheet_salary.currency!r}, db={db_salary.currency!r})"
            )
            differences.append(
                FieldDifference(
                    field_name=f"{field_name}.currency",
                    sheet_value=sheet_salary.currency,
                    db_value=db_salary.currency,
                    message=message,
                )
            )

        if sheet_salary.period != db_salary.period:
            message = (
                f"Row {row_id}: {field_name}.period mismatch "
                f"(sheet={sheet_salary.period!r}, db={db_salary.period!r})"
            )
            differences.append(
                FieldDifference(
                    field_name=f"{field_name}.period",
                    sheet_value=sheet_salary.period,
                    db_value=db_salary.period,
                    message=message,
                )
            )

        return differences

    def _build_diff_payload(
        self,
        sheet_snapshot: SheetRowSnapshot,
        application: Application | None,
        differences: list[FieldDifference],
        errors: list[str],
    ) -> dict[str, Any]:
        application_id = application.id if application is not None else None
        diff_items = [
            {
                "field": diff.field_name,
                "sheet_value": diff.sheet_value,
                "db_value": diff.db_value,
                "message": diff.message,
            }
            for diff in differences
        ]

        sheet_payload = self._sheet_snapshot_to_payload(sheet_snapshot, application_id)
        db_snapshot = self._application_to_payload(application)

        payload = {
            "application_id": application_id,
            "row_id": sheet_snapshot.row_id,
            "company": sheet_snapshot.company
            or (application.company if application is not None else None),
            "role_title": sheet_snapshot.title
            or (application.title if application is not None else None),
            "differences": diff_items,
            "sheet_payload": sheet_payload,
            "db_snapshot": db_snapshot,
            "errors": errors,
        }

        return payload

    def _sheet_snapshot_to_payload(
        self,
        snapshot: SheetRowSnapshot,
        application_id: int | None,
    ) -> dict[str, Any]:
        meta = {
            "contacts": snapshot.meta_contacts,
            "job_description": snapshot.meta_job_description,
            "notes": snapshot.meta_notes,
        }

        payload = {
            "application_id": application_id,
            "row_id": snapshot.row_id,
            "company": snapshot.company,
            "employment_type": snapshot.employment_type,
            "work_mode": snapshot.work_mode,
            "title": snapshot.title,
            "status": snapshot.status,
            "stage": snapshot.stage,
            "applied_at": self._serialize_datetime(snapshot.applied_at),
            "responded_at": self._serialize_datetime(snapshot.responded_at),
            "next_follow_up_at": self._serialize_datetime(snapshot.next_follow_up_at),
            "meta": meta,
            "salary_applied": self._salary_snapshot_to_payload(snapshot.salary_applied),
            "salary_proposed": self._salary_snapshot_to_payload(
                snapshot.salary_proposed
            ),
        }

        return payload

    def _application_to_payload(
        self,
        application: Application | None,
    ) -> dict[str, Any] | None:
        if application is None:
            return None

        payload = {
            "application_id": application.id,
            "row_id": application.row_id,
            "company": application.company,
            "employment_type": application.employment_type,
            "work_mode": application.work_mode,
            "title": application.title,
            "status": application.status,
            "stage": application.stage,
            "applied_at": self._serialize_datetime(application.applied_at),
            "responded_at": self._serialize_datetime(application.responded_at),
            "next_follow_up_at": self._serialize_datetime(
                application.next_follow_up_at
            ),
            "meta": application.meta or {},
            "salary_applied": self._salary_snapshot_to_payload(
                self._to_salary_snapshot(application.salary_applied)
            ),
            "salary_proposed": self._salary_snapshot_to_payload(
                self._to_salary_snapshot(application.salary_proposed)
            ),
        }

        return payload

    def _salary_snapshot_to_payload(
        self,
        snapshot: SalarySnapshot | None,
    ) -> dict[str, Any] | None:
        if snapshot is None:
            return None

        payload = {
            "amount_from": self._serialize_decimal(snapshot.amount_from),
            "amount_to": self._serialize_decimal(snapshot.amount_to),
            "currency": snapshot.currency,
            "period": snapshot.period,
        }

        return payload

    def _serialize_decimal(self, value: Decimal | None) -> str | None:
        if value is None:
            return None

        return format(value, "f")

    def _extract_decimal(self, value: Any) -> Decimal | None:
        if value is None:
            return None

        if isinstance(value, Decimal):
            return value

        if isinstance(value, (int, float)):
            return Decimal(str(value))

        if isinstance(value, str):
            normalized = value.strip()

            if not normalized:
                return None

            try:
                decimal_value = Decimal(normalized)
            except Exception as e:
                msg = f"Invalid salary amount value: {value}"
                self._logger.error(msg)
                raise ValueError(msg) from e

            return decimal_value

        msg = f"Unsupported salary amount type: {type(value)}"
        self._logger.error(msg)
        raise ValueError(msg)

    def _normalize_salary_text(self, value: Any) -> str | None:
        if value is None:
            return None

        if isinstance(value, str):
            normalized = value.strip()

            return normalized or None

        return str(value)

    def _serialize_datetime(self, value: datetime | None) -> str | None:
        if value is None:
            return None

        normalized = self._strip_timezone(value)
        normalized = normalized.replace(microsecond=0)
        result = normalized.isoformat()

        return result

    def _format_salary(self, snapshot: SalarySnapshot | None) -> str:
        if snapshot is None:
            return "None"

        amount_part: str | None = None

        if snapshot.amount_from is not None and snapshot.amount_to is not None:
            if snapshot.amount_from == snapshot.amount_to:
                amount_part = f"{snapshot.amount_from}"
            else:
                amount_part = f"{snapshot.amount_from}-{snapshot.amount_to}"
        elif snapshot.amount_from is not None:
            amount_part = f"from {snapshot.amount_from}"
        elif snapshot.amount_to is not None:
            amount_part = f"to {snapshot.amount_to}"

        parts: list[str] = []

        if amount_part:
            parts.append(amount_part)

        if snapshot.currency:
            parts.append(snapshot.currency)

        if snapshot.period:
            parts.append(snapshot.period)

        return " ".join(parts) if parts else "empty"

    def _to_salary_snapshot(self, salary: Salary | None) -> SalarySnapshot | None:
        if salary is None:
            return None

        return SalarySnapshot(
            amount_from=salary.amount_from,
            amount_to=salary.amount_to,
            currency=salary.currency,
            period=salary.period,
        )

    def _build_sheet_row_snapshot(
        self,
        row: list[Any],
        row_id: int,
    ) -> SheetRowSnapshot:
        errors: list[str] = []
        has_values = self._has_row_values(row)

        company = self._get_cell(row, 0)
        employment_type = self._get_cell(row, 1)
        work_mode = self._get_cell(row, 2)
        title = self._get_cell(row, 3)
        status = self._get_cell(row, 8)

        applied_at = self._parse_sheet_date(
            self._get_cell(row, 9),
            "%d/%m/%Y",
            errors,
            row_id,
            "applied_at",
        )
        responded_at = self._parse_sheet_date(
            self._get_cell(row, 10),
            "%d/%m/%Y",
            errors,
            row_id,
            "responded_at",
        )
        next_follow_up_at = self._parse_sheet_date(
            self._get_cell(row, 11),
            "%d/%m/%Y %H:%M:%S",
            errors,
            row_id,
            "next_follow_up_at",
        )

        stage = self._get_cell(row, 12)
        meta_contacts = self._get_cell(row, 13)
        meta_job_description = self._get_cell(row, 14)
        meta_notes = self._get_cell(row, 15)

        currency = self._get_cell(row, 6) or "USD"
        period = self._get_cell(row, 7) or "yearly"

        salary_applied = self._build_salary_snapshot(
            self._get_cell(row, 4),
            currency,
            period,
            errors,
            row_id,
            "salary_applied",
        )
        salary_proposed = self._build_salary_snapshot(
            self._get_cell(row, 5),
            currency,
            period,
            errors,
            row_id,
            "salary_proposed",
        )

        return SheetRowSnapshot(
            row_id=row_id,
            has_values=has_values,
            company=company,
            employment_type=employment_type,
            work_mode=work_mode,
            title=title,
            status=status,
            applied_at=applied_at,
            responded_at=responded_at,
            next_follow_up_at=next_follow_up_at,
            stage=stage,
            meta_contacts=meta_contacts,
            meta_job_description=meta_job_description,
            meta_notes=meta_notes,
            salary_applied=salary_applied,
            salary_proposed=salary_proposed,
            errors=errors,
        )

    def _has_row_values(self, row: list[Any]) -> bool:
        for value in row:
            if value is None:
                continue

            if isinstance(value, str):
                if value.strip():
                    return True

                continue

            return True

        return False

    def _get_cell(self, row: list[Any], index: int) -> str | None:
        if index >= len(row):
            return None

        value = row[index]

        if value is None:
            return None

        if isinstance(value, str):
            normalized = value.strip()

            return normalized or None

        normalized = str(value).strip()

        return normalized or None

    def _parse_sheet_date(
        self,
        value: str | None,
        fmt: str,
        errors: list[str],
        row_id: int,
        field_name: str,
    ) -> datetime | None:
        if value is None:
            return None

        text = value.strip()
        if not text:
            return None

        iso_candidate = text
        if iso_candidate.endswith("Z"):
            iso_candidate = f"{iso_candidate[:-1]}+00:00"

        try:
            parsed_iso = datetime.fromisoformat(iso_candidate)

            return parsed_iso
        except Exception:
            pass

        formats: list[str] = []

        def _register_format(pattern: str) -> None:
            if pattern not in formats:
                formats.append(pattern)

        _register_format(fmt)
        _register_format("%Y-%m-%d %H:%M:%S")
        _register_format("%Y-%m-%d")
        _register_format("%d/%m/%Y %H:%M:%S")
        _register_format("%d/%m/%Y")

        for pattern in formats:
            try:
                parsed = datetime.strptime(text, pattern)

                return parsed
            except Exception:
                continue

        errors.append(
            f"Row {row_id}: unable to parse {field_name}='{value}'",
        )

        return None

    def _build_salary_snapshot(
        self,
        value: str | None,
        currency: str,
        period: str,
        errors: list[str],
        row_id: int,
        field_name: str,
    ) -> SalarySnapshot | None:
        if value is None:
            return None

        try:
            salary = self._parse_salary(value)
        except ValueError:
            errors.append(
                f"Row {row_id}: unable to parse {field_name}='{value}'",
            )

            return None

        if salary is None:
            return None

        return SalarySnapshot(
            amount_from=salary.amount_from,
            amount_to=salary.amount_to,
            currency=currency,
            period=period,
        )

    def _normalize_scalar(self, value: Any) -> Any:
        if isinstance(value, str):
            stripped = value.strip()

            return stripped or None

        return value

    def get_last_processed_row(self, *, add_header_rows: bool = True) -> int:
        try:
            value = self._redis_client.get(ROWS_PROCESSED_KEY)
        except Exception as e:
            msg = f"Failed to read last processed row from Redis: {e}"
            self._logger.error(msg)
            raise ValueError(msg)

        if value is None:
            rows_processed = 0
        else:
            try:
                rows_processed = int(value)
            except (TypeError, ValueError) as e:
                msg = f"Invalid last processed row value in Redis: {value} | {e}"
                self._logger.error(msg)
                raise ValueError(msg)

        self._logger.debug(
            "Retrieved last processed row from Redis: %s",
            rows_processed,
        )

        if add_header_rows:
            rows_processed += 2  # for header rows

        return rows_processed

    def _enqueue_embedding_job(self, application: Application) -> None:
        if application.id is None:
            self._logger.debug(
                "Skipping embedding job because application ID is missing",
            )

            return

        payload = (
            f"COMPANY {application.company} "
            f"TITLE {application.title} "
            f"EMPLOYMENT TYPE {application.employment_type} "
            f"WORK MODE {application.work_mode} "
            f"META {application.meta}"
        )
        normalized_payload = normalize_content(payload)

        self._embedding_publisher.enqueue(
            application_id=application.id,
            payload=normalized_payload,
        )

    def _map_to_application_and_salary_models(
        self,
        vals: list[list[Any]],
        first_new_row_id: int,
    ) -> tuple[list[Application], list[tuple[Salary | None, Salary | None]]]:
        applications: list[Application] = []
        salaries: list[tuple[Salary | None, Salary | None]] = []

        first_new_row_id -= 1
        for row in vals:
            first_new_row_id += 1

            if not self._is_row_valid_for_processing(row, first_new_row_id):
                continue

            row_len = len(row)
            self._logger.debug(f"Processing row with length {row_len}: {row}")

            application = Application(
                company=row[0],
                employment_type=row[1],
                work_mode=row[2],
                title=row[3],
                status=row[8],
                applied_at=datetime.strptime(row[9], "%d/%m/%Y"),
                responded_at=(
                    datetime.strptime(row[10], "%d/%m/%Y")
                    if row_len >= 11 and row[10] not in ("", None)
                    else None
                ),
                next_follow_up_at=(
                    datetime.strptime(row[11], "%d/%m/%Y %H:%M:%S")
                    if row_len >= 12 and row[11] not in ("", None)
                    else None
                ),
                stage=row[12] if row_len >= 13 and row[12] not in ("", None) else None,
                meta={
                    "contacts": row[13]
                    if row_len >= 14 and row[13] not in ("", None)
                    else None,
                    "job_description": row[14]
                    if row_len >= 15 and row[14] not in ("", None)
                    else None,
                    "notes": row[15]
                    if row_len >= 16 and row[15] not in ("", None)
                    else None,
                },
                row_id=first_new_row_id,
            )

            salary_applied = (
                self._parse_salary(row[4])
                if row_len >= 5 and row[4] not in ("", None)
                else None
            )
            salary_offered = (
                self._parse_salary(row[5])
                if row_len >= 6 and row[5] not in ("", None)
                else None
            )

            currency = (
                row[6] if row_len >= 7 and row[6] not in ("", None) else "USD"
            )
            period = (
                row[7] if row_len >= 8 and row[7] not in ("", None) else "yearly"
            )

            if salary_applied:
                salary_applied.currency = currency
                salary_applied.period = period
            if salary_offered:
                salary_offered.currency = currency
                salary_offered.period = period

            applications.append(application)
            salaries.append((salary_applied, salary_offered))

        return applications, salaries

    def _filter_trailing_invalid_rows(
        self, rows: list[list[Any]]
    ) -> tuple[list[list[Any]], int]:
        filtered_rows = rows.copy()
        skipped_count = 0

        while filtered_rows:
            last_row = filtered_rows[-1]

            if self._is_row_valid_for_processing(last_row):
                break

            filtered_rows.pop()
            skipped_count += 1

        return filtered_rows, skipped_count

    def _is_row_valid_for_processing(
        self,
        row: list[Any],
        row_number: int | None = None,
    ) -> bool:
        row_label = f"Row {row_number}" if row_number is not None else "Row"

        if not row:
            self._logger.warning(f"{row_label} is empty, skipping")
            return False

        required_columns = [
            (0, "company"),
            (1, "employment_type"),
            (2, "work_mode"),
            (3, "title"),
            (8, "status"),
            (9, "applied_at"),
        ]

        for index, field_name in required_columns:
            if len(row) <= index:
                self._logger.warning(
                    f"{row_label} missing column for {field_name}, skipping: {row}"
                )

                return False

            cell_value = row[index]

            if cell_value is None:
                self._logger.warning(
                    f"{row_label} has empty {field_name}, skipping: {row}"
                )

                return False

            if isinstance(cell_value, str) and cell_value.strip() == "":
                self._logger.warning(
                    f"{row_label} has empty {field_name}, skipping: {row}"
                )

                return False

        return True

    def _parse_salary(self, val: str) -> Salary | None:
        if not val:
            return None

        stripped = val.strip()
        if not stripped:
            return None

        lowered = stripped.lower()
        has_k_suffix = "k" in lowered
        normalized = lowered.replace("k", "").replace(" ", "").replace(",", "")
        parts = [part for part in normalized.split("-") if part]

        if not parts:
            msg = f"Invalid salary value: {val}"
            self._logger.error(msg)
            raise ValueError(msg)

        multiplier = Decimal(1000 if has_k_suffix else 1)

        try:
            if len(parts) == 2:
                amount_from = Decimal(parts[0]) * multiplier
                amount_to = Decimal(parts[1]) * multiplier
            else:
                amount = Decimal(parts[0]) * multiplier
                amount_from = amount
                amount_to = amount
        except Exception as e:
            msg = f"Invalid salary value: {val} | {e}"
            self._logger.error(msg)
            raise ValueError(msg) from e

        salary = Salary(
            amount_from=amount_from,
            amount_to=amount_to,
        )

        return salary

    def _normalize_salary_scaling(
        self,
        application: Application,
        sheet_snapshot: SheetRowSnapshot,
    ) -> None:
        if application is None:
            return

        changed_entities: list[Salary] = []

        if self._normalize_salary_component(
            application.salary_applied,
            sheet_snapshot.salary_applied,
            "salary_applied",
            application.id,
            sheet_snapshot.row_id,
        ):
            if application.salary_applied is not None:
                changed_entities.append(application.salary_applied)

        if self._normalize_salary_component(
            application.salary_proposed,
            sheet_snapshot.salary_proposed,
            "salary_proposed",
            application.id,
            sheet_snapshot.row_id,
        ):
            if application.salary_proposed is not None:
                changed_entities.append(application.salary_proposed)

        if not changed_entities:
            return

        try:
            for entity in changed_entities:
                self._db.add(entity)

            self._db.commit()
        except Exception as e:
            self._db.rollback()
            msg = (
                f"Failed to normalize salary scaling for application id={application.id}: {e}"
            )
            self._logger.error(msg)
            raise ValueError(msg)

    def _normalize_salary_component(
        self,
        db_salary: Salary | None,
        sheet_salary: SalarySnapshot | None,
        label: str,
        application_id: int | None,
        row_id: int,
    ) -> bool:
        if db_salary is None or sheet_salary is None:
            return False

        changed = False

        if self._should_downscale_by_thousand(
            db_salary.amount_from,
            sheet_salary.amount_from,
        ):
            db_salary.amount_from = sheet_salary.amount_from
            changed = True

        if self._should_downscale_by_thousand(
            db_salary.amount_to,
            sheet_salary.amount_to,
        ):
            db_salary.amount_to = sheet_salary.amount_to
            changed = True

        if changed:
            self._logger.info(
                "Normalized %s for application id=%s row_id=%s due to x1000 mismatch",
                label,
                application_id,
                row_id,
            )

        return changed

    def _should_downscale_by_thousand(
        self,
        db_value: Decimal | None,
        sheet_value: Decimal | None,
    ) -> bool:
        if db_value is None or sheet_value is None:
            return False

        if sheet_value == 0:
            return False

        thousand = Decimal(1000)
        expected = sheet_value * thousand

        return db_value == expected

    def list_(
        self,
        application_status_include: list[ApplicationStatus] | None,
        application_status_exclude: list[ApplicationStatus] | None,
        is_reply_email_received: bool,
    ) -> list[dict]:
        try:
            applications = self._get_applications_list(
                application_status_include,
                application_status_exclude,
                is_reply_email_received,
            )
            self._logger.info(f"Fetched {len(applications)} applications from DB")
            count_embeddings_added, applications = self._add_embeddings_to_applications(applications)
            self._logger.info(f"Added embeddings to {count_embeddings_added} applications")
            return [self._application_to_dict(app) for app in applications]
        except Exception as e:
            msg = f"Error listing newest applications: {e}"
            self._logger.error(msg)
            raise ValueError(msg)

    def _get_applications_list(
        self,
        application_status_include: list[ApplicationStatus] | None,
        application_status_exclude: list[ApplicationStatus] | None,
        is_reply_email_received: bool,
    ) -> list[Application]:
        try:
            q = self._db.query(Application)

            if application_status_exclude is not None:
                q = q.where(Application.status.notin_(application_status_exclude))
            elif application_status_include is not None:
                q = q.where(Application.status.in_(application_status_include))
                match application_status_include:
                    case ApplicationStatus.APPLIED:
                        reply_received = and_(
                            Application.applied_email_received.isnot(None),
                            Application.applied_email_id.isnot(None),
                        )

                        if is_reply_email_received:
                            q = q.where(reply_received)
                        else:
                            q = q.where(not_(reply_received))
                    case ApplicationStatus.DENIED:
                        reply_received = and_(
                            Application.denied_email_received.isnot(None),
                            Application.denied_email_id.isnot(None),
                        )

                        if is_reply_email_received:
                            q = q.where(reply_received)
                        else:
                            q = q.where(not_(reply_received))
                    case _:
                        self._logger.debug(
                            f"Filtering by is_reply_email_received is not supported for status: {application_status_include}"
                        )

            return q.all()
        except Exception as e:
            msg = f"Error fetching newest applications: {e}"
            self._logger.error(msg)
            raise ValueError(msg)

    def _add_embeddings_to_applications(
        self, applications: list[Application]
    ) -> tuple[int, list[Application]]:
        count_embeddings_added: int = 0

        for application in applications:
            if application.embedding is not None:
                self._logger.debug(
                    f"Embedding already exists for application {application.id}, skipping generation"
                )

                continue

            try:
                application_as_str = f"COMPANY {application.company} TITLE {application.title} EMPLOYMENT TYPE {application.employment_type} WORK MODE {application.work_mode} META {application.meta}"
                application_as_str = normalize_content(application_as_str)
                self._logger.info(
                    f"application_as_str normalized: {application_as_str}"
                )

                application.embedding = self._openai_service.get_embedding(
                    application_as_str
                )

                self._db.add(application)
                count_embeddings_added += 1
            except Exception as e:
                msg = f"Error adding embedding for application {application.id}: {e}"
                self._logger.error(msg)
                raise ValueError(msg)

        try:
            self._db.commit()
        except Exception as e:
            msg = f"Failed to commit DB changes: {e}"
            self._logger.error(msg)
            raise ValueError(msg)

        return count_embeddings_added, applications

    def _application_to_dict(self, application: Application) -> dict:
        salary_applied: Salary | None = None
        salary_proposed: Salary | None = None
        embedding: list[float] | None = None

        if application.salary_applied_id:
            try:
                salary_applied = (
                    self._db.query(Salary)
                    .filter(Salary.id == application.salary_applied_id)
                    .first()
                )
            except Exception as e:
                msg = f"Error fetching salary_applied: {e}"
                self._logger.error(msg)
                raise ValueError(msg)

        if application.salary_proposed_id:
            try:
                salary_proposed = (
                    self._db.query(Salary)
                    .filter(Salary.id == application.salary_proposed_id)
                    .first()
                )
            except Exception as e:
                msg = f"Error fetching salary_proposed: {e}"
                self._logger.error(msg)
                raise ValueError(msg)

        if application.embedding is not None:
            embedding = [float(value) for value in application.embedding]

        return {
            "id": application.id,
            "created_at": application.created_at.isoformat()
            if application.created_at
            else None,
            "updated_at": application.updated_at.isoformat()
            if application.updated_at
            else None,
            "company": application.company,
            "title": application.title,
            "employment_type": application.employment_type,
            "work_mode": application.work_mode,
            "status": application.status,
            "applied_at": application.applied_at.isoformat()
            if application.applied_at
            else None,
            "responded_at": application.responded_at.isoformat()
            if application.responded_at
            else None,
            "next_follow_up_at": application.next_follow_up_at.isoformat()
            if application.next_follow_up_at
            else None,
            "stage": application.stage,
            "meta": application.meta,
            "embedding": embedding,
            "row_id": application.row_id,
            "applied_email_received": application.applied_email_received.isoformat()
            if application.applied_email_received
            else None,
            "applied_email_id": application.applied_email_id,
            "denied_email_received": application.denied_email_received.isoformat()
            if application.denied_email_received
            else None,
            "denied_email_id": application.denied_email_id,
            "meeting_inv_email_received": application.meeting_inv_email_received.isoformat()
            if application.meeting_inv_email_received
            else None,
            "meeting_inv_email_id": application.meeting_inv_email_id,
            "meeting_crt_email_received": application.meeting_crt_email_received.isoformat()
            if application.meeting_crt_email_received
            else None,
            "meeting_crt_email_id": application.meeting_crt_email_id,
            "meeting_upd_email_received": application.meeting_upd_email_received.isoformat()
            if application.meeting_upd_email_received
            else None,
            "meeting_upd_email_id": application.meeting_upd_email_id,
            "meeting_cncl_email_received": application.meeting_cncl_email_received.isoformat()
            if application.meeting_cncl_email_received
            else None,
            "meeting_cncl_email_id": application.meeting_cncl_email_id,
            "salary_applied": salary_applied.to_dict() if salary_applied else None,
            "salary_proposed": salary_proposed.to_dict() if salary_proposed else None,
        }

    def update(
        self,
        dto: dict,
    ) -> None:
        resolved_sheet_id = Config().SHEET_ID
        if not resolved_sheet_id:
            msg = "Sheet id is required (SHEET_ID env is missing)"
            self._logger.error(msg)
            raise ValueError(msg)

        def _parse_dt(val) -> None | datetime:
            if val is None:
                return None

            if isinstance(val, datetime):
                return val

            if isinstance(val, (int, float)):
                try:
                    timestamp = float(val)
                except Exception:
                    return None

                try:
                    return datetime.fromtimestamp(timestamp, tz=timezone.utc)
                except Exception:
                    return None

            if isinstance(val, str):
                text = val.strip()
                if not text:
                    return None

                iso_candidate = text
                if iso_candidate.endswith("Z"):
                    iso_candidate = f"{iso_candidate[:-1]}+00:00"

                try:
                    return datetime.fromisoformat(iso_candidate)
                except Exception:
                    pass

                if text.isdigit():
                    try:
                        return datetime.fromtimestamp(int(text), tz=timezone.utc)
                    except Exception:
                        pass
                else:
                    try:
                        numeric_value = float(text)
                    except Exception:
                        numeric_value = None

                    if numeric_value is not None:
                        try:
                            return datetime.fromtimestamp(numeric_value, tz=timezone.utc)
                        except Exception:
                            pass

                for fmt in (
                    "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%dT%H:%M:%S",
                    "%d/%m/%Y %H:%M:%S",
                    "%d/%m/%Y",
                ):
                    try:
                        return datetime.strptime(text, fmt)
                    except Exception:
                        continue

            return None

        try:
            app_id = dto.get("application_id")
            row_id = dto.get("row_id")
            if app_id is None or row_id is None:
                raise ValueError(
                    "Both application_id (or id) and row_id are required in dto"
                )

            application: Application | None = (
                self._db.query(Application)
                .where(Application.id == app_id, Application.row_id == row_id)
                .first()
            )
            if application is None:
                raise ValueError(
                    f"Application not found by id={app_id} or row_id={row_id}"
                )

            if "company" in dto:
                application.company = dto.get("company")
            if "employment_type" in dto:
                application.employment_type = dto.get("employment_type")
            if "work_mode" in dto:
                application.work_mode = dto.get("work_mode")
            if "title" in dto:
                application.title = dto.get("title")

            if "status" in dto:
                application.status = dto["status"]
            if "applied_at" in dto:
                parsed_applied_at = _parse_dt(dto.get("applied_at"))
                if parsed_applied_at is None:
                    msg = "parsed_applied_at is None"
                    self._logger.error(msg)
                    raise ValueError(msg)
                application.applied_at = parsed_applied_at
            if "responded_at" in dto:
                application.responded_at = _parse_dt(dto.get("responded_at"))
            if "next_follow_up_at" in dto:
                application.next_follow_up_at = _parse_dt(dto.get("next_follow_up_at"))
            if "stage" in dto:
                application.stage = dto.get("stage")
            if "meta" in dto:
                application.meta = dto.get("meta")

            if "applied_email_received" in dto:
                application.applied_email_received = _parse_dt(
                    dto.get("applied_email_received")
                )
            if "applied_email_id" in dto:
                application.applied_email_id = dto.get("applied_email_id")
            if "denied_email_received" in dto:
                application.denied_email_received = _parse_dt(
                    dto.get("denied_email_received")
                )
            if "denied_email_id" in dto:
                application.denied_email_id = dto.get("denied_email_id")
            if "meeting_inv_email_received" in dto:
                application.meeting_inv_email_received = _parse_dt(
                    dto.get("meeting_inv_email_received")
                )
            if "meeting_inv_email_id" in dto:
                application.meeting_inv_email_id = dto.get("meeting_inv_email_id")
            if "meeting_crt_email_received" in dto:
                application.meeting_crt_email_received = _parse_dt(
                    dto.get("meeting_crt_email_received")
                )
            if "meeting_crt_email_id" in dto:
                application.meeting_crt_email_id = dto.get("meeting_crt_email_id")
            if "meeting_upd_email_received" in dto:
                application.meeting_upd_email_received = _parse_dt(
                    dto.get("meeting_upd_email_received")
                )
            if "meeting_upd_email_id" in dto:
                application.meeting_upd_email_id = dto.get("meeting_upd_email_id")
            if "meeting_cncl_email_received" in dto:
                application.meeting_cncl_email_received = _parse_dt(
                    dto.get("meeting_cncl_email_received")
                )
            if "meeting_cncl_email_id" in dto:
                application.meeting_cncl_email_id = dto.get("meeting_cncl_email_id")

            applied_update_requested = any(
                key in dto
                for key in (
                    "salary_applied_from",
                    "salary_applied_to",
                    "salary_currency",
                    "salary_period",
                )
            )

            salary_applied: Salary | None = None
            if application.salary_applied_id is not None:
                salary_applied = (
                    self._db.query(Salary)
                    .where(Salary.id == application.salary_applied_id)
                    .first()
                )

            if applied_update_requested:
                applied_from_value = (
                    self._extract_decimal(dto["salary_applied_from"])
                    if "salary_applied_from" in dto
                    else None
                )
                applied_to_value = (
                    self._extract_decimal(dto["salary_applied_to"])
                    if "salary_applied_to" in dto
                    else None
                )
                applied_amount_present = (
                    applied_from_value is not None or applied_to_value is not None
                )

                if salary_applied is None:
                    if applied_amount_present:
                        applied_currency = (
                            self._normalize_salary_text(dto.get("salary_currency")) or "USD"
                        )
                        applied_period = (
                            self._normalize_salary_text(dto.get("salary_period")) or "yearly"
                        )
                        salary_applied = Salary(
                            amount_from=cast(Any, applied_from_value),
                            amount_to=cast(Any, applied_to_value),
                            currency=applied_currency,
                            period=applied_period,
                        )
                        self._db.add(salary_applied)
                        self._db.flush()
                        application.salary_applied_id = salary_applied.id
                elif salary_applied is not None:
                    if "salary_applied_from" in dto:
                        salary_applied.amount_from = cast(Any, applied_from_value)
                    if "salary_applied_to" in dto:
                        salary_applied.amount_to = cast(Any, applied_to_value)

                    if "salary_currency" in dto:
                        applied_currency = self._normalize_salary_text(dto.get("salary_currency"))
                        if applied_currency is not None:
                            salary_applied.currency = applied_currency

                    if "salary_period" in dto:
                        applied_period = self._normalize_salary_text(dto.get("salary_period"))
                        if applied_period is not None:
                            salary_applied.period = applied_period

                    should_remove_applied = (
                        "salary_applied_from" in dto
                        and "salary_applied_to" in dto
                        and applied_from_value is None
                        and applied_to_value is None
                        and "salary_currency" not in dto
                        and "salary_period" not in dto
                    )

                    if should_remove_applied:
                        application.salary_applied_id = None
                        self._db.delete(salary_applied)
                    else:
                        self._db.add(salary_applied)

            proposed_update_requested = any(
                key in dto
                for key in (
                    "salary_offered_from",
                    "salary_offered_to",
                    "salary_currency",
                    "salary_period",
                )
            )

            salary_proposed: Salary | None = None
            if application.salary_proposed_id is not None:
                salary_proposed = (
                    self._db.query(Salary)
                    .where(Salary.id == application.salary_proposed_id)
                    .first()
                )

            if proposed_update_requested:
                offered_from_value = (
                    self._extract_decimal(dto["salary_offered_from"])
                    if "salary_offered_from" in dto
                    else None
                )
                offered_to_value = (
                    self._extract_decimal(dto["salary_offered_to"])
                    if "salary_offered_to" in dto
                    else None
                )
                offered_amount_present = (
                    offered_from_value is not None or offered_to_value is not None
                )

                if salary_proposed is None:
                    if offered_amount_present:
                        offered_currency = (
                            self._normalize_salary_text(dto.get("salary_currency")) or "USD"
                        )
                        offered_period = (
                            self._normalize_salary_text(dto.get("salary_period")) or "yearly"
                        )
                        salary_proposed = Salary(
                            amount_from=cast(Any, offered_from_value),
                            amount_to=cast(Any, offered_to_value),
                            currency=offered_currency,
                            period=offered_period,
                        )
                        self._db.add(salary_proposed)
                        self._db.flush()
                        application.salary_proposed_id = salary_proposed.id
                elif salary_proposed is not None:
                    if "salary_offered_from" in dto:
                        salary_proposed.amount_from = cast(Any, offered_from_value)
                    if "salary_offered_to" in dto:
                        salary_proposed.amount_to = cast(Any, offered_to_value)

                    if "salary_currency" in dto:
                        offered_currency = self._normalize_salary_text(dto.get("salary_currency"))
                        if offered_currency is not None:
                            salary_proposed.currency = offered_currency

                    if "salary_period" in dto:
                        offered_period = self._normalize_salary_text(dto.get("salary_period"))
                        if offered_period is not None:
                            salary_proposed.period = offered_period

                    should_remove_proposed = (
                        "salary_offered_from" in dto
                        and "salary_offered_to" in dto
                        and offered_from_value is None
                        and offered_to_value is None
                        and "salary_currency" not in dto
                        and "salary_period" not in dto
                    )

                    if should_remove_proposed:
                        application.salary_proposed_id = None
                        self._db.delete(salary_proposed)
                    else:
                        self._db.add(salary_proposed)

            self._db.add(application)
            self._db.commit()
            self._logger.info(
                f"Updated application id={application.id} (row_id={application.row_id})"
            )
        except Exception as e:
            self._db.rollback()
            msg = f"Failed to update application: {e}"
            self._logger.error(msg)
            raise ValueError(msg)

        self._update_on_remote(resolved_sheet_id, dto)

    def _update_on_remote(self, sheet_id: str, dto: dict) -> None:
        row_id = dto.get("row_id")
        if row_id is None:
            return

        def _update_salary_cell(
            column: str,
            amount_from: Any,
            amount_to: Any,
        ) -> None:
            payload = {
                "amount_from": amount_from,
                "amount_to": amount_to,
            }
            salary_text = self._salary_payload_to_sheet_text(payload)

            self._sheets_service.update_values(
                sheet_id,
                "applications_list",
                f"{column}{row_id}",
                salary_text,
            )

        if "salary_applied_from" in dto or "salary_applied_to" in dto:
            _update_salary_cell(
                "E",
                dto.get("salary_applied_from"),
                dto.get("salary_applied_to"),
            )

        if "salary_offered_from" in dto or "salary_offered_to" in dto:
            _update_salary_cell(
                "F",
                dto.get("salary_offered_from"),
                dto.get("salary_offered_to"),
            )

        if "salary_currency" in dto:
            currency_val = self._normalize_salary_text(dto.get("salary_currency")) or ""
            self._sheets_service.update_values(
                sheet_id,
                "applications_list",
                f"G{row_id}",
                currency_val,
            )

        if "salary_period" in dto:
            period_val = self._normalize_salary_text(dto.get("salary_period")) or ""
            self._sheets_service.update_values(
                sheet_id,
                "applications_list",
                f"H{row_id}",
                period_val,
            )

        if "status" in dto:
            ceil = f"I{row_id}"
            val = dto["status"]
            self._sheets_service.update_values(
                sheet_id,
                "applications_list",
                ceil,
                val,
            )
            self._logger.info("Sheet update status row=%s val=%s", row_id, val)

        if "responded_at" in dto:
            from datetime import datetime

            ceil = f"K{row_id}"
            responded_at_val = dto.get("responded_at")
            if isinstance(responded_at_val, datetime):
                val = responded_at_val.strftime("%d/%m/%Y")
            elif isinstance(responded_at_val, str):
                # Attempt to parse and format; fallback to original string
                try:
                    for fmt in (
                        "%Y-%m-%dT%H:%M:%S.%fZ",
                        "%Y-%m-%dT%H:%M:%S%z",
                        "%Y-%m-%dT%H:%M:%S",
                        "%d/%m/%Y %H:%M:%S",
                        "%d/%m/%Y",
                    ):
                        try:
                            val = datetime.strptime(responded_at_val, fmt).strftime(
                                "%d/%m/%Y"
                            )
                            break
                        except Exception:
                            continue
                    else:
                        val = responded_at_val
                except Exception:
                    val = responded_at_val
            else:
                val = ""
            self._sheets_service.update_values(
                sheet_id,
                "applications_list",
                ceil,
                val,
            )
            self._logger.info("Sheet update responded_at row=%s val=%s", row_id, val)

        if "stage" in dto:
            ceil = f"M{row_id}"
            stage_val = dto.get("stage")
            try:
                val = str(int(stage_val))
            except Exception:
                val = "" if stage_val is None else str(stage_val)
            self._sheets_service.update_values(
                sheet_id,
                "applications_list",
                ceil,
                val,
            )
            self._logger.info("Sheet update stage row=%s val=%s", row_id, val)

        ceil = f"L{row_id}"
        dt: Any = None

        if "next_follow_up_at" in dto:
            dt = dto.get("next_follow_up_at")
        else:
            ics_list = dto.get("ics_files") or []
            req_ics = next(
                (i for i in ics_list if (i or {}).get("method") == "REQUEST"),
                None,
            )
            if req_ics and isinstance(req_ics, dict):
                content = req_ics.get("content") or ""

                self._logger.info(f"Found content for meeting creation: {content}")

                try:
                    cal = Calendar.from_ical(content)
                    event = next((c for c in cal.walk("vevent")), None)
                    if event is not None:
                        dtstart = event.get("dtstart")
                        if dtstart is not None:
                            dt = getattr(dtstart, "dt", dtstart)
                except Exception:
                    self._logger.warning("Failed to parse ICS content for meeting creation")

        if isinstance(dt, str):
            try:
                dt_text = dt.strip()
                if dt_text.endswith("Z"):
                    dt_text = f"{dt_text[:-1]}+00:00"

                dt = datetime.fromisoformat(dt_text)
            except Exception:
                try:
                    dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    self._logger.warning("Failed to parse next_follow_up_at string '%s'", dt)
                    dt = None

        if dt is not None:
            from datetime import datetime

            if isinstance(dt, datetime):
                meeting_dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                meeting_dt_str = datetime.combine(dt, datetime.min.time()).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )

            val = meeting_dt_str
            self._sheets_service.update_values(
                sheet_id,
                "applications_list",
                ceil,
                val,
            )
            self._logger.info("Sheet update next_follow_up_at row=%s val=%s", row_id, val)

    def update_external(
        self,
        sheet_page: str,
        db_snapshot: dict[str, Any],
    ) -> None:
        resolved_sheet_id = Config().SHEET_ID
        if not resolved_sheet_id:
            raise ValueError("Sheet id is required (SHEET_ID env is missing)")

        if not sheet_page:
            sheet_page = "applications_list"

        if not isinstance(db_snapshot, dict):
            raise ValueError("db_snapshot must be an object")

        row_id_value = db_snapshot.get("row_id")

        if row_id_value is None:
            raise ValueError("db_snapshot.row_id is required")

        try:
            row_id = int(row_id_value)
        except Exception as e:
            msg = f"Invalid db_snapshot.row_id value: {row_id_value}"
            self._logger.error(msg)
            raise ValueError(msg) from e

        updates = self._build_external_updates(row_id, db_snapshot)

        if not updates:
            self._logger.info(
                "No external updates collected for row_id=%s, skipping",
                row_id,
            )

            return

        try:
            self._sheets_service.batch_update_values(
                resolved_sheet_id,
                sheet_page,
                updates,
            )
        except Exception as e:
            msg = (
                f"Failed to update Google Sheet via update-external for row_id={row_id}: {e}"
            )
            self._logger.error(msg)
            raise ValueError(msg)

        self._logger.info(
            "Updated Google Sheet row row_id=%s via update-external",
            row_id,
        )

    def _build_external_updates(
        self,
        row_id: int,
        snapshot: dict[str, Any],
    ) -> list[tuple[str, str]]:
        updates: list[tuple[str, str]] = []

        def add(column: str, value: Any, *, skip_when_empty: bool = False) -> None:
            if skip_when_empty and (value is None or value == ""):
                return

            updates.append((f"{column}{row_id}", self._stringify_sheet_value(value)))

        add("A", snapshot.get("company"))
        add("B", snapshot.get("employment_type"))
        add("C", snapshot.get("work_mode"))
        add("D", snapshot.get("title"))

        salary_applied_payload = snapshot.get("salary_applied")
        salary_proposed_payload = snapshot.get("salary_proposed")

        applied_salary_text = self._salary_payload_to_sheet_text(salary_applied_payload)
        proposed_salary_text = self._salary_payload_to_sheet_text(salary_proposed_payload)

        add("E", applied_salary_text)
        add("F", proposed_salary_text)

        currency = self._normalize_scalar((salary_applied_payload or {}).get("currency"))
        if currency is None:
            currency = self._normalize_scalar((salary_proposed_payload or {}).get("currency"))

        period = self._normalize_scalar((salary_applied_payload or {}).get("period"))
        if period is None:
            period = self._normalize_scalar((salary_proposed_payload or {}).get("period"))

        add("G", currency)
        add("H", period)

        add("I", snapshot.get("status"))
        add("J", self._format_snapshot_datetime(snapshot.get("applied_at"), "%d/%m/%Y"))
        responded_at_value = self._format_snapshot_datetime(
            snapshot.get("responded_at"),
            "%d/%m/%Y",
        )
        add("K", responded_at_value, skip_when_empty=True)
        add(
            "L",
            self._format_snapshot_datetime(
                snapshot.get("next_follow_up_at"),
                "%d/%m/%Y %H:%M:%S",
            ),
        )

        add("M", self._format_stage_for_sheet(snapshot.get("stage")))

        meta = snapshot.get("meta") if isinstance(snapshot.get("meta"), dict) else {}
        add("N", (meta or {}).get("contacts"))
        add("O", (meta or {}).get("job_description"))
        add("P", (meta or {}).get("notes"))

        return updates

    def _format_stage_for_sheet(self, stage: Any) -> str:
        if stage is None:
            return ""

        try:
            stage_value = int(stage)
        except Exception:
            return self._stringify_sheet_value(stage)

        return str(stage_value + 1)

    def _format_snapshot_datetime(self, value: Any, fmt: str) -> str:
        if value is None:
            return ""

        if isinstance(value, datetime):
            normalized = self._strip_timezone(value).replace(microsecond=0)

            return normalized.strftime(fmt)

        if isinstance(value, (int, float)):
            try:
                dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
                normalized = self._strip_timezone(dt).replace(microsecond=0)

                return normalized.strftime(fmt)
            except Exception:
                return str(value)

        if isinstance(value, str):
            text = value.strip()

            if not text:
                return ""

            iso_candidate = text
            if iso_candidate.endswith("Z"):
                iso_candidate = f"{iso_candidate[:-1]}+00:00"

            try:
                dt = datetime.fromisoformat(iso_candidate)
                normalized = self._strip_timezone(dt).replace(microsecond=0)

                return normalized.strftime(fmt)
            except Exception:
                pass

            for pattern in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y",
            ):
                try:
                    dt = datetime.strptime(text, pattern)
                    normalized = self._strip_timezone(dt).replace(microsecond=0)

                    return normalized.strftime(fmt)
                except Exception:
                    continue

            return text

        return str(value)

    def _salary_payload_to_sheet_text(self, payload: Any) -> str:
        if not isinstance(payload, dict):
            return ""

        amount_from = self._extract_decimal(payload.get("amount_from"))
        amount_to = self._extract_decimal(payload.get("amount_to"))

        from_text = self._format_salary_amount_for_sheet(amount_from)
        to_text = self._format_salary_amount_for_sheet(amount_to)

        if from_text and to_text:
            if from_text == to_text:
                return from_text

            return f"{from_text}-{to_text}"

        if from_text:
            return from_text

        if to_text:
            return to_text

        return ""

    def _format_salary_amount_for_sheet(self, value: Decimal | None) -> str | None:
        if value is None:
            return None

        try:
            normalized = value.normalize()
        except Exception:
            return None

        try:
            text = format(normalized, "f")
        except Exception:
            text = str(normalized)

        if "." in text:
            trimmed = text.rstrip("0").rstrip(".")
            text = trimmed or "0"

        if not text:
            text = "0"

        return text or "0"

    def _stringify_sheet_value(self, value: Any) -> str:
        if value is None:
            return ""

        if isinstance(value, str):
            return value

        return str(value)


def get_application_service(
    db_session: Session = Depends(get_db),
) -> ApplicationService:
    redis_client = get_redis_client()
    sheets_service = get_sheets_service()
    openai_service = get_openai_service()
    embedding_publisher = get_embedding_publisher()

    service = ApplicationService(
        db_session,
        redis_client,
        sheets_service,
        openai_service,
        embedding_publisher,
    )

    return service

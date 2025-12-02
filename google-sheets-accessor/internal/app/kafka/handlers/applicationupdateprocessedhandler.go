package handlers

import (
	"context"

	kafkaclient "github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/kafka"
)

type ApplicationUpdateProcessedHandler struct{}

func NewApplicationUpdateProcessedHandler() *ApplicationUpdateProcessedHandler {
	return &ApplicationUpdateProcessedHandler{}
}

func (h *ApplicationUpdateProcessedHandler) Handle(ctx context.Context, message kafkaclient.Message) error {
	return nil
}

// def _process_application_update_message(msg: Message) -> None:
//     key_bytes = msg.key() or b""
//     key_str = key_bytes.decode("utf-8", errors="ignore").strip()

//     value_bytes = msg.value() or b""
//     try:
//         payload = json.loads(value_bytes.decode("utf-8"))
//     except json.JSONDecodeError:
//         logger.warning(
//             "Failed to decode application update payload for key %s",
//             key_str or "<empty>",
//         )
//         return

//     logger.info(
//         "Received application update message key=%s payload=%s",
//         key_str or "<empty>",
//         payload,
//     )

//     try:
//         _apply_application_update(payload)
//     except Exception:
//         logger.exception(
//             "Failed to apply application update for key=%s payload=%s",
//             key_str or "<empty>",
//             payload,
//         )

// def _apply_application_update(payload: Any) -> None:
//     if not isinstance(payload, dict):
//         logger.warning("application update payload is not a dict: %s", type(payload))

//         return

//     is_meeting_confirmation = _is_meeting_confirmation(payload)
//     is_meeting_creation_confirmation = _is_meeting_creation_confirmation(payload)
//     is_denied_confirmation = _is_denied_confirmation(payload)

//     if not (is_meeting_confirmation or is_denied_confirmation):
//         return

//     sheet_id = config.SHEET_ID
//     if not sheet_id:
//         logger.warning("Skipping application update: SHEET_ID is not configured")

//         return

//     mapped_application = payload.get("mapped_application") or {}
//     if not isinstance(mapped_application, dict):
//         logger.warning(
//             "Skipping application update: mapped_application is missing or invalid",
//         )

//         return

//     application_id = mapped_application.get("id")
//     row_id = mapped_application.get("row_id")

//     if application_id is None or row_id is None:
//         logger.warning(
//             "Skipping application update: application_id or row_id is missing "
//             "(application_id=%s, row_id=%s)",
//             application_id,
//             row_id,
//         )

//         return

//     stage_value: int | None = None
//     if is_meeting_creation_confirmation:
//         stage_raw = mapped_application.get("stage")
//         stage_value = _parse_stage(stage_raw)

//     status_value = "denied" if is_denied_confirmation else payload.get("status") or mapped_application.get("status")
//     responded_at_value = payload.get("responded_at") or mapped_application.get("responded_at")
//     if is_denied_confirmation and not responded_at_value:
//         responded_at_value = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

//     next_follow_up_value = None
//     if is_meeting_creation_confirmation:
//         next_follow_up_value = payload.get("next_follow_up_at") or mapped_application.get("next_follow_up_at")

//     session: Session = session_maker()
//     try:
//         application_service = get_application_service(db_session=session)
//         update_payload = {
//             "application_id": application_id,
//             "row_id": row_id,
//         }
//         if stage_value is not None:
//             update_payload["stage"] = stage_value
//         if status_value is not None:
//             update_payload["status"] = status_value
//         if responded_at_value is not None:
//             update_payload["responded_at"] = responded_at_value
//         if next_follow_up_value is not None:
//             update_payload["next_follow_up_at"] = next_follow_up_value
//         logger.info(
//             "Applying application update: app_id=%s row_id=%s payload=%s",
//             application_id,
//             row_id,
//             update_payload,
//         )

//         application_service.update(update_payload)

//         if is_meeting_creation_confirmation:
//             logger.info(
//                 "Stage updated for %s: application_id=%s row_id=%s stage=%s",
//                 _get_email_label(payload) or "meeting_confirmation",
//                 application_id,
//                 row_id,
//                 stage_value,
//             )
//         elif is_denied_confirmation:
//             logger.info(
//                 "Application marked as denied: application_id=%s row_id=%s responded_at=%s",
//                 application_id,
//                 row_id,
//                 responded_at_value,
//             )
//     except Exception:
//         session.rollback()
//         raise
//     finally:
//         session.close()

// def _is_meeting_creation_confirmation(payload: dict[str, Any]) -> bool:
//     label = _get_email_label(payload)
//     action = str(payload.get("action") or "").lower()

//     if label != MEETING_CREATION_LABEL:
//         return False

//     if action and action != "cnfm":
//         return False

//     return True

// def _is_meeting_confirmation(payload: dict[str, Any]) -> bool:
//     label = _get_email_label(payload)
//     action = str(payload.get("action") or "").lower()

//     if label not in MEETING_CONFIRMATION_LABELS:
//         return False

//     if action and action != "cnfm":
//         return False

//     return True

// def _is_denied_confirmation(payload: dict[str, Any]) -> bool:
//     email = payload.get("email") or {}
//     label = str(email.get("label") or "").lower()
//     action = str(payload.get("action") or "").lower()

//     if label != "denied":
//         return False

//     if action and action != "cnfm":
//         return False

//     return True

// def _parse_stage(value: Any) -> int:
//     if value is None:
//         return 0

//     try:
//         return int(str(value).strip())
//     except Exception:
//         logger.warning("Invalid stage value '%s', defaulting to 0", value)

//         return 0

// def _get_email_label(payload: dict[str, Any]) -> str:
//     email = payload.get("email") or {}

//     return str(email.get("label") or "").lower()

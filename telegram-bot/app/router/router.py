import logging

from aiogram import Router, types, F
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery

from app.router.handlers.buttons.applied import get_button_applied_handler
from app.router.handlers.buttons.denied import get_button_denied_handler
from app.router.handlers.buttons.meeting_inv import get_button_meeting_inv_handler
from app.router.handlers.buttons.meeting_crt import get_button_meeting_crt_handler
from app.router.handlers.buttons.meeting_upd import get_button_meeting_upd_handler
from app.router.handlers.buttons.meeting_cncl import get_button_meeting_cncl_handler
from app.router.handlers.buttons.application_diff import get_application_diff_button_handler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = Router()

button_applied_handler = get_button_applied_handler()
button_denied_handler = get_button_denied_handler()
button_meeting_inv_handler = get_button_meeting_inv_handler()
button_meeting_crt_handler = get_button_meeting_crt_handler()
button_meeting_upd_handler = get_button_meeting_upd_handler()
button_meeting_cncl_handler = get_button_meeting_cncl_handler()
button_application_diff_handler = get_application_diff_button_handler()


@router.message(CommandStart())
async def cmd_start(message: types.Message) -> None:
    user = message.from_user
    tg_id_str = str(user.id) if user is not None else "unknown"
    if user is None:
        logger.error("Received /start without from_user, using 'unknown' tg_id")

    await message.answer(
        "Привет! Я мультиюзер-бот.\n"
        "Команды:\n"
        "• /set <key> <value>\n"
        "• /get <key>\n"
        "• /me — твои данные\n\n"
        "Из Kafka я читаю ключ=TG_ID, value=строка или JSON и шлю тебе.\n\n"
        f"tg_id = {tg_id_str}"
    )


@router.callback_query(F.data.startswith("applied:cnfm:"))
async def handle_applied_cnfm(callback: CallbackQuery) -> None:
    await button_applied_handler.cnfm(callback)


@router.callback_query(F.data.startswith("applied:dtls:"))
async def handle_applied_dtls(callback: CallbackQuery) -> None:
    await button_applied_handler.dtls(callback)


@router.callback_query(F.data.startswith("applied:skp:"))
async def handle_applied_skp(callback: CallbackQuery) -> None:
    await button_applied_handler.skp(callback)


@router.callback_query(F.data.startswith("denied:cnfm:"))
async def handle_denied_cnfm(callback: CallbackQuery) -> None:
    await button_denied_handler.cnfm(callback)


@router.callback_query(F.data.startswith("denied:dtls:"))
async def handle_denied_dtls(callback: CallbackQuery) -> None:
    await button_denied_handler.dtls(callback)


@router.callback_query(F.data.startswith("denied:skp:"))
async def handle_denied_skp(callback: CallbackQuery) -> None:
    await button_denied_handler.skp(callback)


@router.callback_query(F.data.startswith("meeting_inv:cnfm:"))
async def handle_meeting_inv_cnfm(callback: CallbackQuery) -> None:
    await button_meeting_inv_handler.cnfm(callback)


@router.callback_query(F.data.startswith("meeting_inv:dtls:"))
async def handle_meeting_inv_dtls(callback: CallbackQuery) -> None:
    await button_meeting_inv_handler.dtls(callback)


@router.callback_query(F.data.startswith("meeting_inv:skp:"))
async def handle_meeting_inv_skp(callback: CallbackQuery) -> None:
    await button_meeting_inv_handler.skp(callback)


@router.callback_query(F.data.startswith("meeting_crt:cnfm:"))
async def handle_meeting_crt_cnfm(callback: CallbackQuery) -> None:
    await button_meeting_crt_handler.cnfm(callback)


@router.callback_query(F.data.startswith("meeting_crt:dtls:"))
async def handle_meeting_crt_dtls(callback: CallbackQuery) -> None:
    await button_meeting_crt_handler.dtls(callback)


@router.callback_query(F.data.startswith("meeting_crt:skp:"))
async def handle_meeting_crt_skp(callback: CallbackQuery) -> None:
    await button_meeting_crt_handler.skp(callback)


@router.callback_query(F.data.startswith("meeting_upd:cnfm:"))
async def handle_meeting_upd_cnfm(callback: CallbackQuery) -> None:
    await button_meeting_upd_handler.cnfm(callback)


@router.callback_query(F.data.startswith("meeting_upd:dtls:"))
async def handle_meeting_upd_dtls(callback: CallbackQuery) -> None:
    await button_meeting_upd_handler.dtls(callback)


@router.callback_query(F.data.startswith("meeting_upd:skp:"))
async def handle_meeting_upd_skp(callback: CallbackQuery) -> None:
    await button_meeting_upd_handler.skp(callback)


@router.callback_query(F.data.startswith("meeting_cncl:cnfm:"))
async def handle_meeting_cncl_cnfm(callback: CallbackQuery) -> None:
    await button_meeting_cncl_handler.cnfm(callback)


@router.callback_query(F.data.startswith("meeting_cncl:dtls:"))
async def handle_meeting_cncl_dtls(callback: CallbackQuery) -> None:
    await button_meeting_cncl_handler.dtls(callback)


@router.callback_query(F.data.startswith("meeting_cncl:skp:"))
async def handle_meeting_cncl_skp(callback: CallbackQuery) -> None:
    await button_meeting_cncl_handler.skp(callback)


@router.callback_query(F.data.startswith("application_diff:cnfm:"))
async def handle_application_diff_cnfm(callback: CallbackQuery) -> None:
    await button_application_diff_handler.apply_sheet(callback)


@router.callback_query(F.data.startswith("application_diff:aplsh:"))
async def handle_application_diff_apply_sheet(callback: CallbackQuery) -> None:
    await button_application_diff_handler.apply_sheet(callback)


@router.callback_query(F.data.startswith("application_diff:aplin:"))
async def handle_application_diff_apply_internal(callback: CallbackQuery) -> None:
    await button_application_diff_handler.apply_internal(callback)


@router.callback_query(F.data.startswith("application_diff:skp:"))
async def handle_application_diff_skp(callback: CallbackQuery) -> None:
    await button_application_diff_handler.skp(callback)

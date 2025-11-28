from __future__ import annotations

import logging

from aiogram.types import Message


SEPARATOR = "\n\n"


async def append_button_response(
    *,
    message: Message,
    response_text: str,
    logger: logging.Logger,
) -> bool:
    base_text = message.text
    edit_method = message.edit_text

    if base_text is None:
        base_text = message.caption
        edit_method = message.edit_caption

    if base_text is None:
        base_text = ""

    separator = SEPARATOR if base_text else ""
    updated_text = f"{base_text}{separator}{response_text}"

    try:
        await edit_method(
            updated_text,
            reply_markup=None,
        )
    except Exception as e:
        logger.warning(
            "Failed to append response text to message %s: %s",
            message.message_id,
            e,
        )

        return False

    return True

import string

WHITESPACE_TO_REMOVE: dict[int, None] = {
    ord("\n"): None,
    ord("\r"): None,
    ord("\t"): None,
    ord("\v"): None,
    ord("\f"): None,
}

PUNCTUATION_TO_REMOVE: dict[int, None] = {
    ord(symbol): None for symbol in string.punctuation
}

CHARACTERS_TO_REMOVE: dict[int, None] = {
    **WHITESPACE_TO_REMOVE,
    **PUNCTUATION_TO_REMOVE,
}


def normalize_content(value: str) -> str:
    if not value:
        result = ""

        return result

    lowered_value = value.lower()

    normalized_value = lowered_value.translate(CHARACTERS_TO_REMOVE)

    return normalized_value

from __future__ import annotations

from app.enums import EmailLabel
from app.services.scoring.features import TimeTauResolver


class StaticTimeTauResolver(TimeTauResolver):
    def resolve(
        self,
        label: EmailLabel,
        application: dict[str, object],
    ) -> float:
        _ = application
        mapping: dict[EmailLabel, float] = {
            EmailLabel.APPLIED: 60.0,
            EmailLabel.DENIED: 60.0 * 24.0 * 3.0,
            EmailLabel.MEETING_INV: 60.0 * 24.0 * 3.0,
            EmailLabel.MEETING_CRT: 60.0 * 24.0 * 3.0,
            EmailLabel.MEETING_UPD: 60.0 * 24.0 * 3.0,
            EmailLabel.MEETING_CNCL: 60.0 * 24.0 * 3.0,
            EmailLabel.OFFER: 60.0 * 24.0 * 7.0,
        }

        try:
            tau = mapping[label]
        except KeyError as e:
            message = f"Unsupported label for time tau resolver: {label}"
            raise ValueError(message) from e

        return tau

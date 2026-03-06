package messages

import "github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/enums"

type DiffUpdatePayload struct {
	ApplicationRowID int64                 `json:"application_row_id"`
	UpdateDirection  enums.UpdateDirection `json:"update_direction"`
}

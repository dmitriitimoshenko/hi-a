package mocks

import (
	"context"

	"github.com/go-telegram/bot/models"
	"github.com/stretchr/testify/mock"
)

type BotMock struct {
	mock.Mock
}

func (m *BotMock) SendMessage(ctx context.Context, message string, keyboard *models.InlineKeyboardMarkup) error {
	args := m.Called(ctx, message, keyboard)

	return args.Error(0)
}

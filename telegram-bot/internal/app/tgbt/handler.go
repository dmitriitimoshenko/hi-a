package tgbt

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/enums"
	tgbot "github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

const (
	errorMessage        = "⭕ INTERNAL ERROR OCCURED ⭕"
	confirmPopUpMessage = "☑️ Confirming..."
	confirmMessage      = "✅ Confirmed"

	KAFKA_TOPIC_APPLICATION_UPDATE_UNPROCESSED = "KAFKA_TOPIC_APPLICATION_UPDATE_UNPROCESSED"
	applicationUpdateUnprocessedConfirmKey     = "cnfm_button_pressed"
)

type redisClient interface {
	Get(ctx context.Context, key string) (string, bool, error)
	Delete(ctx context.Context, key string) (int64, error)
}

type kafkaClient interface {
	Publish(ctx context.Context, topic string, key []byte, value []byte) error
}

type TelegramBotHandler struct {
	logger      *slog.Logger
	redisClient redisClient
	kafkaClient kafkaClient
}

func NewTelegramBotHandler(
	logger *slog.Logger,
	redisClient redisClient,
	kafkaClient kafkaClient,
) *TelegramBotHandler {
	return &TelegramBotHandler{
		logger:      logger,
		redisClient: redisClient,
		kafkaClient: kafkaClient,
	}
}

func (h *TelegramBotHandler) Handle(ctx context.Context, b *tgbot.Bot, update *models.Update) {
	switch {
	case update.CallbackQuery != nil:
		data := update.CallbackQuery.Data

		switch {
		case strings.HasPrefix(data, "applied:cnfm:"):
			h.handleAppliedConfirmation(ctx, b, update)
		}
	}
}

func (h *TelegramBotHandler) handleAppliedConfirmation(ctx context.Context, b *tgbot.Bot, update *models.Update) {
	data := update.CallbackQuery.Data

	dataParts := strings.Split(data, ":")

	emailID := dataParts[len(dataParts)-1]
	if emailID == "" && update.CallbackQuery != nil {
		h.notifyInternalError(ctx, b, update)
		h.logger.Error("[handleAppliedConfirmation] emailID is empty")
		return
	}

	prefix := enums.EmailLabel(dataParts[0])
	if !prefix.IsValid() || prefix != enums.EmailLabelApplied {
		h.notifyInternalError(ctx, b, update)
		h.logger.Error("[handleAppliedConfirmation] invalid prefix of not \"applied\"", "prefix", prefix)
		return
	}

	cacheKey := fmt.Sprintf("%d:%s", update.CallbackQuery.From.ID, emailID)
	val, ok, err := h.redisClient.Get(ctx, cacheKey)
	if err != nil {
		h.notifyInternalError(ctx, b, update)
		h.logger.Error("[handleAppliedConfirmation] failed to get from redis", "err", err)
		return
	}
	if !ok || val == "" {
		h.notifyInternalError(ctx, b, update)
		h.logger.Error("[handleAppliedConfirmation] failed to get from redis: not ok", "err", err)
		return
	}

	ok, err = b.AnswerCallbackQuery(ctx, &tgbot.AnswerCallbackQueryParams{
		CallbackQueryID: update.CallbackQuery.ID,
		Text:            confirmPopUpMessage,
		ShowAlert:       false,
	})
	if err != nil {
		h.logger.Error("[handleAppliedConfirmation] failed to answer callback query", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}
	if !ok {
		h.logger.Error("[handleAppliedConfirmation] failed to answer callback query: not ok")
		h.notifyInternalError(ctx, b, update)
		return
	}

	topicToPublish := os.Getenv("KAFKA_TOPIC_APPLICATION_UPDATE_UNPROCESSED")
	if err = h.kafkaClient.Publish(ctx, topicToPublish, []byte(applicationUpdateUnprocessedConfirmKey), []byte(val)); err != nil {
		h.logger.Error("[handleAppliedConfirmation] failed to publish to kafka", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}

	if _, err = h.redisClient.Delete(ctx, cacheKey); err != nil {
		h.logger.Error("[handleAppliedConfirmation] failed to delete from redis", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}

	if err = h.removeInlineKeyboard(ctx, b, update.Message); err != nil {
		h.logger.Error("[handleAppliedConfirmation] failed to remove inline keyboard", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}

	replyMessage := update.Message.ReplyToMessage
	if err = h.appendLineToMessage(ctx, b, confirmMessage, replyMessage); err != nil {
		h.logger.Error("[handleAppliedConfirmation] failed to append line to message", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}

	ok, err = b.AnswerCallbackQuery(ctx, &tgbot.AnswerCallbackQueryParams{
		CallbackQueryID: update.CallbackQuery.ID,
		Text:            confirmMessage,
		ShowAlert:       false,
	})
	if err != nil {
		h.logger.Error("[handleAppliedConfirmation] failed to answer callback query", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}
	if !ok {
		h.logger.Error("[handleAppliedConfirmation] failed to answer callback query: not ok")
		h.notifyInternalError(ctx, b, update)
		return
	}
}

func (h *TelegramBotHandler) notifyInternalError(ctx context.Context, b *tgbot.Bot, update *models.Update) {
	_, err := b.SendMessage(ctx, &tgbot.SendMessageParams{
		ChatID: update.CallbackQuery.Message.Message.Chat.ID,
		Text:   errorMessage,
	})
	if err != nil {
		h.logger.Error("[handleAppliedConfirmation] failed to send error message in Telegram", "err", err)
	}
}

func (h *TelegramBotHandler) removeInlineKeyboard(
	ctx context.Context,
	b *tgbot.Bot,
	message *models.Message,
) error {
	if _, err := b.EditMessageReplyMarkup(ctx, &tgbot.EditMessageReplyMarkupParams{
		ChatID:    message.Chat.ID,
		MessageID: message.ID,
	}); err != nil {
		return fmt.Errorf("failed to edit message reply markup: %w", err)
	}

	return nil
}

func (h *TelegramBotHandler) appendLineToMessage(
	ctx context.Context,
	b *tgbot.Bot,
	add string,
	message *models.Message,
) error {
	if message == nil {
		return fmt.Errorf("message is nil")
	}

	baseText := message.Text
	newText := fmt.Sprintf("%s\n\n%s", baseText, add)
	message.Text = newText

	if _, err := b.EditMessageText(ctx, &tgbot.EditMessageTextParams{
		ChatID:    message.Chat.ID,
		MessageID: message.ID,
		ParseMode: models.ParseModeHTML,
		Text:      newText,
	}); err != nil {
		return fmt.Errorf("failed to edit message text: %w", err)
	}

	return nil
}
